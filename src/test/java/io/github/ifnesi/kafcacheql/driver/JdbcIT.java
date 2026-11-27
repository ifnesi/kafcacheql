package io.github.ifnesi.kafcacheql.driver;

import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.common.header.internals.RecordHeaders;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import io.github.ifnesi.kafcacheql.testkit.TestStack.CountingRegistry;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.sqlite.Function;

class JdbcIT {
    private static final String PRIVATE_BIG_INTEGER = "98765432109876543210987654321";
    private static final String URL = "jdbc:kafcacheql://" + TestStack.BOOTSTRAP;
    private static final Schema VALUE = new Schema.Parser().parse("""
            {"type":"record","name":"JdbcValue","fields":[{"name":"n","type":"int"},{"name":"ok","type":["null","boolean"]}]}
            """);
    @TempDir Path dir;
    @BeforeAll static void stack() throws Exception { TestStack.requireUp(); Class.forName(KafCacheQLDriver.class.getName()); }

    Properties settings() {
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("cache.db").toString());
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        p.setProperty("sync.batch.records", "1");
        p.setProperty("max.poll.records", "1");
        p.setProperty("append.default.window", "all");
        return p;
    }
    static String topic(String policy) {
        String topic = TestStack.uniqueTopic("jdbc-" + policy);
        TestStack.createTopic(topic, 1, Map.of("cleanup.policy", policy));
        return topic;
    }
    static void send(String topic, int n, Long time) {
        var record = new GenericData.Record(VALUE); record.put("n", n); record.put("ok", n == 0 ? null : n % 2 == 0);
        TestStack.sendAvro(topic, "key" + n, record, time, TestStack.SchemaIdIn.PREFIX);
    }
    static List<Integer> values(ResultSet rows) throws SQLException {
        var result = new ArrayList<Integer>();
        while (rows.next()) result.add(rows.getInt(1));
        return result;
    }

    /** PLAN §7.1: headers preserve order/duplicates, with strict UTF-8 text or base64, including masked text.
     * §7.3: older decoder output is served unchanged offline and rebuilt on the next online use.
     */
    @Test void headersAreReadableAndOlderCachesRebuildOnlyOnline() throws Exception {
        String plain = topic("delete"), maskedTopic = topic("compact");
        var p = settings(); p.setProperty("mask.secret", "test-transform-secret");
        p.setProperty("topic." + maskedTopic + ".mask.headers", "client-ip");
        var headers = new RecordHeaders().add("text", TestStack.utf8("café"))
                .add("binary", new byte[] {0, (byte) 255}).add("null", null).add("empty", new byte[0])
                .add("tag", TestStack.utf8("first")).add("tag", TestStack.utf8("second"));
        TestStack.send(plain, 0, null, null, TestStack.utf8("{}"), headers);
        TestStack.send(plain, 0, null, null, TestStack.utf8("{}"), null);
        TestStack.send(maskedTopic, 0, null, TestStack.utf8("key"), TestStack.utf8("{}"),
                new RecordHeaders().add("client-ip", TestStack.utf8("private-ip-123")));
        String hash = masked(TestStack.utf8("private-ip-123"));
        assertTrue(hash.matches("[0-9a-f]{64}"));
        var expected = Map.of(plain, """
                [{"key":"text","value":"café"},{"key":"binary","base64":"AP8="},{"key":"null","value":null},{"key":"empty","value":""},{"key":"tag","value":"first"},{"key":"tag","value":"second"}]""",
                maskedTopic, "[{\"key\":\"client-ip\",\"value\":\"" + hash + "\"}]");
        var old = Map.of(plain, """
                [{"key":"text","value":"Y2Fmw6k="},{"key":"binary","value":"AP8="},{"key":"null","value":null},{"key":"empty","value":""},{"key":"tag","value":"Zmlyc3Q="},{"key":"tag","value":"c2Vjb25k"}]""",
                maskedTopic, "[{\"key\":\"client-ip\",\"value\":\"" + Base64.getEncoder().encodeToString(TestStack.utf8(hash)) + "\"}]");
        var generations = new HashMap<String, Long>();
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
             var cache = CacheContext.acquire(DriverConfig.load(URL, p)); var execution = new Execution(java.time.Duration.ofSeconds(30))) {
            try (var r = s.executeQuery("SELECT _headers FROM " + StorageDdl.quote(plain) + " WHERE _offset=1")) {
                assertTrue(r.next()); assertEquals("[]", r.getString(1)); assertFalse(r.next());
            }
            for (String topic : List.of(plain, maskedTopic)) {
                try (var r = s.executeQuery("SELECT _headers FROM " + StorageDdl.quote(topic) + " WHERE _offset=0")) {
                    assertTrue(r.next()); assertEquals(expected.get(topic), r.getString(1)); assertFalse(r.next());
                }
                long generation = cache.read(reader -> TopicRegistry.generation(reader, topic).orElseThrow());
                generations.put(topic, generation);
                // Seed the exact version-6 representation, including on a transformed topic.
                cache.write(topic, generation, execution, writer -> {
                    try (var update = writer.prepareStatement("UPDATE " + StorageDdl.quote(StorageDdl.tableName(generation)) + " SET _headers=? WHERE _offset=0")) {
                        update.setString(1, old.get(topic)); assertEquals(1, update.executeUpdate());
                    }
                    try (var update = writer.prepareStatement("UPDATE \"$topics\" SET decoder_version=6 WHERE id=?")) {
                        update.setLong(1, generation); assertEquals(1, update.executeUpdate());
                    }
                    return null;
                });
            }
        }
        p.setProperty("offline", "true");
        try (var c = DriverManager.getConnection("jdbc:kafcacheql://", p); var s = c.createStatement()) {
            for (String topic : List.of(plain, maskedTopic)) {
                try (var r = s.executeQuery("SELECT _headers FROM " + StorageDdl.quote(topic) + " WHERE _offset=0")) {
                    assertTrue(r.next()); assertEquals(old.get(topic), r.getString(1)); assertFalse(r.next());
                }
                try (var r = s.executeQuery("SELECT id,decoder_version FROM \"$topics\" WHERE topic='" + topic + "'")) {
                    assertTrue(r.next()); assertEquals(generations.get(topic).longValue(), r.getLong(1)); assertEquals(6, r.getInt(2));
                }
            }
        }
        p.setProperty("offline", "false");
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            for (String topic : List.of(plain, maskedTopic)) {
                try (var r = s.executeQuery("SELECT _headers FROM " + StorageDdl.quote(topic) + " WHERE _offset=0")) {
                    assertTrue(r.next()); assertEquals(expected.get(topic), r.getString(1)); assertFalse(r.next());
                }
                try (var r = s.executeQuery("SELECT id,decoder_version FROM \"$topics\" WHERE topic='" + topic + "'")) {
                    assertTrue(r.next()); assertTrue(r.getLong(1) > generations.get(topic)); assertEquals(7, r.getInt(2));
                }
            }
        }
    }

    /** PLAN §7.4: drop/mask runs before storage; equal masked values join across topics. */
    @Test void recordTransformsKeepPiiOutOfTheCache() throws Exception {
        String a = topic("delete"), b = topic("compact"), big = topic("delete"), badKey = topic("compact");
        var p = settings(); p.setProperty("mask.secret", "test-transform-secret");
        p.setProperty("topic." + big + ".drop", "_key,_value");
        p.setProperty("topic." + badKey + ".drop", "_key,_value");
        p.setProperty("topic." + badKey + ".key.format", "json");
        TestStack.sendString(big, "big-private-key", "{\"privateId\":" + PRIVATE_BIG_INTEGER + "}", null);
        TestStack.sendString(badKey, "undecodable-private-key", "{\"private\":\"private-payload\"}", null);
        byte[] framed = null;
        for (String t : List.of(a, b)) {
            p.setProperty("topic." + t + ".mask", (t.equals(a) ? "_key.email" : "_key") + ",email,n,address.zip");
            p.setProperty("topic." + t + ".drop", "phone,address.street,_key.phone,_key.address.street,_key.address.zip");
            p.setProperty("topic." + t + ".mask.headers", "client-ip");
            p.setProperty("topic." + t + ".drop.headers", "authorization");
            var schema = new Schema.Parser().parse("""
                    {"type":"record","name":"PrivateValue","fields":[{"name":"email","type":"string"},
                    {"name":"phone","type":"string"},{"name":"n","type":"long"},{"name":"flag","type":"boolean"},
                    {"name":"address","type":{"type":"record","name":"Address","fields":[{"name":"street","type":"string"},{"name":"zip","type":"string"}]}}]}
                    """);
            var value = new GenericData.Record(schema);
            value.put("email", "pii@example.test"); value.put("phone", "private-phone-123"); value.put("n", 42L); value.put("flag", true);
            var address = new GenericData.Record(schema.getField("address").schema()); address.put("street", "private-street-123"); address.put("zip", "private-zip-123");
            value.put("address", address);
            var headers = new RecordHeaders().add("client-ip", "private-ip-123".getBytes()).add("authorization", "private-auth-123".getBytes());
            byte[] data = TestStack.serializeAvro(t, headers, value, TestStack.SchemaIdIn.PREFIX, false);
            if (t.equals(a)) framed = data;
            byte[] key = t.equals(a) ? TestStack.serializeAvro(t, headers, value, TestStack.SchemaIdIn.PREFIX, true)
                    : "private-key@example.test".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            TestStack.send(t, 0, null, key, data, headers);
        }
        p.setProperty("topic." + a + ".transform", PrivacyTransform.class.getName());
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var r = s.executeQuery("SELECT email,phone,address,n,_key,_headers,flag,_value FROM " + StorageDdl.quote(a))) {
                assertTrue(r.next()); assertEquals(masked("pii@example.test"), r.getString(1));
                assertNull(r.getObject(2)); assertEquals("{\"zip\":\"" + masked("private-zip-123") + "\"}", r.getString(3));
                assertType(r, "n", JDBCType.VARCHAR); assertTrue(r.getString(4).matches("[0-9a-f]{64}"));
                var key = JacksonMapper.INSTANCE.readTree(r.getString(5));
                assertEquals(masked("pii@example.test"), key.get("email").asText()); assertFalse(key.has("phone"));
                var headers = JacksonMapper.INSTANCE.readTree(r.getString(6));
                assertEquals(1, headers.size()); assertEquals("client-ip", headers.get(0).get("key").asText());
                assertEquals(masked("private-ip-123".getBytes()), headers.get(0).get("value").asText());
                assertFalse(headers.get(0).has("base64"));
                assertFalse(JacksonMapper.INSTANCE.readTree(r.getString("_value")).has("phone"));
                assertNull(r.getObject("flag"), "a transformed value that does not fit its schema reads NULL");
            }
            try (var r = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(a) + " a JOIN " + StorageDdl.quote(b) + " b ON a.email=b.email")) {
                assertTrue(r.next()); assertEquals(1, r.getInt(1));
            }
            // A second update replaces state using the HMAC of the original key, independent of visible key changes.
            TestStack.send(b, 0, null, "private-key@example.test".getBytes(), "{\"email\":\"pii@example.test\"}".getBytes(), new RecordHeaders());
            try (var r = s.executeQuery("SELECT count(*),max(_offset) FROM " + StorageDdl.quote(b))) {
                assertTrue(r.next()); assertEquals(1, r.getInt(1)); assertEquals(1, r.getInt(2));
            }
            // A damaged Avro payload fails closed, and a throwing hook produces the same payload-free row.
            TestStack.send(a, 0, null, "error-private-key".getBytes(), Arrays.copyOf(framed, 5), new RecordHeaders().add("pii", "private-header".getBytes()));
            TestStack.send(a, 0, null, "throw-private-key".getBytes(), "{\"action\":\"throw\"}".getBytes(), new RecordHeaders());
            TestStack.send(a, 0, null, "skip-private-key".getBytes(), "{\"action\":\"skip\"}".getBytes(), new RecordHeaders());
            try (var r = s.executeQuery("SELECT _offset,_key,_value,_headers,_decode_error FROM " + StorageDdl.quote(a) + " WHERE _offset>=1 ORDER BY _offset")) {
                for (int i = 1; i <= 2; i++) {
                    assertTrue(r.next()); assertEquals(i, r.getInt(1));
                    assertNull(r.getObject(2)); assertNull(r.getObject(3)); assertNull(r.getObject(4));
                    assertNotNull(r.getString(5)); if (i == 2) assertTrue(r.getString(5).contains("deliberate transform failure"));
                }
                assertFalse(r.next(), "null from apply skips the row, while its offsets are covered");
            }
            try (var r = s.executeQuery("SELECT max(end_offset) FROM \"$ranges\" WHERE topic_ref=(SELECT id FROM \"$topics\" WHERE topic='" + a + "')")) {
                assertTrue(r.next()); assertEquals(4, r.getInt(1));
            }
            TestStack.send(b, 0, null, "private-key@example.test".getBytes(), Arrays.copyOf(framed, 5), new RecordHeaders());
            try (var r = s.executeQuery("SELECT _key,_value,_headers,_decode_error FROM " + StorageDdl.quote(b))) {
                assertTrue(r.next()); assertNull(r.getObject(1)); assertNull(r.getObject(2)); assertNull(r.getObject(3));
                assertNotNull(r.getString(4)); assertFalse(r.next(), "a failed update replaces old current state");
            }
            // PLAN §7.4: a throwing Java transform gives a payload-free error row, not a failed sync.
            for (String action : List.of("before", "linkage", "after"))
                TestStack.sendString(a, "{\"email\":\"pii@example.test\"}", "{\"action\":\"" + action + "\"}", null);
            try (var r = s.executeQuery("SELECT _offset,_key,_value,_headers,_decode_error FROM " + StorageDdl.quote(a) + " WHERE _offset>=4 ORDER BY _offset")) {
                for (int offset = 4; offset <= 6; offset++) {
                    assertTrue(r.next()); assertEquals(offset, r.getInt(1));
                    if (offset == 5) {
                        assertNull(r.getObject(2)); assertNull(r.getObject(3)); assertNull(r.getObject(4));
                        assertEquals("Record transform: missing dependency [hidden]", r.getString(5));
                    } else {
                        assertNotNull(r.getString(3)); assertNull(r.getString(5));
                    }
                }
                assertFalse(r.next());
            }
            TestStack.sendString(a, "{\"email\":\"pii@example.test\"}", "{\"action\":\"later\"}", null);
            try (var r = s.executeQuery("SELECT _value,_decode_error FROM " + StorageDdl.quote(a) + " WHERE _offset=7")) {
                assertTrue(r.next()); assertEquals("later", JacksonMapper.INSTANCE.readTree(r.getString(1)).get("action").asText());
                assertNull(r.getString(2)); assertFalse(r.next());
            }
            // PLAN §7.4: internal failures contain no payload, and original tombstones still delete state.
            assertAll(
                    () -> {
                        try (var r = s.executeQuery("SELECT _key,_value,_headers,_decode_error FROM " + StorageDdl.quote(big))) {
                            assertTrue(r.next()); assertNull(r.getObject(1)); assertNull(r.getObject(2)); assertNull(r.getObject(3));
                            assertNotNull(r.getString(4)); assertFalse(r.getString(4).contains(PRIVATE_BIG_INTEGER));
                            assertFalse(r.next());
                        }
                    },
                    () -> {
                        TestStack.sendString(b, "private-key@example.test", "{\"email\":\"pii@example.test\"}", null);
                        try (var r = s.executeQuery("SELECT count(*),count(_decode_error) FROM " + StorageDdl.quote(b))) {
                            assertTrue(r.next()); assertEquals(1, r.getInt(1)); assertEquals(0, r.getInt(2));
                        }
                        TestStack.sendString(b, "private-key@example.test", null, null);
                        try (var r = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(b))) {
                            assertTrue(r.next()); assertEquals(0, r.getInt(1), "a decoded transformed tombstone deletes its key");
                        }
                    },
                    () -> {
                        try (var r = s.executeQuery("SELECT _key,_value,_headers,_decode_error FROM " + StorageDdl.quote(badKey))) {
                            assertTrue(r.next()); assertNull(r.getObject(1)); assertNull(r.getObject(2)); assertNull(r.getObject(3));
                            assertNotNull(r.getString(4)); assertFalse(r.next());
                        }
                        TestStack.sendString(badKey, "undecodable-private-key", null, null);
                        try (var r = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(badKey))) {
                            assertTrue(r.next()); assertEquals(0, r.getInt(1), "a tombstone deletes even when its key cannot decode");
                        }
                    },
                    // Check sidecars while the cache is open, then the checkpointed database after close.
                    () -> assertNoPii(p));
        }
        assertNoPii(p);
    }

    private static String masked(Object value) throws Exception {
        var mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec("test-transform-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(JacksonMapper.INSTANCE.writeValueAsBytes(value)));
    }

    private static void assertNoPii(Properties p) throws Exception {
        for (String suffix : List.of("", "-wal", "-shm")) {
            Path file = Path.of(p.getProperty("cache.path") + suffix);
            if (!java.nio.file.Files.exists(file)) continue;
            String bytes = new String(java.nio.file.Files.readAllBytes(file), java.nio.charset.StandardCharsets.ISO_8859_1);
            for (String pii : List.of("private-key@example.test", "pii@example.test", "private-phone-123", "private-street-123", "private-ip-123", "private-auth-123", "private-zip-123", "error-private-key", "throw-private-key", "skip-private-key", "private-header", "test-transform-secret", PRIVATE_BIG_INTEGER, "big-private-key", "undecodable-private-key", "private-payload"))
                assertFalse(bytes.contains(pii), file + " contains " + pii);
        }
    }

    public static class PrivacyTransform implements io.github.ifnesi.kafcacheql.transform.RecordTransform {
        static String revision = "1";
        @Override public String version() { return revision; }
        @Override @SuppressWarnings("unchecked") public Record apply(Record record) {
            if (record.value() instanceof Map<?, ?>) {
                var value = (Map<String,Object>) record.value();
                if ("throw".equals(value.get("action"))) throw new IllegalArgumentException("deliberate transform failure");
                if ("linkage".equals(value.get("action"))) throw new NoClassDefFoundError("missing dependency test-transform-secret");
                if ("skip".equals(value.get("action"))) return null;
                value.put("flag", "changed type");
                value.put("phone", "private-phone-123"); // Built-ins run after the hook, even for a reintroduced field.
            }
            return record;
        }
    }

    public static class VersionErrorTransform extends PrivacyTransform {
        @Override public String version() { throw new AssertionError("version failure first-secret"); }
    }

    /** PLAN §7.4: changed fingerprints rebuild online, are refused offline and cannot share an open cache. */
    @Test void transformFingerprintsGuardCacheReuse() throws Exception {
        String topic = topic("compact"), table = StorageDdl.quote(topic);
        TestStack.send(topic, 0, null, "pii-key".getBytes(), "{\"email\":\"private-email\"}".getBytes(), new RecordHeaders());
        var p = settings(); p.setProperty("topic." + topic + ".mask", "_key,email");
        assertTrue(assertThrows(SQLException.class, () -> DriverManager.getConnection(URL, p)).getMessage().contains("mask.secret"));
        p.setProperty("mask.secret", "first-secret"); p.setProperty("topic." + topic + ".transform", "missing.Transform");
        assertTrue(assertThrows(SQLException.class, () -> DriverManager.getConnection(URL, p)).getMessage().contains("missing.Transform"));
        p.setProperty("topic." + topic + ".transform", VersionErrorTransform.class.getName());
        var versionError = assertThrows(SQLException.class, () -> DriverManager.getConnection(URL, p));
        assertEquals("Configuring record transform for " + topic + ": version failure [hidden]", versionError.getMessage());
        assertNotNull(versionError.getCause());
        assertFalse(versionError.getCause().getMessage().contains("first-secret"));
        p.setProperty("topic." + topic + ".transform", PrivacyTransform.class.getName());
        String first; long generation;
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var r = s.executeQuery("SELECT email FROM " + table)) { assertTrue(r.next()); first = r.getString(1); }
            try (var r = s.executeQuery("SELECT id FROM \"$topics\"")) { assertTrue(r.next()); generation = r.getLong(1); }
            var other = (Properties) p.clone(); other.setProperty("mask.secret", "second-secret");
            assertTrue(assertThrows(SQLException.class, () -> DriverManager.getConnection(URL, other)).getMessage().contains("different record transform"));
        }
        p.setProperty("mask.secret", "second-secret"); p.setProperty("offline", "true");
        byte[] before = java.nio.file.Files.readAllBytes(Path.of(p.getProperty("cache.path")));
        assertTrue(assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:kafcacheql://", p)).getMessage().contains("record transform settings"));
        assertArrayEquals(before, java.nio.file.Files.readAllBytes(Path.of(p.getProperty("cache.path"))));
        p.setProperty("offline", "false");
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var r = s.executeQuery("SELECT email FROM " + table)) { assertTrue(r.next()); assertNotEquals(first, r.getString(1)); }
            try (var r = s.executeQuery("SELECT id FROM \"$topics\"")) { assertTrue(r.next()); assertTrue(r.getLong(1) > generation); generation = r.getLong(1); }
        }
        PrivacyTransform.revision = "2";
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var r = s.executeQuery("SELECT email FROM " + table)) { assertTrue(r.next()); }
            try (var r = s.executeQuery("SELECT id FROM \"$topics\"")) { assertTrue(r.next()); assertTrue(r.getLong(1) > generation); generation = r.getLong(1); }
        }
        p.setProperty("offline", "true");
        try (var c = DriverManager.getConnection("jdbc:kafcacheql://localhost:1", p); var s = c.createStatement();
             var r = s.executeQuery("SELECT email FROM " + table)) { assertTrue(r.next()); assertNotEquals("private-email", r.getString(1)); }
        finally { PrivacyTransform.revision = "1"; }
        p.setProperty("offline", "false"); p.remove("topic." + topic + ".mask"); p.remove("topic." + topic + ".transform");
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var r = s.executeQuery("SELECT email FROM " + table)) { assertTrue(r.next()); assertEquals("private-email", r.getString(1)); }
            try (var r = s.executeQuery("SELECT id FROM \"$topics\"")) { assertTrue(r.next()); assertTrue(r.getLong(1) > generation); }
        }
    }

    /** 0019d / PLAN §5.3: opening unreachable brokers fails after 15 s by default;
     * a user-set default.api.timeout.ms replaces the opening limit, and the error names the brokers.
     */
    @Test void unreachableBrokersRespectTheOpeningTimeout() throws Exception {
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        String brokers = "127.0.0.1:" + port;
        for (int timeout : List.of(15_000, 5_000, 30_000)) {
            Properties p = settings();
            if (timeout != 15_000) p.setProperty("default.api.timeout.ms", Integer.toString(timeout));
            // Kafka requires an explicit API timeout to be at least its request timeout.
            if (timeout == 5_000) p.setProperty("request.timeout.ms", "1000");
            long start = System.nanoTime();
            SQLException error = assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:kafcacheql://" + brokers, p));
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println("Opening timeout " + timeout + " ms: failed after " + elapsed + " ms: " + error.getMessage());
            assertAll(
                    () -> assertEquals("08006", error.getSQLState()),
                    () -> assertTrue(error.getMessage().contains(brokers), error.getMessage()),
                    () -> assertTrue(elapsed >= timeout - 1000 && elapsed < timeout + 4000,
                            "opening took " + elapsed + " ms, expected about " + timeout + " ms"));
        }
    }

    /** 0016c / PLAN §10: a fixed set of consumers fetches the waiting partitions, feeding one writer;
     * each batch has its own generation-checked transaction and ranges. §10.1: one deadline/token
     * stops the whole execution; §10: batches already committed are kept after cancellation.
     * §10: each owner decodes with its own decoder/reader; the shared registry loads an ID once.
     */
    @Test void parallelFetchCoversAllPartitionsAndCancellationKeepsCommittedBatches() throws Exception {
        String topic = TestStack.uniqueTopic("jdbc-parallel"), table = StorageDdl.quote(topic);
        TestStack.createTopic(topic, 6, Map.of());
        var schema = new Schema.Parser().parse(VALUE.toString().replace("JdbcValue", "Parallel_" + topic.replace('-', '_')));
        String schemaId = null;
        for (int partition = 0; partition < 6; partition++) for (int offset = 0; offset < 12; offset++) {
            var value = new GenericData.Record(schema); value.put("n", partition * 12 + offset); value.put("ok", true);
            var headers = new RecordHeaders();
            byte[] bytes = TestStack.serializeAvro(topic, headers, value, TestStack.SchemaIdIn.PREFIX, false);
            schemaId = Integer.toString(ByteBuffer.wrap(bytes, 1, 4).getInt());
            TestStack.send(topic, partition, null, TestStack.utf8("k" + offset), bytes, headers);
        }
        var p = settings(); p.setProperty("client.id", topic); p.setProperty("sync.fetch.parallelism", "4");
        p.setProperty("sync.batch.records", "2"); p.setProperty("max.poll.records", "2");
        try (var logs = LogCapture.capture(); var registry = new CountingRegistry()) {
            p.setProperty("schema.registry.url", registry.url());
            String expectedSchema = schemaId;
            for (boolean cancel : List.of(false, true)) {
                registry.reset();
                var schemaRequested = new CountDownLatch(1); var schemaRelease = new CountDownLatch(1);
                registry.beforeResponse(() -> {
                    schemaRequested.countDown();
                    try { schemaRelease.await(); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                });
                p.setProperty("cache.path", dir.resolve(cancel ? "cancel.db" : "complete.db").toString());
                var held = new CountDownLatch(1); var release = new CountDownLatch(1);
                var once = new AtomicBoolean();
                try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
                     var control = CacheContext.acquire(DriverConfig.load(URL, p));
                     var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
                    control.hooks(new SyncWorker.Hooks() {
                        @Override public void afterCommit(SyncBatch batch) throws SQLException {
                            assertTrue(Thread.currentThread().getName().startsWith("kafcacheql-sync-"), "only the worker commits");
                            if (once.compareAndSet(false, true)) { held.countDown(); await(release); }
                        }
                    });
                    var querying = tasks.submit(() -> {
                        try (var rows = s.executeQuery("SELECT n FROM " + table + " ORDER BY _partition,_offset")) { return values(rows); }
                    });
                    try {
                        try {
                            assertTrue(schemaRequested.await(10, TimeUnit.SECONDS));
                            TestStack.await(() -> Thread.getAllStackTraces().entrySet().stream().filter(entry ->
                                    entry.getKey().getName().startsWith("kafcacheql-fetch-" + topic)
                                    && Arrays.stream(entry.getValue()).anyMatch(frame -> frame.getClassName().equals(
                                            "io.github.ifnesi.kafcacheql.schema.SchemaRegistry") && frame.getMethodName().equals("resolve")))
                                    .count() == 4, "four owners resolving the same new schema together");
                        } finally { schemaRelease.countDown(); }
                        assertTrue(held.await(10, TimeUnit.SECONDS));
                        TestStack.await(() -> consumerBeans(topic).size() == 4, "four consumers for six waiting partitions");
                        if (cancel) {
                            s.cancel();
                            var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> querying.get(2, TimeUnit.SECONDS));
                            assertInstanceOf(SQLException.class, failure.getCause());
                            assertEquals("57014", ((SQLException) failure.getCause()).getSQLState());
                        }
                    } finally { release.countDown(); }
                    long generation = control.read(r -> TopicRegistry.generation(r, topic).orElseThrow());
                    if (cancel) {
                        // A writer barrier confirms cancellation has released the worker.
                        try (var execution = new Execution(java.time.Duration.ofSeconds(5))) { control.write(topic, generation, execution, w -> null); }
                        assertEquals(2, cachedCount(control, topic));
                        assertEquals(2L, control.<Long>read(r -> {
                            try (var q = r.createStatement(); var rows = q.executeQuery("SELECT sum(end_offset-start_offset) FROM \"$ranges\"")) {
                                assertTrue(rows.next()); return rows.getLong(1);
                            }
                        }).longValue());
                        assertFalse(Thread.getAllStackTraces().entrySet().stream().anyMatch(entry ->
                                entry.getKey().getName().startsWith("kafcacheql-fetch-" + topic)
                                && Arrays.stream(entry.getValue()).anyMatch(frame -> frame.getMethodName().equals("fetchRange"))),
                                "no consumer is still fetching after the cancelled request drains");
                        control.hooks(new SyncWorker.Hooks() {});
                        try (var rows = s.executeQuery("SELECT n FROM " + table + " ORDER BY _partition,_offset")) {
                            assertEquals(java.util.stream.IntStream.range(0, 72).boxed().toList(), values(rows));
                        }
                    } else assertEquals(java.util.stream.IntStream.range(0, 72).boxed().toList(), querying.get(10, TimeUnit.SECONDS));
                    control.read(r -> {
                        try (var q = r.createStatement(); var rows = q.executeQuery("SELECT partition_no,start_offset,end_offset FROM \"$ranges\" ORDER BY partition_no")) {
                            for (int partition = 0; partition < 6; partition++) {
                                assertTrue(rows.next()); assertEquals(partition, rows.getInt(1)); assertEquals(0, rows.getLong(2)); assertEquals(12, rows.getLong(3));
                            }
                            assertFalse(rows.next());
                        }
                        return null;
                    });
                    assertEquals(1, registry.calls(), "one registry request for the ID shared by all six partitions: " + registry.paths());
                    assertEquals(List.of(expectedSchema), control.read(r -> {
                        var refs = new ArrayList<String>();
                        try (var q = r.createStatement(); var rows = q.executeQuery("SELECT ref FROM \"$schemas\"")) {
                            while (rows.next()) refs.add(rows.getString(1));
                        }
                        return refs;
                    }), "the writer persisted the schema with the committed records");
                    // Like sync.batch.records, a second lease may choose a different parallelism.
                    var other = new Properties(); other.putAll(p); other.setProperty("sync.fetch.parallelism", "1");
                    try (var connection = DriverManager.getConnection(URL, other); var statement = connection.createStatement();
                         var rows = statement.executeQuery("SELECT count(*) FROM " + table)) { assertTrue(rows.next()); assertEquals(72, rows.getInt(1)); }
                } finally { release.countDown(); schemaRelease.countDown(); }
                assertTrue(consumerBeans(topic).isEmpty(), "last connection closes every owned consumer");
            }
            assertTrue(logs.lines().stream().noneMatch(line -> line.startsWith("WARN ")), logs.lines().toString());
        }
    }

    private static Set<javax.management.ObjectName> consumerBeans(String clientId) {
        try { return java.lang.management.ManagementFactory.getPlatformMBeanServer().queryNames(
                new javax.management.ObjectName("kafka.consumer:type=app-info,id=" + clientId + "*"), null); }
        catch (javax.management.MalformedObjectNameException e) { throw new IllegalArgumentException(e); }
    }

    /** 0016b / PLAN §10: used SQLite pages, oldest append records across topics/partitions first,
     * then whole compacted topics in last-query order; evicted ranges can be fetched again.
     * Eviction waits for the checked reader snapshot, so that query still sees all its records.
     * Append cache retention expires old timestamps independently of Kafka and the size limit;
     * unset means no age expiry, compacted state stays whole, and expired ranges can be fetched again.
     * The used-page limit also applies to cache-only queries after reopening an oversized cache.
     */
    @Test void sizeLimitEvictsOldestAppendRowsThenWholeCompactedTopicsAndRefetches() throws Exception {
        String a = topic("delete"), b = topic("delete"), cold = topic("compact"), hot = topic("compact");
        // Two partitions and out-of-order timestamps ensure eviction cannot just trim an offset prefix.
        TestStack.addPartitions(a, 2); TestStack.addPartitions(b, 2);
        String payload = "x".repeat(32_000);
        long now = TestStack.recent(0), limit = 512 * 1024;
        for (String topic : List.of(cold, hot)) for (int i = 0; i < 5; i++)
            TestStack.send(topic, 0, now + i, TestStack.utf8("k" + i), TestStack.utf8(payload), null);
        int[] ranks = {9, 1, 7, 3, 11, 5};
        for (int i = 0; i < ranks.length; i++) for (String topic : List.of(a, b))
            TestStack.send(topic, i % 2, now + ranks[i] + (topic.equals(b) ? 1 : 0),
                    TestStack.utf8("k" + i), TestStack.utf8(payload), null);
        var p = settings(); p.setProperty("cache.max.bytes", Long.toString(limit));
        p.setProperty("sync.batch.records", "2"); p.setProperty("max.poll.records", "2");
        try (var c = DriverManager.getConnection(URL, p); var cache = CacheContext.acquire(DriverConfig.load(URL, p));
             var s = c.createStatement()) {
            for (String topic : List.of(cold, hot)) try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(topic))) {
                assertEquals(List.of(0, 1, 2, 3, 4), values(r));
            }
            assertTrue(usedBytes(cache) < limit, "compacted data alone initially fits");
            try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(a) + " UNION ALL SELECT _offset FROM " + StorageDdl.quote(b))) {
                int count = 0; while (r.next()) count++; assertEquals(12, count, "the reader survives eviction of its own rows");
            }
            TestStack.await(() -> usedBytes(cache) <= limit, "append eviction reaches the used-page limit");
            var remaining = cache.read(reader -> {
                var times = new ArrayList<Long>();
                for (String topic : List.of(a, b)) {
                    long id = TopicRegistry.generation(reader, topic).orElseThrow();
                    try (var q = reader.createStatement(); var r = q.executeQuery("SELECT _timestamp FROM " + StorageDdl.quote(StorageDdl.tableName(id)))) {
                        while (r.next()) times.add(r.getLong(1) - now);
                    }
                }
                Collections.sort(times); return times;
            });
            assertFalse(remaining.isEmpty()); assertTrue(remaining.size() < 12);
            assertEquals(java.util.stream.LongStream.rangeClosed(13 - remaining.size(), 12).boxed().toList(), remaining);
            for (String topic : List.of(cold, hot)) assertEquals(5, cachedCount(cache, topic));
            cache.read(reader -> {
                try (var q = reader.createStatement(); var r = q.executeQuery("SELECT max(pruned_below) FROM \"$partitions\"")) {
                    assertTrue(r.next()); assertEquals(0, r.getLong(1));
                }
                return null;
            });
            var fetched = new AtomicInteger();
            cache.hooks(new SyncWorker.Hooks() { @Override public void afterCommit(SyncBatch batch) { fetched.addAndGet(batch.records().size()); } });
            try (var r = s.executeQuery("SELECT _timestamp FROM " + StorageDdl.quote(a) + " WHERE _partition=1 AND _offset>=0 AND _offset<1")) {
                assertTrue(r.next()); assertEquals(now + 1, r.getLong(1)); assertFalse(r.next());
            }
            assertEquals(1, fetched.get(), "the evicted offset is fetched again, without a pruning floor blocking it");
            TestStack.await(() -> usedBytes(cache) <= limit, "refetch eviction completes");
            long hotId = cache.read(reader -> TopicRegistry.generation(reader, hot).orElseThrow());
            // cold was created/queried first, but touching it now makes hot the least recently queried.
            try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(cold))) { assertEquals(5, values(r).size()); }
            TestStack.await(() -> cacheLastQuery(cache, cold) > cacheLastQuery(cache, hot), "last query timestamps are persisted");
            for (int i = 5; i < 11; i++) TestStack.send(cold, 0, now + i,
                    TestStack.utf8("k" + i), TestStack.utf8(payload), null);
            try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(cold))) { assertEquals(11, values(r).size()); }
            TestStack.await(() -> cacheDeleted(cache, hot), "the least recently queried compacted topic is evicted whole");
            assertEquals(0, cachedCount(cache, a)); assertEquals(0, cachedCount(cache, b)); assertEquals(11, cachedCount(cache, cold));
            assertTrue(usedBytes(cache) <= limit);
            cache.read(reader -> { assertTrue(TopicRegistry.resolve(reader, hot).isEmpty()); return null; });
            fetched.set(0);
            try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(hot))) { assertEquals(List.of(0, 1, 2, 3, 4), values(r)); }
            assertEquals(5, fetched.get());
            assertTrue(cache.read(reader -> TopicRegistry.generation(reader, hot).orElseThrow()) > hotId);
            TestStack.await(() -> cacheDeleted(cache, cold), "refetched compacted state evicts the older whole topic");
            assertEquals(5, cachedCount(cache, hot)); assertTrue(usedBytes(cache) <= limit);
        }
        // No record or whole compacted topic can reclaim the cache's own metadata pages.
        p.setProperty("cache.max.bytes", "1");
        TestStack.send(hot, 0, now + 20, TestStack.utf8("last"), TestStack.utf8(payload), null);
        try (var logs = LogCapture.capture(); var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
             var cache = CacheContext.acquire(DriverConfig.load(URL, p)); var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(hot))) {
            assertEquals(6, values(r).size());
            TestStack.await(() -> logs.lines().stream().anyMatch(line -> line.startsWith("WARN ") && line.contains("cache.max.bytes")), "one warning when nothing can be evicted");
            assertEquals(1, logs.lines().stream().filter(line -> line.startsWith("WARN ") && line.contains("cache.max.bytes")).count());
            assertTrue(usedBytes(cache) > 1);
        }
        // Cancellation commits a batch but opens no reader snapshot. Reopening must still enforce the limit.
        p.setProperty("cache.path", dir.resolve("reopened.db").toString());
        String cachedWindow = "SELECT _offset FROM " + StorageDdl.quote(a) + " WHERE _partition=0 AND _offset>=0 AND _offset<2";
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
             var cache = CacheContext.acquire(DriverConfig.load(URL, p))) {
            cache.hooks(new SyncWorker.Hooks() { @Override public void afterCommit(SyncBatch batch) throws SQLException { s.cancel(); } });
            assertEquals("57014", assertThrows(SQLException.class, () -> s.executeQuery(cachedWindow)).getSQLState());
            assertEquals(2, cachedCount(cache, a)); assertTrue(usedBytes(cache) > 1);
        }
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
             var cache = CacheContext.acquire(DriverConfig.load(URL, p))) {
            var fetched = new AtomicInteger();
            cache.hooks(new SyncWorker.Hooks() { @Override public void afterCommit(SyncBatch batch) { fetched.addAndGet(batch.records().size()); } });
            try (var r = s.executeQuery(cachedWindow)) { assertEquals(List.of(0, 1), values(r)); }
            assertEquals(0, fetched.get(), "the reopened query reads only cached records");
            TestStack.await(() -> {
                try { return cachedCount(cache, a) == 0; }
                catch (SQLException e) { throw new IllegalStateException(e); }
            }, "a cache-only query evicts data left over the limit by an earlier process");
        }
        // Reuse the Kafka data in a fresh, unlimited cache: old timestamps occur between newer offsets.
        long old = now - TimeUnit.HOURS.toMillis(2);
        for (String topic : List.of(a, b)) {
            TestStack.send(topic, 0, old, TestStack.utf8("old"), TestStack.utf8(payload), null);
            TestStack.send(topic, 0, now, TestStack.utf8("new"), TestStack.utf8(payload), null);
        }
        TestStack.send(hot, 0, old, TestStack.utf8("old"), TestStack.utf8(payload), null);
        p.remove("cache.max.bytes"); p.setProperty("cache.path", dir.resolve("age.db").toString());
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
             var cache = CacheContext.acquire(DriverConfig.load(URL, p))) {
            for (String topic : List.of(a, b, hot)) {
                try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(topic))) {
                    assertEquals(topic.equals(hot) ? 7 : 8, values(r).size());
                }
                assertEquals(topic.equals(hot) ? 7 : 8, cachedCount(cache, topic), "age expiry is unset by default");
            }
        }
        p.setProperty("cache.retention.hours", "1");
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
             var cache = CacheContext.acquire(DriverConfig.load(URL, p))) {
            var fetched = new AtomicInteger();
            cache.hooks(new SyncWorker.Hooks() { @Override public void afterCommit(SyncBatch batch) { fetched.addAndGet(batch.records().size()); } });
            try (var r = s.executeQuery("SELECT _offset FROM " + StorageDdl.quote(a)
                    + " UNION ALL SELECT _offset FROM " + StorageDdl.quote(b)
                    + " UNION ALL SELECT _offset FROM " + StorageDdl.quote(hot))) {
                assertEquals(23, values(r).size(), "age expiry also waits for the query snapshot");
            }
            var different = new Properties(); different.putAll(p); different.setProperty("cache.retention.hours", "2");
            assertTrue(assertThrows(SQLException.class, () -> DriverManager.getConnection(URL, different))
                    .getMessage().contains("cache.retention.hours"));
            assertEquals(0, fetched.get(), "age expiry runs even when a query fetches no new records");
            TestStack.await(() -> {
                try { return cachedCount(cache, a) == 7 && cachedCount(cache, b) == 7; }
                catch (SQLException e) { throw new IllegalStateException(e); }
            }, "only old append timestamps expire, without a size limit");
            assertEquals(7, cachedCount(cache, hot), "old compacted values never expire individually");
            try (var r = s.executeQuery("SELECT _timestamp FROM " + StorageDdl.quote(a)
                    + " WHERE _partition=0 AND _offset>=3 AND _offset<4")) {
                assertTrue(r.next()); assertEquals(old, r.getLong(1)); assertFalse(r.next());
            }
            assertEquals(1, fetched.get(), "Kafka still has the age-expired record and it can be fetched again");
            cache.read(reader -> {
                try (var q = reader.createStatement(); var r = q.executeQuery("SELECT max(pruned_below) FROM \"$partitions\"")) {
                    assertTrue(r.next()); assertEquals(0, r.getLong(1));
                }
                return null;
            });
        }
    }

    private static long usedBytes(CacheContext.Lease cache) {
        try { return cache.read(reader -> {
            try (var q = reader.createStatement(); var r = q.executeQuery("SELECT (page_count-freelist_count)*page_size FROM pragma_page_count(),pragma_freelist_count(),pragma_page_size()")) {
                r.next(); return r.getLong(1);
            }
        }); } catch (SQLException e) { throw new IllegalStateException(e); }
    }
    private static int cachedCount(CacheContext.Lease cache, String topic) throws SQLException {
        return cache.read(reader -> {
            long id = TopicRegistry.generation(reader, topic).orElseThrow();
            try (var q = reader.createStatement(); var r = q.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(StorageDdl.tableName(id)))) { r.next(); return r.getInt(1); }
        });
    }
    private static boolean cacheDeleted(CacheContext.Lease cache, String topic) {
        try { return cache.read(reader -> TopicRegistry.observe(reader, topic).orElseThrow().deleted()); }
        catch (SQLException e) { throw new IllegalStateException(e); }
    }
    private static long cacheLastQuery(CacheContext.Lease cache, String topic) {
        try { return cache.read(reader -> {
            try (var q = reader.prepareStatement("SELECT last_query_ms FROM \"$topics\" WHERE topic=?")) {
                q.setString(1, topic); try (var r = q.executeQuery()) { r.next(); return r.getLong(1); }
            }
        }); } catch (SQLException e) { throw new IllegalStateException(e); }
    }

    /** 0007c / PLAN §6.5: a newer compacted value replaces its key; only a Kafka null deletes it.
     * §7.1: failed decoding stores base64 and an error. §9: a cached offset below log start rebuilds
     * a compacted topic, including compact,delete retention without tombstones.
     */
    @Test void compactedRetentionFailedUpdatesAndJsonNull() throws Exception {
        String topic = TestStack.uniqueTopic("jdbc-compact-edges"), table = StorageDdl.quote(topic);
        TestStack.createTopic(topic, 1, Map.of("cleanup.policy", "compact,delete", "retention.ms", "-1",
                "segment.ms", "1000", "file.delete.delay.ms", "0"));
        var p = settings(); p.setProperty("topic." + topic + ".value.format", "json");
        for (String key : List.of("bad", "json-null", "expired")) {
            TestStack.sendJson(topic, key, "{\"status\":\"old\"}", TestStack.recent(0));
        }
        try (var c = DriverManager.getConnection(URL, p); var statement = c.createStatement();
             var control = CacheContext.acquire(DriverConfig.load(URL, p))) {
            try (var rows = statement.executeQuery("SELECT count(*) FROM " + table)) { assertTrue(rows.next()); assertEquals(3, rows.getInt(1)); }
            TestStack.sendJson(topic, "bad", "not-json", TestStack.recent(1));
            TestStack.sendJson(topic, "json-null", "null", TestStack.recent(2));
            try (var rows = statement.executeQuery("SELECT _key ->> '$',_offset,_value,_decode_error FROM " + table + " ORDER BY _offset")) {
                assertTrue(rows.next()); assertEquals("expired", rows.getString(1)); assertEquals(2, rows.getLong(2));
                assertEquals("{\"status\":\"old\"}", rows.getString(3)); assertNull(rows.getString(4));
                assertTrue(rows.next()); assertEquals("bad", rows.getString(1)); assertEquals(3, rows.getLong(2));
                assertEquals("\"" + Base64.getEncoder().encodeToString(TestStack.utf8("not-json")) + "\"", rows.getString(3));
                assertNotNull(rows.getString(4));
                assertTrue(rows.next()); assertEquals("json-null", rows.getString(1)); assertEquals(4, rows.getLong(2));
                assertEquals("null", rows.getString(3)); assertFalse(rows.wasNull()); assertNull(rows.getString(4));
                assertFalse(rows.next());
            }
            // A future timestamp rolls a fresh segment and keeps its row while the old segment expires.
            TestStack.sendJson(topic, "tail", "{\"status\":\"kept\"}", System.currentTimeMillis() + 120_000);
            try (var rows = statement.executeQuery("SELECT count(*) FROM " + table)) { assertTrue(rows.next()); assertEquals(4, rows.getInt(1)); }
            long generation = control.read(r -> io.github.ifnesi.kafcacheql.cache.TopicRegistry.generation(r, topic).orElseThrow());
            var resource = new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic);
            TestStack.admin().incrementalAlterConfigs(Map.of(resource, List.of(new org.apache.kafka.clients.admin.AlterConfigOp(
                    new org.apache.kafka.clients.admin.ConfigEntry("retention.ms", "1000"), org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET))))
                    .all().get(10, TimeUnit.SECONDS);
            var partition = new org.apache.kafka.common.TopicPartition(topic, 0);
            TestStack.await(() -> {
                try { return TestStack.admin().listOffsets(Map.of(partition, OffsetSpec.earliest())).all().get(10, TimeUnit.SECONDS)
                        .get(partition).offset() == 5; }
                catch (Exception e) { throw new IllegalStateException(e); }
            }, "retention removes compact,delete records without tombstones");
            try (var logs = LogCapture.capture(); var rows = statement.executeQuery("SELECT status,_offset FROM " + table)) {
                assertTrue(rows.next()); assertEquals("kept", rows.getString(1)); assertEquals(5, rows.getLong(2)); assertFalse(rows.next());
                assertTrue(logs.lines().stream().anyMatch(line -> line.startsWith("WARN ") && line.contains(topic)
                        && line.contains("log start")), logs.lines().toString());
            }
            assertTrue(control.read(r -> io.github.ifnesi.kafcacheql.cache.TopicRegistry.generation(r, topic).orElseThrow()) > generation);
        }
    }

    /** 0012b / PLAN §12.1–12.2: Refresh lists Kafka topics and queues missing cached topics for deletion
     * without waiting or making another Kafka call. §10.1: an older observation cannot delete a newer generation.
     * §9: deletion drops storage, view, ranges and partitions, retaining the generation with deleted_ms.
     * §10.1: busy background writes are logged and dropped; closing never waits for another process's lock.
     */
    @SuppressWarnings("try") // Explicit close is checked while the other process still holds its lock.
    @Test void metadataRefreshDropsMissingTopicsButNotRecreatedGenerations() throws Exception {
        String base = TestStack.uniqueTopic("jdbc-refresh"), gone = base + "-gone", race = base + "-race", busy = base + "-busy";
        for (String topic : List.of(gone, race, busy)) {
            TestStack.createTopic(topic, 1, Map.of()); send(topic, 1, null);
        }
        var p = settings(); p.setProperty("topics.include", Pattern.quote(base) + ".*");
        p.setProperty("topics.exclude", Pattern.quote(busy));
        var config = DriverConfig.load(URL, p);
        try (var c = DriverManager.getConnection(URL, p); var control = CacheContext.acquire(config);
             var clients = KafkaClients.acquire(config); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String topic : List.of(gone, race, busy)) {
                try (var s = c.createStatement(); var r = s.executeQuery("SELECT n FROM " + StorageDdl.quote(topic))) { assertEquals(List.of(1), values(r)); }
            }
            long goneId = control.read(r -> TopicRegistry.generation(r, gone).orElseThrow());
            long raceId = control.read(r -> TopicRegistry.generation(r, race).orElseThrow());
            long busyId = control.read(r -> TopicRegistry.generation(r, busy).orElseThrow());
            var paused = new CountDownLatch(1); var release = new CountDownLatch(1);
            control.hooks(new SyncWorker.Hooks() {
                @Override public void beforeCommit(SyncBatch batch) throws SQLException {
                    if (!batch.plan().topic().name().equals(busy)) return;
                    paused.countDown();
                    try { if (!release.await(20, TimeUnit.SECONDS)) throw new SQLException("Test did not release worker"); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException(e); }
                }
            });
            send(busy, 2, null);
            var syncing = executor.submit(() -> {
                try (var other = DriverManager.getConnection(URL, p); var s = other.createStatement();
                     var r = s.executeQuery("SELECT n FROM " + StorageDdl.quote(busy) + " ORDER BY _offset")) { return values(r); }
            });
            try {
                assertTrue(paused.await(10, TimeUnit.SECONDS));
                TestStack.deleteTopic(gone); TestStack.deleteTopic(race);
                Field admin = KafkaClients.class.getDeclaredField("admin"); admin.setAccessible(true);
                Admin original = (Admin) admin.get(clients.clients());
                var calls = new ArrayList<String>();
                admin.set(clients.clients(), Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (self, method, args) -> {
                    calls.add(method.getName());
                    try { return method.invoke(original, args); }
                    catch (InvocationTargetException e) { throw e.getCause(); }
                }));
                try {
                    var refresh = executor.submit(() -> {
                        var names = new HashSet<String>();
                        try (var rows = c.getMetaData().getTables(null, null, "%", null)) { while (rows.next()) names.add(rows.getString("TABLE_NAME")); }
                        return names;
                    });
                    assertEquals(Set.of(), refresh.get(5, TimeUnit.SECONDS));
                    assertEquals(List.of("listTopics"), calls);
                    assertEquals(1, release.getCount(), "Refresh returned while the worker remained paused");
                } finally { admin.set(clients.clients(), original); }
                assertFalse(control.<Boolean>read(r -> TopicRegistry.observe(r, gone).orElseThrow().deleted()));
                TestStack.createTopic(race, 1, Map.of()); send(race, 9, null);
                Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                        System.getProperty("java.class.path"), io.github.ifnesi.kafcacheql.sql.PipelineProcess.class.getName(),
                        control.path().toString(), race, "jdbc").redirectError(ProcessBuilder.Redirect.INHERIT).start();
                try {
                    assertTrue(child.waitFor(15, TimeUnit.SECONDS)); assertEquals(0, child.exitValue());
                    assertEquals("ROW 9", new String(child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim());
                } finally { child.destroyForcibly(); }
                assertTrue(control.read(r -> TopicRegistry.generation(r, race).orElseThrow()) > raceId);
            } finally { release.countDown(); }
            assertEquals(List.of(1, 2), syncing.get(10, TimeUnit.SECONDS));
            control.hooks(new SyncWorker.Hooks() {});
            TestStack.await(() -> {
                try { return control.read(r -> TopicRegistry.observe(r, gone).orElseThrow().deleted()); }
                catch (SQLException e) { throw new IllegalStateException(e); }
            }, "Refresh drops the missing cached topic");
            // The other queued drop was already scheduled before the missing topic's deletion committed.
            try (var execution = new Execution(java.time.Duration.ofSeconds(10))) { control.write(busy, busyId, execution, w -> null); }
            var deleted = control.read(r -> TopicRegistry.observe(r, gone).orElseThrow());
            assertEquals(goneId, deleted.id()); assertTrue(deleted.deleted());
            control.read(r -> {
                assertTrue(TopicRegistry.resolve(r, gone).isEmpty());
                try (var s = r.createStatement(); var rows = s.executeQuery("SELECT count(*) FROM sqlite_schema WHERE name="
                        + "'" + StorageDdl.tableName(goneId) + "'")) { assertTrue(rows.next()); assertEquals(0, rows.getInt(1)); }
                for (String table : List.of("$ranges", "$partitions")) {
                    try (var s = r.createStatement(); var rows = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(table) + " WHERE topic_ref=" + goneId)) {
                        assertTrue(rows.next()); assertEquals(0, rows.getInt(1));
                    }
                }
                assertFalse(TopicRegistry.observe(r, race).orElseThrow().deleted());
                assertTrue(TopicRegistry.generation(r, race).orElseThrow() > raceId);
                assertEquals(busyId, TopicRegistry.generation(r, busy).orElseThrow(), "metadata filters do not delete a listed topic");
                return null;
            });
            try (var rows = c.getMetaData().getColumns(null, null, gone, null)) { assertFalse(rows.next()); }
            try (var s = c.createStatement(); var rows = s.executeQuery("SELECT n FROM " + StorageDdl.quote(race))) { assertEquals(List.of(9), values(rows)); }
            TestStack.deleteTopic(race);
            Process holder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                    System.getProperty("java.class.path"), io.github.ifnesi.kafcacheql.sync.SyncProcess.class.getName(),
                    "lock", control.path().toString(), race, TestStack.BOOTSTRAP)
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            Future<?> close = null;
            try (var logs = LogCapture.capture()) {
                assertEquals("LOCKED", executor.submit(() -> holder.inputReader().readLine()).get(15, TimeUnit.SECONDS));
                try (var rows = c.getMetaData().getTables(null, null, race, null)) { assertFalse(rows.next()); }
                // Ensure the drop has started before closing: otherwise close could discard it unattempted.
                TestStack.await(() -> logs.lines().stream().anyMatch(line -> line.contains("Dropped background cache write"))
                        || Thread.getAllStackTraces().entrySet().stream().anyMatch(entry ->
                            entry.getKey().getName().equals("kafcacheql-sync-" + control.path().getFileName())
                            && Arrays.stream(entry.getValue()).anyMatch(frame -> frame.getClassName().equals(TopicRegistry.class.getName())
                                    && frame.getMethodName().equals("delete"))), "the background drop attempts the held write lock");
                close = executor.submit(() -> { c.close(); control.close(); return null; });
                close.get(2, TimeUnit.SECONDS);
                assertTrue(holder.isAlive(), "close finishes while the competing process still holds its lock");
                assertTrue(logs.lines().stream().anyMatch(line -> line.startsWith("WARN ") && line.contains("Dropped background cache write")));
            } finally {
                holder.getOutputStream().write('\n'); holder.getOutputStream().flush();
                try { assertTrue(holder.waitFor(5, TimeUnit.SECONDS)); assertEquals(0, holder.exitValue()); }
                finally { holder.destroyForcibly(); }
                if (close != null) close.get(10, TimeUnit.SECONDS);
            }
        }
    }

    /** 0016d / PLAN §8.4: a simple offset-descending literal LIMIT replaces the default window,
     * per selected partition. Other predicates keep the default; invisible offsets extend by the shortfall.
     * 0019a / §8.4: the built-in default reads everything Kafka holds, with no time or record cap or warning.
     */
    @Test void tailQueriesFetchOnlySelectedTailsAndExtendPastTransactionGaps() throws Exception {
        String topic = TestStack.uniqueTopic("jdbc-tail"), table = StorageDdl.quote(topic);
        TestStack.createTopic(topic, 3, Map.of("cleanup.policy", "delete"));
        long old = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        int[] lengths = {5, 7, 6};
        for (int partition = 0; partition < lengths.length; partition++) {
            for (int n = 0; n < lengths[partition]; n++) TestStack.send(topic, partition, old,
                    TestStack.utf8("key" + n), TestStack.utf8("value" + n), null);
        }
        var p = settings(); p.remove("schema.registry.url"); p.remove("append.default.window");
        p.setProperty("append.default.window.max.records", "1");
        try (var c = DriverManager.getConnection(URL, p); var statement = c.createStatement();
             var rows = statement.executeQuery("SELECT _partition,_offset FROM " + table + " ORDER BY _partition,_offset")) {
            for (int partition = 0; partition < lengths.length; partition++) for (int offset = 0; offset < lengths[partition]; offset++) {
                assertTrue(rows.next()); assertEquals(partition, rows.getInt(1)); assertEquals(offset, rows.getLong(2));
            }
            assertFalse(rows.next()); assertNull(statement.getWarnings());
        }
        p.setProperty("append.default.window", "30s");
        for (boolean selected : List.of(false, true)) {
            p.setProperty("cache.path", dir.resolve("tail-" + selected + ".db").toString());
            var fetched = new ArrayList<String>();
            try (var c = DriverManager.getConnection(URL, p); var cache = CacheContext.acquire(DriverConfig.load(URL, p));
                 var statement = c.createStatement()) {
                cache.hooks(new SyncWorker.Hooks() {
                    @Override public void afterCommit(SyncBatch batch) {
                        batch.records().forEach(r -> fetched.add(batch.partition() + ":" + r.offset()));
                    }
                });
                String sql = "SELECT t._partition," + (selected ? "abs(t._offset)" : "t._offset") + " FROM " + table + " t"
                        + (selected ? " WHERE t._partition IN (0,2)" : "")
                        + " ORDER BY " + (selected ? "t._offset" : "_offset") + " DESC LIMIT 2";
                try (var rows = statement.executeQuery(sql)) {
                    var offsets = new ArrayList<Integer>();
                    while (rows.next()) {
                        if (selected) assertNotEquals(1, rows.getInt(1));
                        offsets.add(rows.getInt(2));
                    }
                    assertEquals(selected ? List.of(5, 4) : List.of(6, 5), offsets);
                }
                assertNull(statement.getWarnings());
                assertEquals(selected ? Set.of("0:3", "0:4", "2:4", "2:5")
                        : Set.of("0:3", "0:4", "1:5", "1:6", "2:4", "2:5"), new HashSet<>(fetched));
                assertEquals(selected ? 4 : 6, fetched.size());
                fetched.clear();
                // Already cached tails must not leak into the normal default window.
                for (String fallback : List.of("SELECT _offset FROM " + table + " WHERE _key IS NOT NULL ORDER BY _offset DESC LIMIT 2",
                        "SELECT _offset FROM " + table + " ORDER BY _offset ASC LIMIT 2",
                        "SELECT _offset FROM " + table + " ORDER BY _offset DESC LIMIT 2 OFFSET 1",
                        "SELECT _partition AS _offset FROM " + table + " ORDER BY _offset DESC LIMIT 2",
                        "SELECT DISTINCT _partition FROM " + table + " ORDER BY _offset DESC LIMIT 2",
                        "SELECT _partition FROM " + table + " GROUP BY _partition ORDER BY _offset DESC LIMIT 2",
                        "SELECT row_number() OVER (ORDER BY _offset) FROM " + table + " ORDER BY _offset DESC LIMIT 2",
                        "SELECT _offset FROM " + table + " ORDER BY _offset DESC,_partition LIMIT 2",
                        "SELECT a._offset FROM " + table + " a JOIN " + table + " b ON a._offset=b._offset ORDER BY a._offset DESC LIMIT 2")) {
                    try (var rows = statement.executeQuery(fallback)) { assertFalse(rows.next(), fallback); }
                    assertTrue(warnings(statement).contains("default window applied"), fallback + ": " + warnings(statement));
                }
                try (var prepared = c.prepareStatement("SELECT _offset FROM " + table + " ORDER BY _offset DESC LIMIT ?")) {
                    prepared.setInt(1, 2);
                    try (var rows = prepared.executeQuery()) { assertFalse(rows.next()); }
                    assertTrue(warnings(prepared).contains("default window applied"));
                }
                try (var rows = statement.executeQuery("SELECT count(*) FROM " + table + " ORDER BY _offset DESC LIMIT 2")) {
                    assertTrue(rows.next()); assertEquals(0, rows.getLong(1)); assertFalse(rows.next());
                }
                assertTrue(warnings(statement).contains("default window applied"));
                assertTrue(fetched.isEmpty(), fetched.toString());
            }
        }
        String tx = topic("delete"), txTable = StorageDdl.quote(tx);
        TestStack.inTransaction(tx, true, producer -> {
            for (int n = 0; n < 3; n++) producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                    tx, 0, old, TestStack.utf8("k" + n), TestStack.utf8("v" + n)));
        });
        for (boolean aborted : List.of(false, true)) {
            if (aborted) TestStack.inTransaction(tx, false, producer -> {
                for (int n = 0; n < 3; n++) producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(
                        tx, 0, old, TestStack.utf8("aborted" + n), TestStack.utf8("invisible")));
            });
            p.setProperty("cache.path", dir.resolve("tail-tx-" + aborted + ".db").toString());
            var fetched = new ArrayList<Long>(); var starts = new ArrayList<Long>();
            try (var c = DriverManager.getConnection(URL, p); var cache = CacheContext.acquire(DriverConfig.load(URL, p));
                 var statement = c.createStatement()) {
                cache.hooks(new SyncWorker.Hooks() {
                    @Override public void beforeFetch(io.github.ifnesi.kafcacheql.sync.SyncPlan plan) {
                        starts.add(plan.windows().get(0).span().lowerEndpoint());
                    }
                    @Override public void afterCommit(SyncBatch batch) { batch.records().forEach(r -> fetched.add(r.offset())); }
                });
                try (var rows = statement.executeQuery("SELECT _offset FROM " + txTable + " ORDER BY _offset DESC LIMIT 2")) {
                    assertEquals(List.of(2, 1), values(rows));
                }
                assertNull(statement.getWarnings()); assertEquals(List.of(2L, 1L), fetched);
                assertEquals(aborted ? List.of(6L, 4L, 2L, 1L) : List.of(2L, 1L), starts);
                fetched.clear();
                try (var rows = statement.executeQuery("SELECT _offset FROM " + txTable + " ORDER BY _offset DESC LIMIT 10")) {
                    assertEquals(List.of(2, 1, 0), values(rows));
                }
                assertEquals(List.of(0L), fetched); assertNull(statement.getWarnings());
            }
        }
    }

    /** 0015f / PLAN §7.1–7.3: Confluent handles references and message indexes from each record's
     * prefix/header. §6.6: one message type gets descriptor-derived columns; several get JSON only.
     * §7.2: int64 stays BIGINT even though JsonFormat writes a string; nested values stay JSON.
     */
    @Test void protobufRecordsUseMessageIndexesReferencesAndTypedColumns() throws Exception {
        String single = topic("delete"), mixed = topic("delete"), file = single + "-address.proto";
        String address = "syntax = \"proto3\"; package shop; message Address { string city = 1; }";
        String source = """
                syntax = "proto3";
                package shop;
                import "%s";
                message Other { string note = 1; }
                message Order {
                  int64 order_id = 1;
                  int32 quantity = 2;
                  double ratio = 3;
                  bool ok = 4;
                  string text = 5;
                  enum State { OPEN = 0; CLOSED = 1; }
                  State state = 6;
                  bytes raw = 7;
                  Address address = 8;
                  repeated int32 items = 9;
                  map<string, int64> labels = 10;
                  oneof choice { string choice_text = 11; int64 choice_n = 12; }
                  optional int32 optional_n = 13;
                }
                """.formatted(file);
        try (var rest = new RestService(TestStack.REGISTRY)) {
            rest.registerSchema(address, "PROTOBUF", List.of(), file, false);
        }
        var schema = new ProtobufSchema(source, List.of(new SchemaReference(file, file, 1)), Map.of(file, address), null, null);
        var order = DynamicMessage.newBuilder(schema.toDescriptor("shop.Order"));
        JsonFormat.parser().merge("""
                {"order_id":"9007199254740993","quantity":3,"ratio":1.25,"ok":true,"text":"hello",
                 "state":"CLOSED","raw":"AQI=","address":{"city":"London"},"items":[1,2],
                 "labels":{"stock":"7"},"choice_n":"9"}
                """, order);
        var other = DynamicMessage.newBuilder(schema.toDescriptor("shop.Other"));
        JsonFormat.parser().merge("{\"note\":\"other\"}", other);
        var p = settings(); p.setProperty("topic." + single + ".key.format", "protobuf");
        // Values use auto, keys force protobuf, and Order is the non-default message index [1].
        for (var where : TestStack.SchemaIdIn.values()) sendProtobuf(single, other.build(), order.build(), where);
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var rows = s.executeQuery("SELECT * FROM " + StorageDdl.quote(single) + " ORDER BY _offset")) {
                for (String name : List.of("order_id", "quantity", "choice_n")) assertType(rows, name, JDBCType.BIGINT);
                assertType(rows, "ratio", JDBCType.DOUBLE); assertType(rows, "ok", JDBCType.BOOLEAN);
                for (String name : List.of("key_note", "text", "state", "raw", "address", "items", "labels", "choice_text"))
                    assertType(rows, name, JDBCType.VARCHAR);
                for (int i = 0; i < 2; i++) {
                    assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                    assertEquals(9007199254740993L, rows.getObject("order_id"));
                    assertEquals(3L, rows.getObject("quantity")); assertEquals(9L, rows.getObject("choice_n"));
                    assertEquals(1.25, rows.getObject("ratio")); assertEquals(true, rows.getObject("ok"));
                    assertEquals("other", rows.getString("key_note")); assertEquals("hello", rows.getString("text"));
                    assertEquals("CLOSED", rows.getString("state")); assertEquals("AQI=", rows.getString("raw"));
                    assertEquals("{\"city\":\"London\"}", rows.getString("address")); assertEquals("[1,2]", rows.getString("items"));
                    assertEquals("{\"stock\":\"7\"}", rows.getString("labels")); assertNull(rows.getObject("choice_text"));
                    assertEquals("9007199254740993", JacksonMapper.INSTANCE.readTree(rows.getString("_value")).get("order_id").textValue());
                    assertEquals(i == 1, rows.getString("_value_schema_id").contains("-"));
                }
                assertFalse(rows.next());
            }
            // SQL comparisons use the integer view expression, not JsonFormat's quoted digits.
            try (var rows = s.executeQuery("SELECT order_id FROM " + StorageDdl.quote(single) + " WHERE order_id=9007199254740993")) {
                assertEquals(2, values(rows).size());
            }
            // 0015g / PLAN §7.2: defaults without presence are values; unset optional/message/oneof fields are NULL.
            sendProtobuf(single, DynamicMessage.newBuilder(other.getDescriptorForType()).build(),
                    DynamicMessage.newBuilder(order.getDescriptorForType()).build(), TestStack.SchemaIdIn.HEADER);
            try (var rows = s.executeQuery("SELECT * FROM " + StorageDdl.quote(single) + " WHERE _offset=2")) {
                assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                assertAll(() -> assertEquals(false, rows.getObject("ok")),
                        () -> assertEquals(0L, rows.getObject("quantity")),
                        () -> assertEquals(0L, rows.getObject("order_id")),
                        () -> assertEquals(0.0, rows.getObject("ratio")),
                        () -> assertEquals("", rows.getObject("text")),
                        () -> assertEquals("OPEN", rows.getObject("state")),
                        () -> assertEquals("", rows.getObject("raw")),
                        () -> assertEquals("[]", rows.getObject("items")),
                        () -> assertEquals("{}", rows.getObject("labels")),
                        () -> assertEquals("", rows.getObject("key_note")),
                        () -> assertNull(rows.getObject("optional_n")),
                        () -> assertNull(rows.getObject("address")),
                        () -> assertNull(rows.getObject("choice_text")),
                        () -> assertNull(rows.getObject("choice_n")),
                        () -> assertEquals(JacksonMapper.INSTANCE.readTree("""
                                {"order_id":"0","quantity":0,"ratio":0.0,"ok":false,"text":"","state":"OPEN","raw":"","items":[],"labels":{}}
                                """), JacksonMapper.INSTANCE.readTree(rows.getString("_value"))));
                assertFalse(rows.next());
            }
            sendProtobuf(mixed, null, order.build(), TestStack.SchemaIdIn.PREFIX);
            try (var rows = s.executeQuery("SELECT order_id FROM " + StorageDdl.quote(mixed))) {
                assertTrue(rows.next()); assertEquals(9007199254740993L, rows.getObject(1)); assertFalse(rows.next());
            }
            // Same schema ID, different message index: the existing typed columns must disappear.
            sendProtobuf(mixed, null, other.build(), TestStack.SchemaIdIn.PREFIX);
            try (var rows = s.executeQuery("SELECT * FROM " + StorageDdl.quote(mixed) + " ORDER BY _offset")) {
                assertEquals(9, rows.getMetaData().getColumnCount());
                assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                String id = rows.getString("_value_schema_id");
                assertEquals("London", JacksonMapper.INSTANCE.readTree(rows.getString("_value")).get("address").get("city").textValue());
                assertTrue(rows.next()); assertNull(rows.getString("_decode_error")); assertEquals(id, rows.getString("_value_schema_id"));
                assertEquals("{\"note\":\"other\"}", rows.getString("_value")); assertFalse(rows.next());
            }
            assertEquals(Set.of("_partition", "_offset", "_timestamp", "_key", "_value", "_headers",
                    "_key_schema_id", "_value_schema_id", "_decode_error"), columnTypes(c, mixed).keySet());
            // PLAN §6.6/§7.2: numeric evolution keeps numeric SQL comparisons; unsigned values stay exact;
            // well-known messages follow their JSON representation, including primitive keys and values.
            assertAll(() -> protobufNumericEvolution(s), () -> protobufUnsigned(s), () -> protobufWellKnown(s),
                    () -> protobufWellKnownMix(s), () -> protobufAny(file, address));
        }
    }

    private static void protobufNumericEvolution(Statement statement) throws Exception {
        var checks = new ArrayList<org.junit.jupiter.api.function.Executable>();
        for (String changed : List.of("int32", "double")) checks.add(() -> {
            String topic = topic("delete"), table = StorageDdl.quote(topic);
            try (var rest = new RestService(TestStack.REGISTRY)) { rest.updateCompatibility("NONE", topic + "-value"); }
            // Opposite changes also make this independent of the order in which cached schema IDs sort.
            for (boolean newer : List.of(false, true)) {
                var schema = new ProtobufSchema("syntax=\"proto3\"; package evolution; message Row { "
                        + (newer ? changed : "int64") + " n=1; " + (newer ? "int64" : changed) + " other=2; }");
                var row = DynamicMessage.newBuilder(schema.toDescriptor());
                JsonFormat.parser().merge(newer ? "{\"n\":20,\"other\":\"20\"}" : "{\"n\":\"2\",\"other\":2}", row);
                sendProtobuf(topic, null, row.build(), TestStack.SchemaIdIn.PREFIX);
            }
            try (var rows = statement.executeQuery("SELECT n FROM " + table + " WHERE n>10 ORDER BY _offset")) {
                var values = new ArrayList<Double>(); while (rows.next()) values.add(rows.getDouble(1));
                assertEquals(List.of(20.0), values, changed + " evolution must compare numbers, not quoted digits");
            }
            try (var rows = statement.executeQuery("SELECT n,typeof(n),other,typeof(other) FROM " + table + " ORDER BY _offset")) {
                var type = changed.equals("double") ? JDBCType.DOUBLE : JDBCType.BIGINT;
                assertType(rows, "n", type); assertType(rows, "other", type);
                for (double expected : List.of(2.0, 20.0)) {
                    assertTrue(rows.next()); assertEquals(expected, rows.getDouble(1)); assertEquals(expected, rows.getDouble(3));
                    assertEquals(changed.equals("double") ? "real" : "integer", rows.getString(2));
                    assertEquals(rows.getString(2), rows.getString(4));
                }
                assertFalse(rows.next());
            }
            if (changed.equals("double")) {
                var schema = new ProtobufSchema("syntax=\"proto3\"; package evolution; message Row { double n=1; int64 other=2; }");
                var nonFinite = DynamicMessage.newBuilder(schema.toDescriptor());
                JsonFormat.parser().merge("{\"n\":\"NaN\",\"other\":\"30\"}", nonFinite);
                sendProtobuf(topic, null, nonFinite.build(), TestStack.SchemaIdIn.PREFIX);
                try (var rows = statement.executeQuery("SELECT n FROM " + table + " WHERE _offset=2")) {
                    assertTrue(rows.next()); assertEquals(Double.NaN, rows.getObject(1)); assertEquals("NaN", rows.getString(1));
                }
            }
        });
        assertAll(checks);
    }

    private static void protobufUnsigned(Statement statement) throws Exception {
        String topic = topic("delete");
        var schema = new ProtobufSchema("syntax=\"proto3\"; message Unsigned { uint64 u=1; fixed64 f=2; uint32 u32=3; int64 signed=4; }");
        var value = DynamicMessage.newBuilder(schema.toDescriptor());
        JsonFormat.parser().merge("{\"u\":\"18446744073709551615\",\"f\":\"18446744073709551614\",\"u32\":4294967295,\"signed\":\"9007199254740993\"}", value);
        sendProtobuf(topic, null, value.build(), TestStack.SchemaIdIn.PREFIX);
        try (var rows = statement.executeQuery("SELECT *,typeof(u) AS storage_type FROM " + StorageDdl.quote(topic))) {
            assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
            assertAll(() -> assertType(rows, "u", JDBCType.VARCHAR), () -> assertType(rows, "f", JDBCType.VARCHAR),
                    () -> assertEquals("18446744073709551615", rows.getObject("u")),
                    () -> assertEquals("18446744073709551614", rows.getObject("f")),
                    () -> assertEquals("text", rows.getString("storage_type")));
            assertType(rows, "u32", JDBCType.BIGINT); assertEquals(4294967295L, rows.getObject("u32"));
            assertType(rows, "signed", JDBCType.BIGINT); assertEquals(9007199254740993L, rows.getObject("signed"));
            assertFalse(rows.next());
        }
    }

    private static void protobufWellKnown(Statement statement) throws Exception {
        var schema = new ProtobufSchema("""
                syntax="proto3";
                import "google/protobuf/timestamp.proto";
                import "google/protobuf/duration.proto";
                import "google/protobuf/wrappers.proto";
                import "google/protobuf/struct.proto";
                import "google/protobuf/field_mask.proto";
                import "google/protobuf/any.proto";
                import "google/protobuf/empty.proto";
                message Known {
                  google.protobuf.Timestamp timestamp=1; google.protobuf.Duration duration=2;
                  google.protobuf.DoubleValue d=3; google.protobuf.FloatValue f=4;
                  google.protobuf.Int64Value i64=5; google.protobuf.UInt64Value u64=6;
                  google.protobuf.Int32Value i32=7; google.protobuf.UInt32Value u32=8;
                  google.protobuf.BoolValue b=9; google.protobuf.StringValue s=10; google.protobuf.BytesValue raw=11;
                  google.protobuf.Struct object=12; google.protobuf.Value dynamic=13; google.protobuf.ListValue list=14;
                  google.protobuf.FieldMask mask=15; google.protobuf.Any any=16; google.protobuf.Empty empty=17;
                }
                """);
        String json = """
                {"timestamp":"2023-11-14T22:13:20.123Z","duration":"2.300s","d":1.25,"f":2.5,
                 "i64":"9007199254740993","u64":"18446744073709551615","i32":7,"u32":4294967295,
                 "b":true,"s":"hello","raw":"AQI=","object":{"n":3.0},"dynamic":"text","list":[1.0,"two"],
                 "mask":"customer.name","any":{},"empty":{}}
                """;
        var builder = DynamicMessage.newBuilder(schema.toDescriptor()); JsonFormat.parser().merge(json, builder);
        Message container = builder.build(); var expected = JacksonMapper.INSTANCE.readTree(json);
        var checks = new ArrayList<org.junit.jupiter.api.function.Executable>();
        String nested = topic("delete"); sendProtobuf(nested, null, container, TestStack.SchemaIdIn.PREFIX);
        checks.add(() -> {
            try (var rows = statement.executeQuery("SELECT * FROM " + StorageDdl.quote(nested))) {
                assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                var fields = new ArrayList<org.junit.jupiter.api.function.Executable>();
                for (var field : schema.toDescriptor().getFields()) fields.add(() -> {
                    String name = field.getName(); JDBCType type = wellKnownType(name); assertType(rows, name, type);
                    var node = expected.get(name);
                    Object value = switch (type) {
                        case BIGINT -> node.asLong(); case DOUBLE -> node.asDouble(); case BOOLEAN -> node.asBoolean();
                        default -> node.isTextual() && !name.equals("dynamic") ? node.textValue() : node.toString();
                    };
                    assertEquals(value, rows.getObject(name), name);
                });
                assertAll(fields); assertFalse(rows.next());
            }
        });
        for (var field : container.getDescriptorForType().getFields()) checks.add(() -> {
            Message message = (Message) container.getField(field); String topic = topic("delete");
            sendProtobuf(topic, message, message, TestStack.SchemaIdIn.PREFIX);
            try (var rows = statement.executeQuery("SELECT * FROM " + StorageDdl.quote(topic))) {
                assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                assertEquals(10, rows.getMetaData().getColumnCount(), field.getMessageType().getFullName() + ": metadata and primitive/JSON key only");
                assertType(rows, "key", wellKnownType(field.getName()));
                assertEquals(expected.get(field.getName()), JacksonMapper.INSTANCE.readTree(rows.getString("_value")));
                assertEquals(rows.getString("_key"), rows.getString("_value"));
                assertFalse(rows.next());
            }
        });
        assertAll(checks);
    }

    private static void protobufWellKnownMix(Statement statement) throws Exception {
        // PLAN §6.6: several message identities get only metadata and JSON, even with special JSON shapes.
        String topic = topic("delete"), table = StorageDdl.quote(topic);
        try (var rest = new RestService(TestStack.REGISTRY)) { rest.updateCompatibility("NONE", topic + "-value"); }
        var schema = new ProtobufSchema("syntax=\"proto3\"; message Row { int32 n=1; }");
        var row = DynamicMessage.newBuilder(schema.toDescriptor()); JsonFormat.parser().merge("{\"n\":7}", row);
        sendProtobuf(topic, null, row.build(), TestStack.SchemaIdIn.PREFIX);
        try (var rows = statement.executeQuery("SELECT n FROM " + table)) {
            assertTrue(rows.next()); assertEquals(7L, rows.getObject(1)); assertFalse(rows.next());
        }
        sendProtobuf(topic, null, com.google.protobuf.Timestamp.newBuilder().setSeconds(1700000000).build(), TestStack.SchemaIdIn.PREFIX);
        sendProtobuf(topic, null, com.google.protobuf.Struct.newBuilder().putFields("n",
                com.google.protobuf.Value.newBuilder().setStringValue("not-a-number").build()).build(), TestStack.SchemaIdIn.PREFIX);
        try (var rows = statement.executeQuery("SELECT * FROM " + table + " ORDER BY _offset")) {
            assertEquals(9, rows.getMetaData().getColumnCount(), "message identity precedes JSON-shape conversion");
            for (String json : List.of("{\"n\":7}", "\"2023-11-14T22:13:20Z\"", "{\"n\":\"not-a-number\"}")) {
                assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                assertEquals(JacksonMapper.INSTANCE.readTree(json), JacksonMapper.INSTANCE.readTree(rows.getString("_value")));
            }
            assertFalse(rows.next());
        }
    }

    private void protobufAny(String reference, String address) throws Exception {
        // PLAN §7.2: Any's JSON uses types in the record schema's descriptor graph, without registry lookups.
        String topic = topic("delete"), table = StorageDdl.quote(topic);
        var schema = new ProtobufSchema("""
                syntax="proto3"; package shop;
                import "google/protobuf/any.proto";
                import "%s";
                message Payload { string text=1; }
                message Envelope { google.protobuf.Any body=1; }
                """.formatted(reference), List.of(new SchemaReference(reference, reference, 1)), Map.of(reference, address), null, null);
        var envelope = schema.toDescriptor("shop.Envelope"); var body = envelope.findFieldByName("body");
        sendProtobuf(topic, null, DynamicMessage.newBuilder(envelope).build(), TestStack.SchemaIdIn.PREFIX);
        try (var registry = new CountingRegistry()) {
            var p = settings(); p.setProperty("cache.path", dir.resolve("any.db").toString());
            p.setProperty("schema.registry.url", registry.url());
            try (var connection = DriverManager.getConnection(URL, p); var statement = connection.createStatement()) {
                // Load the schema and its fixed references before any packed value is seen.
                try (var rows = statement.executeQuery("SELECT _decode_error FROM " + table)) {
                    assertTrue(rows.next()); assertNull(rows.getString(1)); assertFalse(rows.next());
                }
                int requests = registry.calls(); assertTrue(requests > 0);
                for (String type : List.of("shop.Payload", "shop.Address", "outside.Missing")) {
                    var descriptor = type.equals("outside.Missing")
                            ? new ProtobufSchema("syntax=\"proto3\"; package outside; message Missing { string text=1; }").toDescriptor()
                            : schema.toDescriptor(type);
                    var payload = DynamicMessage.newBuilder(descriptor);
                    payload.setField(descriptor.getFields().getFirst(), type.equals("shop.Address") ? "London" : "hello");
                    var any = DynamicMessage.newBuilder(body.getMessageType());
                    any.setField(any.getDescriptorForType().findFieldByName("type_url"), "type.googleapis.com/" + type);
                    any.setField(any.getDescriptorForType().findFieldByName("value"), payload.build().toByteString());
                    sendProtobuf(topic, null, DynamicMessage.newBuilder(envelope).setField(body, any.build()).build(), TestStack.SchemaIdIn.PREFIX);
                }
                try (var rows = statement.executeQuery("SELECT body,_decode_error FROM " + table + " WHERE _offset>0 ORDER BY _offset")) {
                    assertType(rows, "body", JDBCType.VARCHAR);
                    for (String expected : List.of("{\"@type\":\"type.googleapis.com/shop.Payload\",\"text\":\"hello\"}",
                            "{\"@type\":\"type.googleapis.com/shop.Address\",\"city\":\"London\"}")) {
                        assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                        assertEquals(JacksonMapper.INSTANCE.readTree(expected), JacksonMapper.INSTANCE.readTree(rows.getString("body")));
                    }
                    assertTrue(rows.next()); String error = rows.getString("_decode_error");
                    assertNotNull(error); assertTrue(error.contains("Cannot find type for url"), error);
                    assertTrue(error.contains("outside.Missing"), error); assertFalse(rows.next());
                }
                assertEquals(requests, registry.calls(), "Any resolves only the already loaded descriptor graph");
            }
        }
    }

    private static JDBCType wellKnownType(String name) {
        return switch (name) {
            case "i64", "i32", "u32" -> JDBCType.BIGINT;
            case "d", "f" -> JDBCType.DOUBLE;
            case "b" -> JDBCType.BOOLEAN;
            default -> JDBCType.VARCHAR;
        };
    }

    private static void sendProtobuf(String topic, Message key, Message value, TestStack.SchemaIdIn where) {
        try (var keys = new KafkaProtobufSerializer<Message>(); var values = new KafkaProtobufSerializer<Message>()) {
            Map<String, Object> config = new HashMap<>(); config.put("schema.registry.url", TestStack.REGISTRY);
            if (where == TestStack.SchemaIdIn.HEADER) {
                config.put("key.schema.id.serializer", "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer");
                config.put("value.schema.id.serializer", "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer");
            }
            keys.configure(config, true); values.configure(config, false);
            var headers = new RecordHeaders();
            TestStack.send(topic, 0, null, keys.serialize(topic, headers, key), values.serialize(topic, headers, value), headers);
        }
    }

    /** 0015a / PLAN §7.1–7.3, §6.6: each prefix/header names its own JSON schema; fixed-version
     * references survive reopening without Registry access, and cached schema evolution drives the view.
     */
    @Test void jsonSchemaRecordsUseTheirOwnSchemasAndCachedReferences() throws Exception {
        String topic = topic("delete"), table = StorageDdl.quote(topic), subject = topic + "-child";
        String child = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}";
        String schemaText = """
                {"type":"object","properties":{
                  "n":{"type":"integer"},"ratio":{"type":"number"},"ok":{"type":"boolean"},
                  "text":{"type":"string"},"state":{"type":"string","enum":["OPEN","CLOSED"]},
                  "array":{"type":"array","items":{"type":"integer"}},
                  "nested":{"$ref":"child.json"},"external":{"$ref":"child.json#/properties/id"},
                  "nullable":{"type":["null","integer"]},"widen":{"type":"integer"},"mixed":{"type":"integer"}}}
                """;
        var references = List.of(new SchemaReference("child.json", subject, 1));
        var first = new JsonSchema(schemaText, references, Map.of("child.json", child), null);
        var second = new JsonSchema(schemaText.replace("\"widen\":{\"type\":\"integer\"}", "\"widen\":{\"type\":\"number\"}").replace("\"mixed\":{\"type\":\"integer\"}",
                "\"mixed\":{\"type\":\"string\"},\"later\":{\"type\":\"string\"}"), references, Map.of("child.json", child), null);
        try (var rest = new RestService(TestStack.REGISTRY)) {
            rest.registerSchema(child, "JSON", List.of(), subject, false);
            rest.updateCompatibility("NONE", topic + "-value");
        }
        var requests = new CopyOnWriteArrayList<String>(); var fail = new AtomicBoolean();
        var http = HttpClient.newHttpClient(); var registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        registry.createContext("/", exchange -> {
            requests.add(exchange.getRequestURI().getPath());
            try {
                if (fail.get()) { exchange.sendResponseHeaders(503, -1); return; }
                var reply = http.send(HttpRequest.newBuilder(URI.create(TestStack.REGISTRY + exchange.getRequestURI())).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                exchange.sendResponseHeaders(reply.statusCode(), reply.body().length); exchange.getResponseBody().write(reply.body());
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new java.io.IOException(e); }
            finally { exchange.close(); }
        });
        registry.start();
        var p = settings(); p.setProperty("schema.registry.url", "http://localhost:" + registry.getAddress().getPort());
        // Exercise auto on values and explicit json-sr on keys, independently.
        p.setProperty("topic." + topic + ".key.format", "json-sr");
        String data = "{\"n\":9000000000,\"ratio\":1.25,\"ok\":true,\"text\":\"hello\",\"state\":\"OPEN\","
                + "\"array\":[1,2],\"nested\":{\"id\":7},\"external\":8,\"nullable\":null,\"widen\":1,\"mixed\":1}";
        try {
            for (var where : TestStack.SchemaIdIn.values()) sendJsonSchema(topic, first, data, where);
            try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
                try (var rows = s.executeQuery("SELECT * FROM " + table + " ORDER BY _offset")) {
                    for (String name : List.of("n", "external", "nullable", "mixed", "widen", "key")) assertType(rows, name, JDBCType.BIGINT);
                    assertType(rows, "ratio", JDBCType.DOUBLE); assertType(rows, "ok", JDBCType.BOOLEAN);
                    for (String name : List.of("text", "state", "array", "nested")) assertType(rows, name, JDBCType.VARCHAR);
                    for (int i = 0; i < 2; i++) {
                        assertTrue(rows.next()); assertNull(rows.getString("_decode_error"));
                        assertEquals(9000000000L, rows.getObject("n")); assertEquals(5L, rows.getObject("key"));
                        assertEquals(1.25, rows.getObject("ratio")); assertEquals(true, rows.getObject("ok"));
                        assertEquals("hello", rows.getObject("text")); assertEquals("OPEN", rows.getObject("state"));
                        assertEquals("[1,2]", rows.getObject("array")); assertEquals("{\"id\":7}", rows.getObject("nested"));
                        assertEquals(8L, rows.getObject("external")); assertNull(rows.getObject("nullable"));
                        assertEquals(i == 0, !rows.getString("_value_schema_id").contains("-"));
                    }
                    assertFalse(rows.next());
                }
                assertFalse(columnTypes(c, topic).containsKey("later"));
                for (var where : TestStack.SchemaIdIn.values()) sendJsonSchema(topic, second,
                        data.replace("\"widen\":1", "\"widen\":1.5").replace("\"mixed\":1", "\"mixed\":\"N/A\",\"later\":\"new\""), where);
                // A registered version with no records must never supply view columns.
                try (var rest = new RestService(TestStack.REGISTRY)) {
                    rest.registerSchema("{\"type\":\"object\",\"properties\":{\"latestOnly\":{\"type\":\"string\"}}}", "JSON", List.of(), topic + "-value", false);
                }
                try (var logs = LogCapture.capture(); var rows = s.executeQuery("SELECT n,mixed,later,widen FROM " + table + " ORDER BY _offset")) {
                    assertType(rows, "mixed", JDBCType.VARCHAR); assertType(rows, "widen", JDBCType.DOUBLE);
                    for (int i = 0; i < 4; i++) {
                        assertTrue(rows.next()); assertEquals(i < 2 ? "1" : "\"N/A\"", rows.getObject("mixed"));
                        assertEquals(i < 2 ? null : "new", rows.getObject("later"));
                        assertEquals(i < 2 ? 1.0 : 1.5, rows.getObject("widen"));
                    }
                    assertFalse(rows.next()); assertTrue(warnings(s).contains("mixed"));
                    assertTrue(logs.lines().stream().anyMatch(l -> l.startsWith("WARN ") && l.contains("mixed")));
                }
                assertFalse(columnTypes(c, topic).containsKey("latestOnly"));
            }
            assertEquals(1, requests.stream().filter(path -> path.equals("/subjects/" + subject + "/versions/1")).count(), requests.toString());
            assertTrue(requests.stream().allMatch(path -> path.startsWith("/schemas/ids/") || path.startsWith("/schemas/guids/")
                    || path.equals("/subjects/" + subject + "/versions/1")), requests.toString());
            int calls = requests.size(); fail.set(true);
            // Fresh decoder, same cache: both schema identities and their reference bodies are on disk.
            for (var where : TestStack.SchemaIdIn.values()) sendJsonSchema(topic, second,
                    data.replace("\"widen\":1", "\"widen\":1.5").replace("\"mixed\":1", "\"mixed\":\"N/A\",\"later\":\"new\""), where);
            try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement();
                 var rows = s.executeQuery("SELECT n,_decode_error FROM " + table + " ORDER BY _offset")) {
                for (int i = 0; i < 6; i++) { assertTrue(rows.next()); assertEquals(9000000000L, rows.getObject(1)); assertNull(rows.getString(2)); }
                assertFalse(rows.next());
            }
            assertEquals(calls, requests.size(), requests.toString());
        } finally { registry.stop(0); }
    }

    private static void sendJsonSchema(String topic, JsonSchema schema,
                                       String data, TestStack.SchemaIdIn where) throws Exception {
        try (var value = new KafkaJsonSchemaSerializer<>();
             var key = new KafkaJsonSchemaSerializer<>()) {
            Map<String, Object> config = new HashMap<>(); config.put("schema.registry.url", TestStack.REGISTRY);
            if (where == TestStack.SchemaIdIn.HEADER) {
                config.put("key.schema.id.serializer", "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer");
                config.put("value.schema.id.serializer", "io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer");
            }
            key.configure(config, true); value.configure(config, false);
            var mapper = JacksonMapper.INSTANCE;
            var headers = new RecordHeaders();
            byte[] k = key.serialize(topic, headers, JsonSchemaUtils.envelope(
                    new JsonSchema("{\"type\":\"integer\"}"), mapper.readTree("5")));
            byte[] v = value.serialize(topic, headers, JsonSchemaUtils.envelope(schema, mapper.readTree(data)));
            TestStack.send(topic, 0, null, k, v, headers);
        }
    }

    /** 0015b / PLAN §6.6, §7.2, §10.1: cached object fields become typed columns, numeric types widen,
     * conflicts stay unconverted with a warning, and regeneration preserves an open reader's snapshot.
     */
    @Test void jsonColumnsEvolveWithTypedValuesWarningsAndReaderSnapshots() throws Exception {
        for (String policy : List.of("delete", "compact")) {
            String topic = topic(policy), table = StorageDdl.quote(topic);
            TestStack.sendJson(topic, "{\"id\":7}", """
                    {"n":7,"mixed":1,"widen":1,"flag":true,"text":"a","nested":{"x":1},"array":[1],
                     "empty":null,"_offset":123,"it's.complicated":9}
                    """, null);
            var p = settings(); p.remove("schema.registry.url"); p.setProperty("cache.path", dir.resolve(policy + ".db").toString());
            try (var c = DriverManager.getConnection(URL, p); var old = c.createStatement(); var next = c.createStatement();
                 var before = old.executeQuery("SELECT * FROM " + table + " WHERE _offset>=0")) {
                assertTrue(before.next()); var metadata = before.getMetaData(); int width = metadata.getColumnCount();
                assertEquals(7L, before.getObject("n")); assertEquals(true, before.getObject("flag"));
                assertEquals(Types.BIGINT, metadata.getColumnType(before.findColumn("widen")));
                assertEquals("{\"x\":1}", before.getString("nested")); assertEquals("[1]", before.getString("array"));
                assertNull(before.getObject("empty")); assertEquals(123L, before.getObject("_offset_1"));
                assertEquals(9L, before.getObject("it's.complicated")); assertEquals(7L, before.getObject("key_id"));
                TestStack.sendJson(topic, "second", """
                        {"n":8,"mixed":"N/A","widen":1.25,"flag":false,"text":"b","nested":{"x":2},
                         "array":[2],"later":"present","key_id":"payload"}
                        """, null);
                try (var logs = LogCapture.capture(); var rows = next.executeQuery("SELECT * FROM " + table + " WHERE _offset>=0 ORDER BY _offset")) {
                    var types = rows.getMetaData();
                    assertEquals(Types.DOUBLE, types.getColumnType(rows.findColumn("widen")));
                    assertEquals("DOUBLE", types.getColumnTypeName(rows.findColumn("widen")));
                    assertEquals(Types.VARCHAR, types.getColumnType(rows.findColumn("mixed")));
                    assertEquals("VARCHAR", types.getColumnTypeName(rows.findColumn("mixed")));
                    assertEquals(Types.BOOLEAN, types.getColumnType(rows.findColumn("flag")));
                    assertEquals(Types.VARCHAR, types.getColumnType(rows.findColumn("nested")));
                    assertTrue(rows.next()); assertEquals("1", rows.getObject("mixed")); assertEquals(1.0, rows.getObject("widen"));
                    assertNull(rows.getObject("later")); assertNull(rows.getObject("key_id_1"));
                    assertTrue(rows.next()); assertEquals("\"N/A\"", rows.getObject("mixed")); assertEquals(1.25, rows.getObject("widen"));
                    assertEquals(false, rows.getObject("flag")); assertEquals("present", rows.getObject("later"));
                    assertEquals("payload", rows.getObject("key_id_1")); assertNull(rows.getObject("key_id")); assertFalse(rows.next());
                    assertTrue(warnings(next).contains("mixed"), warnings(next));
                    assertTrue(logs.lines().stream().anyMatch(line -> line.startsWith("WARN ") && line.contains("mixed")), logs.lines().toString());
                }
                assertEquals(width, metadata.getColumnCount());
                assertEquals(Types.BIGINT, metadata.getColumnType(before.findColumn("widen")));
                for (int i = 1; i <= width; i++) assertNotEquals("later", metadata.getColumnName(i));
                assertFalse(before.next());
                var columns = columnTypes(c, topic);
                assertEquals(Types.DOUBLE, columns.get("widen")); assertEquals(Types.VARCHAR, columns.get("mixed"));
                assertEquals(Types.VARCHAR, columns.get("later")); assertFalse(columns.containsKey("x"));
                try (var rows = next.executeQuery("SELECT n,mixed,later FROM " + table + " WHERE _offset>=0 ORDER BY _offset")) {
                    assertEquals(List.of(7, 8), values(rows)); assertTrue(warnings(next).contains("mixed"));
                }
            }
            p.setProperty("offline", "true");
            try (var c = DriverManager.getConnection("jdbc:kafcacheql://localhost:1", p); var s = c.createStatement();
                 var rows = s.executeQuery("SELECT n,mixed,later FROM " + table + " WHERE _offset>=0 ORDER BY _offset")) {
                assertEquals(Types.BIGINT, rows.getMetaData().getColumnType(1));
                assertEquals(Types.VARCHAR, rows.getMetaData().getColumnType(2)); assertEquals(List.of(7, 8), values(rows));
                assertTrue(warnings(s).contains("mixed")); assertTrue(warnings(s).contains("offline"));
                assertEquals(Types.DOUBLE, columnTypes(c, topic).get("widen"));
            }
        }
    }

    /** 0015b / PLAN §6.6 and the manager's sample rule: newest 1,000 cached records across the topic,
     * including non-object records in that limit; plain strings, bytes and root arrays add no columns.
     */
    @Test void jsonSampleIsLimitedToTheNewestThousandCachedRecords() throws Exception {
        String topic = TestStack.uniqueTopic("jdbc-json-sample"); TestStack.createTopic(topic, 2, Map.of());
        String text = topic("delete"), bytes = topic("delete"), array = topic("delete");
        long base = System.currentTimeMillis() - 60000;
        TestStack.send(topic, 0, base + 5000, null, "{\"newest\":true,\"current\":0,\"number\":3,\"real\":3.5,\"flag\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8), null);
        for (int n = 1; n <= 1000; n++) {
            String json = n == 1 ? "{\"discarded\":true,\"number\":\"N/A\",\"real\":\"N/A\",\"flag\":\"unknown\"}" : n == 2
                    ? "{\"boundary\":true,\"current\":2,\"number\":2}" : "{\"current\":" + n + ",\"number\":" + n + ",\"real\":" + (n + 0.5) + ",\"flag\":true}";
            TestStack.send(topic, n % 2, base + n, null, json.getBytes(java.nio.charset.StandardCharsets.UTF_8), null);
        }
        TestStack.sendJson(text, "k", "{\"hidden\":1}", null); TestStack.sendJson(bytes, "k", "{\"hidden\":1}", null);
        TestStack.sendJson(array, "k", "[{\"hidden\":1}]", null);
        var p = settings(); p.remove("schema.registry.url"); p.setProperty("sync.batch.records", "1000"); p.setProperty("max.poll.records", "1000");
        p.setProperty("topic." + text + ".value.format", "string"); p.setProperty("topic." + bytes + ".value.format", "bytes");
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            try (var rows = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(topic))) { assertTrue(rows.next()); assertEquals(1001, rows.getInt(1)); }
            var columns = columnTypes(c, topic);
            assertEquals(Types.BIGINT, columns.get("number")); assertTrue(columns.containsKey("boundary"));
            assertEquals(Types.DOUBLE, columns.get("real")); assertEquals(Types.BOOLEAN, columns.get("flag"));
            assertEquals(Types.BOOLEAN, columns.get("newest")); assertFalse(columns.containsKey("discarded"));
            assertFalse(warnings(s).contains("conflicting"), warnings(s));
            TestStack.send(topic, 0, base + 6000, null, "\"scalar\"".getBytes(java.nio.charset.StandardCharsets.UTF_8), null);
            try (var rows = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(topic))) { assertTrue(rows.next()); assertEquals(1002, rows.getInt(1)); }
            columns = columnTypes(c, topic); assertFalse(columns.containsKey("boundary")); assertFalse(columns.containsKey("discarded"));
            try (var rows = s.executeQuery("SELECT newest,\"number\",\"real\",flag FROM " + StorageDdl.quote(topic) + " WHERE _timestamp=" + (base + 5000))) {
                assertEquals(Types.BOOLEAN, rows.getMetaData().getColumnType(1)); assertTrue(rows.next());
                assertEquals(true, rows.getObject(1)); assertEquals(3L, rows.getObject(2));
                assertEquals(3.5, rows.getObject(3)); assertEquals(true, rows.getObject(4));
                for (int column = 1; column <= 4; column++) assertEquals(rows.getMetaData().getColumnClassName(column), rows.getObject(column).getClass().getName());
                assertFalse(rows.next());
            }
            // S5-2 / §6.6: the sampled type must never silently coerce an older, incompatible value.
            for (String field : List.of("number", "real", "flag")) {
                Class<?> type = field.equals("flag") ? Boolean.class : field.equals("real") ? Double.class : Long.class;
                List<org.junit.jupiter.api.function.ThrowingConsumer<ResultSet>> getters = List.of(
                        r -> r.getObject(1), r -> r.getObject(field), r -> r.getObject(1, type), r -> r.getObject(field, type),
                        r -> r.getByte(1), r -> r.getByte(field), r -> r.getShort(1), r -> r.getShort(field),
                        r -> r.getInt(1), r -> r.getInt(field), r -> r.getLong(1), r -> r.getLong(field),
                        r -> r.getFloat(1), r -> r.getFloat(field), r -> r.getDouble(1), r -> r.getDouble(field),
                        r -> r.getBoolean(1), r -> r.getBoolean(field), r -> r.getBigDecimal(1), r -> r.getBigDecimal(field));
                for (var getter : getters) {
                    try (var rows = s.executeQuery("SELECT " + StorageDdl.quote(field) + " FROM " + StorageDdl.quote(topic)
                            + " WHERE _partition=1 AND _offset=0")) {
                        assertTrue(rows.next()); String textValue = field.equals("flag") ? "unknown" : "N/A";
                        assertEquals(type.getName(), rows.getMetaData().getColumnClassName(1));
                        assertEquals(textValue, rows.getString(1)); assertEquals(textValue, rows.getString(field));
                        SQLException invalid = assertThrows(SQLException.class, () -> getter.accept(rows));
                        assertEquals("22018", invalid.getSQLState());
                        assertTrue(invalid.getMessage().contains(field), invalid.getMessage());
                        assertTrue(invalid.getMessage().contains(textValue), invalid.getMessage());
                    }
                }
            }
            for (String plain : List.of(text, bytes, array)) {
                try (var rows = s.executeQuery("SELECT * FROM " + StorageDdl.quote(plain))) {
                    assertEquals(9, rows.getMetaData().getColumnCount()); assertTrue(rows.next()); assertFalse(rows.next());
                }
            }
        }
    }

    private static Map<String, Integer> columnTypes(Connection connection, String topic) throws SQLException {
        var types = new LinkedHashMap<String, Integer>();
        try (var columns = connection.getMetaData().getColumns(null, null, topic, null)) {
            while (columns.next()) types.put(columns.getString("COLUMN_NAME"), columns.getInt("DATA_TYPE"));
        }
        return types;
    }

    /** 0015c / PLAN §6.3, §8.3, §10.1, §11: recent freshness checks belong to the context and current
     * generation; online timestamp bounds still use Kafka, without syncing outside their window.
     * Skipped freshness checks never advance caught_up_at_ms or write an unchanged cache.
     */
    @Test void stalenessReusesChecksUntilAnotherJvmChangesTheGeneration() throws Exception {
        for (String policy : List.of("delete", "compact")) {
            String topic = topic(policy), table = StorageDdl.quote(topic);
            long now = System.currentTimeMillis();
            send(topic, 1, now - TimeUnit.HOURS.toMillis(3)); send(topic, 2, now - TimeUnit.HOURS.toMillis(2)); send(topic, 3, now);
            var p = settings(); p.setProperty("cache.path", dir.resolve(policy + ".db").toString()); p.setProperty("max.staleness.ms", "600000");
            var config = DriverConfig.load(URL, p);
            try (var c = DriverManager.getConnection(URL, p); var control = CacheContext.acquire(config); var clients = KafkaClients.acquire(config)) {
                Field admin = KafkaClients.class.getDeclaredField("admin"); admin.setAccessible(true);
                Admin original = (Admin) admin.get(clients.clients()); var calls = new ArrayList<String>();
                var timestampLookups = new AtomicInteger();
                admin.set(clients.clients(), Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (self, method, args) -> {
                    if (method.getName().equals("listOffsets")) {
                        Object spec = ((Map<?, ?>) args[0]).values().iterator().next();
                        if (spec instanceof OffsetSpec.TimestampSpec) timestampLookups.incrementAndGet();
                        else calls.add(spec instanceof OffsetSpec.EarliestSpec ? "logStartOffsets" : "endOffsets");
                    } else calls.add(method.getName());
                    try { return method.invoke(original, args); }
                    catch (InvocationTargetException e) { throw e.getCause(); }
                }));
                var fetched = new AtomicInteger();
                control.hooks(new SyncWorker.Hooks() { @Override public void afterCommit(SyncBatch batch) { fetched.addAndGet(batch.records().size()); } });
                try (var statement = c.createStatement()) {
                    String sql = "SELECT n FROM " + table + " WHERE _offset>=0 ORDER BY _offset";
                    try (var rows = statement.executeQuery("SELECT n FROM " + table + " WHERE _offset<1")) { assertEquals(List.of(1), values(rows)); }
                    assertEquals(2, Collections.frequency(calls, "describeTopics"), "initial registration and planning");
                    assertEquals(1, Collections.frequency(calls, "logStartOffsets")); assertEquals(1, Collections.frequency(calls, "endOffsets"));
                    assertEquals(1, Collections.frequency(calls, "describeConfigs"));
                    var firstCalls = List.copyOf(calls); var before = partitionState(control);
                    try (var rows = statement.executeQuery("SELECT n FROM " + table + " WHERE _offset<1")) { assertEquals(List.of(1), values(rows)); }
                    assertEquals(firstCalls, calls, "the repeated offset query makes no freshness calls");
                    assertEquals(0, timestampLookups.get());
                    // Offset 1 is uncached and outside this timestamp window on the append topic.
                    try (var rows = statement.executeQuery("SELECT n FROM " + table + " WHERE _timestamp>=" + (now - TimeUnit.HOURS.toMillis(1)))) {
                        assertEquals(List.of(3), values(rows));
                    }
                    assertEquals("delete".equals(policy) ? 2 : 3, fetched.get(), "the timestamp query syncs only its window");
                    assertEquals("delete".equals(policy) ? 1 : 0, timestampLookups.get());
                    assertEquals(firstCalls, calls, "timestamp resolution adds no freshness calls");
                    assertEquals(before, partitionState(control));
                    // Another connection reuses the context's freshness check, but resolves its default time through Kafka.
                    var recent = new Properties(); recent.putAll(p); recent.setProperty("append.default.window", "1h");
                    try (var other = DriverManager.getConnection(URL, recent); var s2 = other.createStatement();
                         var rows = s2.executeQuery("SELECT n FROM " + table + " ORDER BY _offset")) {
                        assertEquals("delete".equals(policy) ? List.of(3) : List.of(1, 2, 3), values(rows));
                    }
                    assertEquals("delete".equals(policy) ? 2 : 0, timestampLookups.get());
                    assertEquals("delete".equals(policy) ? 2 : 3, fetched.get());
                    try (var rows = statement.executeQuery(sql)) { assertEquals(List.of(1, 2, 3), values(rows)); }
                    assertEquals(3, fetched.get()); assertEquals(before, partitionState(control));
                    assertEquals(firstCalls, calls);
                    // S5-3 / §10.1: a same-Kafka-ID rebuild cannot be caught by the topic-ID safeguard.
                    for (boolean recreate : policy.equals("compact") ? List.of(false, true) : List.of(true)) {
                        long generation = control.read(r -> io.github.ifnesi.kafcacheql.cache.TopicRegistry.generation(r, topic).orElseThrow());
                        String kafkaId = control.read(r -> {
                            try (var q = r.createStatement(); var rows = q.executeQuery("SELECT topic_uuid FROM \"$topics\"")) { rows.next(); return rows.getString(1); }
                        });
                        if (recreate) TestStack.recreateTopic(topic, 1, Map.of("cleanup.policy", policy));
                        else try (var execution = new Execution(java.time.Duration.ofSeconds(5))) {
                            control.write(topic, generation, execution, writer -> {
                                try (var q = writer.prepareStatement("UPDATE \"$partitions\" SET caught_up_at_ms=0 WHERE topic_ref=?")) {
                                    q.setLong(1, generation); q.executeUpdate();
                                }
                                return null;
                            });
                        }
                        send(topic, 9, null);
                        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                                System.getProperty("java.class.path"), io.github.ifnesi.kafcacheql.sql.PipelineProcess.class.getName(),
                                control.path().toString(), topic, "jdbc").redirectError(ProcessBuilder.Redirect.INHERIT).start();
                        try {
                            assertTrue(child.waitFor(20, TimeUnit.SECONDS)); assertEquals(0, child.exitValue());
                            assertEquals(recreate ? "ROW 9" : "ROW 1\nROW 2\nROW 3\nROW 9",
                                    new String(child.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim());
                        } finally { child.destroyForcibly(); }
                        assertTrue(control.read(r -> io.github.ifnesi.kafcacheql.cache.TopicRegistry.generation(r, topic).orElseThrow()) > generation);
                        assertEquals(recreate, control.read(r -> {
                            try (var q = r.createStatement(); var rows = q.executeQuery("SELECT topic_uuid FROM \"$topics\"")) { rows.next(); return !kafkaId.equals(rows.getString(1)); }
                        }));
                        calls.clear();
                        try (var rows = statement.executeQuery(sql)) { assertEquals(recreate ? List.of(9) : List.of(1, 2, 3, 9), values(rows)); }
                        assertEquals(1, Collections.frequency(calls, "describeTopics"));
                        assertEquals(1, Collections.frequency(calls, "logStartOffsets")); assertEquals(1, Collections.frequency(calls, "endOffsets"));
                        assertEquals(recreate ? 1 : 0, Collections.frequency(calls, "describeConfigs")); assertEquals(3, fetched.get(), "the child filled the replacement cache");
                    }
                } finally { admin.set(clients.clients(), original); }
            }
        }
    }

    /** 0015c / PLAN §8.4, §10.1, §11: offline uses each partition's newest cached timestamp and end;
     * cached views and metadata work without services, while missing or lost ranges fail explicitly.
     */
    @Test void offlineUsesCachedWindowsSchemasAndMetadataWithoutContactingServices() throws Exception {
        String topic = TestStack.uniqueTopic("jdbc-offline"); TestStack.createTopic(topic, 2, Map.of());
        String compact = topic("compact"), uncached = topic("delete"); send(compact, 8, null);
        long hour = TimeUnit.HOURS.toMillis(1), base = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        for (int partition = 0; partition < 2; partition++) for (int n = 0; n < 5; n++) {
            var record = new GenericData.Record(VALUE); record.put("n", partition * 10 + n); record.put("ok", true);
            var headers = new RecordHeaders();
            byte[] bytes = TestStack.serializeAvro(topic, headers, record, TestStack.SchemaIdIn.PREFIX, false);
            TestStack.send(topic, partition, base + partition * TimeUnit.DAYS.toMillis(2) + n * hour, null, bytes, headers);
        }
        var p = settings(); String table = StorageDdl.quote(topic);
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            for (String bound : List.of("_offset<2", "_offset>=3")) {
                try (var rows = s.executeQuery("SELECT n FROM " + table + " WHERE " + bound)) { while (rows.next()) {} }
            }
            try (var rows = s.executeQuery("SELECT n FROM " + StorageDdl.quote(compact))) { assertEquals(List.of(8), values(rows)); }
        }
        var requests = new AtomicInteger(); var registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        registry.createContext("/", exchange -> { requests.incrementAndGet(); exchange.sendResponseHeaders(500, -1); exchange.close(); }); registry.start();
        p.setProperty("offline", "true"); p.setProperty("schema.registry.url", "http://localhost:" + registry.getAddress().getPort());
        p.setProperty("append.default.window", "1h"); p.setProperty("append.default.window.max.records", "2");
        String url = "jdbc:kafcacheql://localhost:1";
        var config = DriverConfig.load(url, p);
        try (var c = DriverManager.getConnection(url, p); var control = CacheContext.acquire(config); var s = c.createStatement()) {
            s.setQueryTimeout(3); var before = partitionState(control);
            try (var rows = s.executeQuery("SELECT n FROM " + table + " ORDER BY _partition,_offset")) { assertEquals(List.of(3, 4, 13, 14), values(rows)); }
            String warnings = warnings(s); assertTrue(warnings.contains("offline"), warnings); assertTrue(warnings.contains("as of"), warnings);
            assertTrue(warnings.contains("partition 0") && warnings.contains("partition 1"), warnings);
            for (String state : before.subList(0, 2)) {
                String[] fields = state.split(":");
                assertTrue(warnings.contains(java.time.Instant.ofEpochMilli(Long.parseLong(fields[2])).toString()), warnings);
            }
            String cached = "SELECT n FROM " + table + " WHERE _offset>=0 AND _offset<2 ORDER BY _partition,_offset";
            try (var rows = s.executeQuery(cached)) { assertEquals(List.of(0, 1, 10, 11), values(rows)); }
            try (var rows = s.executeQuery("SELECT n FROM " + table + " WHERE _offset>=3 AND _timestamp>=" + (base + 4 * hour) + " AND _partition=0")) { assertEquals(List.of(4), values(rows)); }
            try (var rows = s.executeQuery("SELECT n FROM " + table + " WHERE _offset<2 AND _timestamp>=" + (base + hour) + " AND _partition=0")) { assertEquals(List.of(1), values(rows)); }
            // S5-1 / §8.4, §11: cached timestamps cannot prove that missing offsets contain no matches.
            for (String bound : List.of("_timestamp>=" + (base + 2 * hour), "_timestamp=" + (base + 2 * hour),
                    "_timestamp>=" + (base + 100 * hour), "_timestamp>=" + (base + 4 * hour) + " AND _partition=0")) {
                assertMissing(assertThrows(SQLException.class, () -> s.executeQuery("SELECT n FROM " + table + " WHERE " + bound)), topic, "[2..3)");
            }
            assertMissing(assertThrows(SQLException.class, () -> s.executeQuery("SELECT a.n FROM " + table + " a JOIN " + table
                    + " b ON a._partition=b._partition WHERE a._offset>=3 AND a._timestamp>=" + (base + 4 * hour)
                    + " AND b._timestamp>=" + (base + 4 * hour))), topic, "[2..3)");
            var wide = new Properties(); wide.putAll(p); wide.setProperty("append.default.window.max.records", "4");
            try (var other = DriverManager.getConnection(url, wide); var query = other.createStatement()) {
                assertMissing(assertThrows(SQLException.class, () -> query.executeQuery("SELECT n FROM " + table)), topic, "[2..3)");
            }
            try (var rows = s.executeQuery("SELECT n FROM " + StorageDdl.quote(compact))) { assertEquals(List.of(8), values(rows)); }
            try (var tables = c.getMetaData().getTables(null, null, "%", null)) {
                var names = new HashSet<String>(); while (tables.next()) names.add(tables.getString("TABLE_NAME")); assertEquals(Set.of(topic, compact), names);
            }
            try (var columns = c.getMetaData().getColumns(null, null, topic, "n")) { assertEquals(List.of("n"), columnNames(columns)); }
            try (var columns = c.getMetaData().getColumns(null, null, uncached, null)) { assertFalse(columns.next()); }
            SQLException missing = assertThrows(SQLException.class, () -> s.executeQuery("SELECT n FROM " + table + " WHERE _offset>=1 AND _offset<4"));
            assertMissing(missing, topic, "[2..3)");
            SQLException beyond = assertThrows(SQLException.class, () -> s.executeQuery("SELECT n FROM " + table + " WHERE _offset>=4 AND _offset<7"));
            assertMissing(beyond, topic, "[5..7)");
            assertEquals(before, partitionState(control), "offline queries never advance caught_up_at_ms");
            long generation = control.read(r -> io.github.ifnesi.kafcacheql.cache.TopicRegistry.generation(r, topic).orElseThrow());
            try (var execution = new Execution(java.time.Duration.ofSeconds(5))) {
                control.write(topic, generation, execution, writer -> {
                    io.github.ifnesi.kafcacheql.cache.PartitionStore.raiseFloors(writer, generation, Map.of(0, 1L));
                    io.github.ifnesi.kafcacheql.cache.PartitionStore.deleteChunk(writer, generation, 0, 100); return null;
                });
            }
            assertMissing(assertThrows(SQLException.class, () -> s.executeQuery(cached)), topic, "[0..1)");
            assertEquals(0, requests.get()); assertNull(field(control, "clients"));
            assertTrue(((Set<?>) field(field(field(control, "context"), "worker"), "consumers")).isEmpty());
        } finally { registry.stop(0); }
        p.setProperty("append.default.window.max.records", "1");
        try (var c = DriverManager.getConnection(url, p); var s = c.createStatement(); var rows = s.executeQuery("SELECT n FROM " + table + " ORDER BY _partition")) {
            assertEquals(List.of(4, 14), values(rows));
        }
    }

    private static void assertMissing(SQLException failure, String topic, String range) {
        assertEquals("40001", failure.getSQLState());
        assertTrue(failure.getMessage().contains("missing ranges"), failure.getMessage());
        assertTrue(failure.getMessage().contains(topic), failure.getMessage());
        assertTrue(failure.getMessage().contains(range), failure.getMessage());
    }
    private static String warnings(Statement statement) throws SQLException {
        var text = new StringBuilder(); for (var warning = statement.getWarnings(); warning != null; warning = warning.getNextWarning()) text.append(warning.getMessage()).append('\n');
        return text.toString();
    }
    private static List<String> partitionState(CacheContext.Lease cache) throws SQLException {
        return cache.read(r -> { var state = new ArrayList<String>(); try (var s = r.createStatement(); var rows = s.executeQuery("SELECT topic_ref,partition_no,caught_up_at_ms FROM \"$partitions\" ORDER BY topic_ref,partition_no")) {
            while (rows.next()) state.add(rows.getLong(1) + ":" + rows.getInt(2) + ":" + rows.getObject(3));
        } return state; });
    }

    /** PLAN §12.3: read-only, Spring transaction compatibility, SELECT only, warnings, SQLite bindings;
     * §8.3: timestamp operands are folded afresh for each execution; §7.2/0015d: schema-derived result
     * types for every mapping, while user-computed columns keep SQLite's types.
     */
    @Test void jdbcBehaviourAndPreparedWindows() throws Exception {
        String topic = topic("delete"); String table = StorageDdl.quote(topic);
        long base = System.currentTimeMillis() - 10000;
        for (int n = 0; n < 4; n++) send(topic, n, base + 1000 * n);
        Properties p = settings();
        p.setProperty("append.default.window", "1h"); // This flow exercises finite-window warnings explicitly.
        var fetched = new ArrayList<Long>();
        try (var c = DriverManager.getConnection(URL, p); var control = CacheContext.acquire(DriverConfig.load(URL, p))) {
            control.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch batch) { batch.records().forEach(r -> fetched.add(r.offset())); }
            });
            assertTrue(c.isValid(5)); assertTrue(c.isReadOnly());
            c.setReadOnly(false); assertTrue(c.isReadOnly());
            assertTrue(c.getAutoCommit()); c.setAutoCommit(false); c.commit(); c.rollback();
            assertEquals(Connection.TRANSACTION_NONE, c.getTransactionIsolation());
            try (var s = c.createStatement(); var prepared = c.prepareStatement("SELECT n FROM " + table
                    + " WHERE _timestamp>=? AND _timestamp<? AND n>=? ORDER BY _offset")) {
                assertSame(c, s.getConnection());
                assertThrows(SQLFeatureNotSupportedException.class, () -> s.executeUpdate("DELETE FROM " + table));
                prepared.setLong(1, base + 2000); prepared.setLong(2, base + 4000); prepared.setInt(3, 2);
                try (var r = prepared.executeQuery()) { assertEquals(List.of(2, 3), values(r)); }
                assertEquals(List.of(2L, 3L), fetched); fetched.clear();
                prepared.setLong(1, base); prepared.setLong(2, base + 2000); prepared.setInt(3, 0);
                assertTrue(prepared.execute());
                try (var r = prepared.getResultSet()) { assertSame(prepared, r.getStatement()); assertEquals(List.of(0, 1), values(r)); }
                assertEquals(List.of(0L, 1L), fetched); assertNull(prepared.getWarnings());
                s.setFetchSize(2); s.setMaxRows(2);
                try (var r = s.executeQuery("SELECT n AS number,ok FROM " + table + " ORDER BY _offset")) {
                    assertSame(s, r.getStatement());
                    assertEquals(ResultSet.TYPE_FORWARD_ONLY, r.getType()); assertEquals(ResultSet.CONCUR_READ_ONLY, r.getConcurrency());
                    assertEquals(Types.BIGINT, r.getMetaData().getColumnType(1));
                    assertEquals(Types.BOOLEAN, r.getMetaData().getColumnType(2), "the first row is null; types come from its schema");
                    assertEquals(List.of(0, 1), values(r));
                }
                assertNotNull(s.getWarnings()); assertEquals(2, s.getFetchSize()); assertEquals(2, s.getMaxRows());
                s.setMaxRows(0);
                try (var r = s.executeQuery("SELECT n FROM " + table + " WHERE _offset>=0 ORDER BY _offset")) {
                    assertEquals(List.of(0, 1, 2, 3), values(r));
                }
                assertNull(s.getWarnings()); assertFalse(s.getMoreResults()); assertEquals(-1, s.getUpdateCount());
                try (var r = s.executeQuery("WITH c(value) AS (SELECT n FROM " + table + " WHERE _offset>=0) "
                        + "SELECT value AS renamed, value+1 AS n FROM c ORDER BY value")) {
                    assertEquals(Types.BIGINT, r.getMetaData().getColumnType(1));
                    assertEquals(List.of(0, 1, 2, 3), values(r));
                }
                try (var r = s.executeQuery("SELECT h.*,o.n,o.ok FROM " + table
                        + " o JOIN json_each(o._value) h ON 1=1 WHERE o._offset>=0")) {
                    assertEquals(10, r.getMetaData().getColumnCount());
                    assertEquals(Types.BIGINT, r.getMetaData().getColumnType(9));
                    assertEquals(Types.BOOLEAN, r.getMetaData().getColumnType(10));
                }
                try (var r = s.executeQuery("WITH b AS (SELECT * FROM a), a AS (SELECT n FROM " + table
                        + " WHERE _offset>=0) SELECT * FROM b")) {
                    assertEquals(Types.BIGINT, r.getMetaData().getColumnType(1));
                }
                try (var r = s.executeQuery("SELECT * FROM (" + table + " a JOIN " + table
                        + " b USING(_partition,_offset)) WHERE a._offset>=0")) {
                    int last = r.getMetaData().getColumnCount();
                    assertEquals(Types.BOOLEAN, r.getMetaData().getColumnType(last));
                }
                SQLException missing = assertThrows(SQLException.class, () -> s.executeQuery("SELECT status FROM " + table));
                assertFalse(missing.getMessage().contains("no records with a schema are cached"), "schema-backed topics keep SQLite's diagnosis");
                assertAll(() -> valuesCte(s, table), () -> schemaTypes(s), this::withoutRegistry);
            }
        }
    }

    // PLAN §7.2, §12.3: untyped VALUES outputs occupy positions; schema types and Java values agree.
    private static void valuesCte(Statement statement, String table) throws SQLException {
        for (String values : List.of("VALUES ('text',42)", "VALUES ('text',42),('other',43)")) {
            try (var rows = statement.executeQuery("WITH v(x,y) AS (" + values + ") SELECT v.*,t.n,t.ok FROM v JOIN "
                    + table + " t ON 1=1 WHERE t._offset>=0 ORDER BY t._offset DESC")) {
                assertEquals(Types.VARCHAR, rows.getMetaData().getColumnType(1));
                assertEquals("TEXT", rows.getMetaData().getColumnTypeName(1));
                assertType(rows, "y", JDBCType.INTEGER);
                assertType(rows, "n", JDBCType.BIGINT);
                assertType(rows, "ok", JDBCType.BOOLEAN);
                assertTrue(rows.next()); assertEquals("text", rows.getObject(1));
                assertEquals(3L, rows.getObject("n")); assertEquals(false, rows.getObject("ok"));
            }
        }
    }

    // PLAN §7: a registry is needed only for a schema reference; one undecodable record never stops sync.
    // §6.6: without cached schema records, explain missing payload columns and the JSON alternative.
    private void withoutRegistry() throws Exception {
        String topic = topic("delete"), table = StorageDdl.quote(topic);
        Properties p = settings(); p.remove("schema.registry.url"); p.setProperty("cache.path", dir.resolve("no-registry.db").toString());
        try (var c = DriverManager.getConnection(URL, p); var s = c.createStatement()) {
            SQLException missing = assertThrows(SQLException.class, () -> s.executeQuery("SELECT status FROM " + table));
            assertTrue(missing.getMessage().contains("no records with a schema are cached"), missing.getMessage());
            assertTrue(missing.getMessage().contains(topic), missing.getMessage());
            assertTrue(missing.getMessage().contains("_value ->> '$.status'"), missing.getMessage());
            try (var rows = s.executeQuery("SELECT _value ->> '$.status' FROM " + table)) { assertFalse(rows.next()); }
            TestStack.sendJson(topic, "json", "{\"status\":\"open\"}", null);
            TestStack.sendString(topic, "string", "hello", null);
            TestStack.sendBytes(topic, null, new byte[] {(byte) 255}, null);
            try (var rows = s.executeQuery("SELECT _value FROM " + table + " ORDER BY _offset")) {
                int count = 0; while (rows.next()) count++; assertEquals(3, count, "unframed JSON, strings and bytes need no registry");
            }
            send(topic, 1, null);
            var framed = new GenericData.Record(VALUE); framed.put("n", 2); framed.put("ok", true);
            TestStack.sendAvro(topic, "header", framed, null, TestStack.SchemaIdIn.HEADER);
            TestStack.sendJson(topic, "after", "{\"status\":\"after\"}", null);
            try (var rows = s.executeQuery("SELECT _value,_decode_error,_value_schema_id FROM " + table + " ORDER BY _offset")) {
                for (String json : List.of("{\"status\":\"open\"}", "\"hello\"", "\"/w==\"")) {
                    assertTrue(rows.next()); assertEquals(json, rows.getString(1)); assertNull(rows.getString(2));
                }
                for (int i = 0; i < 2; i++) {
                    assertTrue(rows.next()); assertNotNull(rows.getString(3));
                    assertTrue(rows.getString(2).contains("schema.registry.url is needed"), rows.getString(2));
                    assertFalse(rows.getString(2).contains("at null"));
                }
                assertTrue(rows.next()); assertEquals("{\"status\":\"after\"}", rows.getString(1)); assertNull(rows.getString(2));
                assertFalse(rows.next());
            }
            try (var rows = s.executeQuery("SELECT _value ->> '$.status' FROM " + table + " WHERE _offset=0")) {
                assertTrue(rows.next()); assertEquals("open", rows.getString(1));
            }
            // Unrelated SQLite preparation errors must keep their own diagnosis.
            SQLException function = assertThrows(SQLException.class, () -> s.executeQuery("SELECT absent_function(_value) FROM " + table));
            assertFalse(function.getMessage().contains("no records with a schema are cached"));
        }
    }

    private static void schemaTypes(Statement statement) throws Exception {
        // all-types.avsc wraps numbers and date/time values in arrays; these must be scalar view columns.
        Schema schema = new Schema.Parser().parse("""
                {"type":"record","name":"JdbcTypes","fields":[
                  {"name":"bool","type":"boolean"},
                  {"name":"i","type":"int"}, {"name":"l","type":"long"},
                  {"name":"f","type":"float"}, {"name":"d","type":"double"},
                  {"name":"text","type":"string"},
                  {"name":"state","type":{"type":"enum","name":"JdbcState","symbols":["OPEN","CLOSED"]}},
                  {"name":"uuid","type":{"type":"string","logicalType":"uuid"}},
                  {"name":"bytes","type":"bytes"},
                  {"name":"fixed","type":{"type":"fixed","name":"JdbcFixed","size":3}},
                  {"name":"decimal","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
                  {"name":"money","type":{"type":"fixed","name":"JdbcMoney","size":8,"logicalType":"decimal","precision":12,"scale":2}},
                  {"name":"date","type":{"type":"int","logicalType":"date"}},
                  {"name":"timeMillis","type":{"type":"int","logicalType":"time-millis"}},
                  {"name":"timeMicros","type":{"type":"long","logicalType":"time-micros"}},
                  {"name":"timestampMillis","type":{"type":"long","logicalType":"timestamp-millis"}},
                  {"name":"timestampMicros","type":{"type":"long","logicalType":"timestamp-micros"}},
                  {"name":"localMillis","type":{"type":"long","logicalType":"local-timestamp-millis"}},
                  {"name":"localMicros","type":{"type":"long","logicalType":"local-timestamp-micros"}},
                  {"name":"nested","type":{"type":"record","name":"JdbcChild","fields":[{"name":"n","type":"int"}]}},
                  {"name":"map","type":{"type":"map","values":"int"}},
                  {"name":"array","type":{"type":"array","items":"int"}},
                  {"name":"nullable","type":["null","long"]},
                  {"name":"choice","type":["int","string"]},
                  {"name":"nullableChoice","type":["null","int","string"]}
                ]}
                """);
        String topic = topic("delete");
        var record = new GenericData.Record(schema);
        record.put("bool", true);
        for (String name : List.of("i", "date", "timeMillis")) record.put(name, 1);
        for (String name : List.of("l", "timeMicros", "timestampMillis", "timestampMicros", "localMillis", "localMicros")) record.put(name, 1L);
        record.put("f", 1.5f); record.put("d", 2.5d); record.put("text", "text");
        record.put("state", new GenericData.EnumSymbol(schema.getField("state").schema(), "OPEN"));
        record.put("uuid", "123e4567-e89b-12d3-a456-426614174000");
        record.put("bytes", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        record.put("fixed", new GenericData.Fixed(schema.getField("fixed").schema(), new byte[] {1, 2, 3}));
        var decimal = new Conversions.DecimalConversion();
        Schema bytesDecimal = schema.getField("decimal").schema(), fixedDecimal = schema.getField("money").schema();
        record.put("decimal", decimal.toBytes(new BigDecimal("1234.56"), bytesDecimal, bytesDecimal.getLogicalType()));
        record.put("money", decimal.toFixed(new BigDecimal("1234.56"), fixedDecimal, fixedDecimal.getLogicalType()));
        var child = new GenericData.Record(schema.getField("nested").schema()); child.put("n", 7);
        record.put("nested", child); record.put("map", Map.of("n", 7)); record.put("array", List.of(1, 2));
        for (int n = 0; n < 2; n++) {
            record.put("nullable", n == 0 ? null : 7L);
            record.put("choice", n == 0 ? 7 : "seven");
            record.put("nullableChoice", n == 0 ? null : "seven");
            TestStack.sendAvro(topic, "key" + n, record, null, TestStack.SchemaIdIn.PREFIX);
        }
        // PLAN §7.2: non-finite floats/doubles stay strings in JSON, but JDBC numeric getters retain their values.
        double[] nonFinite = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        for (double value : nonFinite) {
            record.put("f", (float) value); record.put("d", value);
            TestStack.sendAvro(topic, "nonfinite" + value, record, null, TestStack.SchemaIdIn.PREFIX);
        }
        Map<JDBCType, List<String>> expected = Map.of(
                JDBCType.BOOLEAN, List.of("bool"),
                JDBCType.BIGINT, List.of("i", "l", "nullable"),
                JDBCType.DOUBLE, List.of("f", "d"),
                JDBCType.NUMERIC, List.of("decimal", "money"),
                JDBCType.VARCHAR, List.of("text", "state", "uuid", "bytes", "fixed", "date", "timeMillis", "timeMicros",
                        "timestampMillis", "timestampMicros", "localMillis", "localMicros", "nested", "map", "array", "choice", "nullableChoice"));
        String columns = String.join(",", schema.getFields().stream().map(f -> StorageDdl.quote(f.name())).toList());
        String table = StorageDdl.quote(topic);
        var classes = Map.of(JDBCType.BOOLEAN, Boolean.class, JDBCType.BIGINT, Long.class,
                JDBCType.DOUBLE, Double.class, JDBCType.NUMERIC, BigDecimal.class, JDBCType.VARCHAR, String.class);
        try (var rows = statement.executeQuery("SELECT " + columns + ",i+1 AS computed,CAST('blob' AS BLOB) AS binary,_value FROM " + table + " WHERE _offset>=0 ORDER BY _offset")) {
            for (var entry : expected.entrySet()) {
                for (String name : entry.getValue()) {
                    assertType(rows, name, entry.getKey());
                    assertEquals(classes.get(entry.getKey()).getName(), rows.getMetaData().getColumnClassName(rows.findColumn(name)), name);
                }
            }
            assertType(rows, "computed", JDBCType.INTEGER);
            assertType(rows, "binary", JDBCType.BLOB);
            assertTrue(rows.next()); assertNull(rows.getObject("nullable")); assertTrue(rows.wasNull());
            for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) {
                Object value = rows.getObject(i);
                // JDBC allows subclasses; computed columns keep SQLite's own Java class metadata.
                if (value != null) assertTrue(Class.forName(rows.getMetaData().getColumnClassName(i)).isInstance(value), rows.getMetaData().getColumnName(i));
            }
            assertEquals(Boolean.class.getName(), rows.getMetaData().getColumnClassName(rows.findColumn("bool")));
            assertEquals(true, rows.getObject("bool")); assertEquals(1L, rows.getObject("i"));
            assertEquals(new BigDecimal("1234.56"), rows.getObject("decimal"));
            assertEquals("7", rows.getObject("choice")); assertEquals(2, rows.getObject("computed"));
            assertEquals(1.5, rows.getDouble("f")); assertEquals(2.5f, rows.getFloat("d"));
            assertTrue(rows.next()); assertEquals(7L, rows.getObject("nullable"));
            for (double expectedValue : nonFinite) {
                assertTrue(rows.next());
                for (String name : List.of("f", "d")) {
                    int column = rows.findColumn(name);
                    assertEquals(Double.valueOf(expectedValue), rows.getObject(column)); assertFalse(rows.wasNull());
                    assertEquals(Double.valueOf(expectedValue), rows.getObject(name));
                    assertEquals(expectedValue, rows.getDouble(column)); assertEquals(expectedValue, rows.getDouble(name));
                    assertEquals((float) expectedValue, rows.getFloat(column)); assertEquals((float) expectedValue, rows.getFloat(name));
                    assertEquals(Double.valueOf(expectedValue), rows.getObject(column, Double.class));
                    assertEquals(Float.valueOf((float) expectedValue), rows.getObject(name, Float.class));
                    assertEquals(Double.toString(expectedValue), rows.getString(name));
                    assertTrue(rows.getString("_value").contains("\"" + name + "\":\"" + expectedValue + "\""));
                }
            }
            assertFalse(rows.next());
        }
        // A computed column named like a schema field must still keep SQLite's type.
        try (var rows = statement.executeQuery("SELECT count(*) AS bool FROM " + table + " WHERE _offset>=0")) {
            assertType(rows, "bool", JDBCType.INTEGER);
            assertTrue(rows.next()); assertEquals(5, rows.getLong(1)); assertFalse(rows.next());
        }
    }

    private static void assertType(ResultSet rows, String name, JDBCType expected) throws SQLException {
        int column = rows.findColumn(name);
        assertEquals(expected.getVendorTypeNumber(), rows.getMetaData().getColumnType(column), name);
        assertEquals(expected.getName(), rows.getMetaData().getColumnTypeName(column), name);
    }

    /** PLAN §12.1–12.2: getTables refreshes Kafka's list on every call; getColumns uses only that list
     * or cached topics, never Kafka. §6.2: collision-aware names agree with query registration;
     * never fetches records or creates tables, and reports the two modes' primary keys.
     */
    @Test void metadataBrowsesWithoutSyncAndFollowsCachedViews() throws Exception {
        String base = TestStack.uniqueTopic("jdbc-metadata");
        String append = base + "-append", compact = base + "-compact", excluded = base + "-excluded";
        TestStack.createTopic(append, 2, Map.of("cleanup.policy", "delete"));
        TestStack.createTopic(compact, 1, Map.of("cleanup.policy", "compact"));
        TestStack.createTopic(excluded, 1, Map.of());
        send(append, 1, null); send(compact, 1, null);
        var registryRequests = new AtomicInteger();
        var http = HttpClient.newHttpClient();
        var registry = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        registry.createContext("/", exchange -> {
            registryRequests.incrementAndGet();
            try {
                var reply = http.send(HttpRequest.newBuilder(URI.create(TestStack.REGISTRY + exchange.getRequestURI())).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                exchange.sendResponseHeaders(reply.statusCode(), reply.body().length);
                exchange.getResponseBody().write(reply.body());
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new java.io.IOException(e); }
            finally { exchange.close(); }
        });
        registry.start();
        Properties p = settings();
        p.setProperty("schema.registry.url", "http://localhost:" + registry.getAddress().getPort());
        p.setProperty("topics.include", "(?i)(sqlite[_-])?" + Pattern.quote(base) + ".*");
        p.setProperty("topics.exclude", Pattern.quote(excluded));
        var config = DriverConfig.load(URL, p);
        try (var c = DriverManager.getConnection(URL, p); var control = CacheContext.acquire(config);
             var clients = KafkaClients.acquire(config)) {
            // Guard the real shared admin: after loading the shared topic list, browsing makes no Kafka calls.
            Field adminField = KafkaClients.class.getDeclaredField("admin"); adminField.setAccessible(true);
            Admin original = (Admin) adminField.get(clients.clients());
            var forbidKafka = new AtomicBoolean(false);
            var calls = new ArrayList<String>();
            var configLookups = new ArrayList<Set<String>>();
            adminField.set(clients.clients(), Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (self, method, args) -> {
                calls.add(method.getName());
                if (method.getName().equals("describeConfigs")) {
                    @SuppressWarnings("unchecked") var resources = (Collection<org.apache.kafka.common.config.ConfigResource>) args[0];
                    configLookups.add(resources.stream().map(org.apache.kafka.common.config.ConfigResource::name).collect(java.util.stream.Collectors.toSet()));
                }
                assertFalse(forbidKafka.get(), "unexpected Kafka call: " + method.getName());
                try { return method.invoke(original, args); }
                catch (InvocationTargetException e) { throw e.getCause(); }
            }));
            try {
                DatabaseMetaData metadata = c.getMetaData();
                assertSame(c, metadata.getConnection()); assertTrue(metadata.isReadOnly()); assertFalse(metadata.supportsTransactions());
                assertEquals(URL, metadata.getURL()); assertEquals("KafCacheQL", metadata.getDriverName());
                // PLAN §12.2: expose KafCacheQL's product identity so tools use standard JDBC introspection.
                assertAll(() -> assertEquals("KafCacheQL", metadata.getDatabaseProductName()),
                        () -> assertEquals(metadata.getDriverVersion(), metadata.getDatabaseProductVersion()));
                // 0013d / PLAN §12.2: database versions are the driver's; unimplemented metadata
                // returns empty results with the names and types in java.sql.DatabaseMetaData's contract.
                assertAll(
                        () -> assertEquals(metadata.getDriverMajorVersion(), metadata.getDatabaseMajorVersion()),
                        () -> assertEquals(metadata.getDriverMinorVersion(), metadata.getDatabaseMinorVersion()),
                        () -> emptyMetadata(metadata.getFunctions(null, null, "%"),
                                List.of("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
                                JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.SMALLINT, JDBCType.VARCHAR),
                        () -> emptyMetadata(metadata.getFunctionColumns(null, null, "%", "%"),
                                List.of("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
                                        "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
                                JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.SMALLINT, JDBCType.INTEGER, JDBCType.VARCHAR,
                                JDBCType.INTEGER, JDBCType.INTEGER, JDBCType.SMALLINT, JDBCType.SMALLINT, JDBCType.SMALLINT, JDBCType.VARCHAR,
                                JDBCType.INTEGER, JDBCType.INTEGER, JDBCType.VARCHAR, JDBCType.VARCHAR),
                        () -> emptyMetadata(metadata.getPseudoColumns(null, null, "%", "%"),
                                List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                                        "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"),
                                JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.INTEGER, JDBCType.INTEGER,
                                JDBCType.INTEGER, JDBCType.INTEGER, JDBCType.VARCHAR, JDBCType.VARCHAR, JDBCType.INTEGER, JDBCType.VARCHAR),
                        () -> emptyMetadata(metadata.getClientInfoProperties(), List.of("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"),
                                JDBCType.VARCHAR, JDBCType.INTEGER, JDBCType.VARCHAR, JDBCType.VARCHAR),
                        () -> emptyMetadata(metadata.getSchemas("missing", "%"), List.of("TABLE_SCHEM", "TABLE_CATALOG"),
                                JDBCType.VARCHAR, JDBCType.VARCHAR),
                        () -> assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, metadata.getRowIdLifetime()),
                        () -> assertFalse(metadata.supportsStoredFunctionsUsingCallSyntax()),
                        () -> assertFalse(metadata.autoCommitFailureClosesAllResultSets()),
                        () -> assertFalse(metadata.generatedKeyAlwaysReturned()));
                assertEquals("\"", metadata.getIdentifierQuoteString());
                assertEquals(control.read(r -> r.getMetaData().getSQLKeywords()), metadata.getSQLKeywords());
                assertEquals(control.read(r -> r.getMetaData().getStringFunctions()), metadata.getStringFunctions());
                assertEquals(control.read(r -> r.getMetaData().getNumericFunctions()), metadata.getNumericFunctions());
                assertEquals(control.read(r -> r.getMetaData().getTimeDateFunctions()), metadata.getTimeDateFunctions());
                assertEquals(control.read(r -> r.getMetaData().getSystemFunctions()), metadata.getSystemFunctions());
                try (var r = metadata.getCatalogs()) { assertTrue(r.next()); assertEquals(c.getCatalog(), r.getString("TABLE_CAT")); assertFalse(r.next()); }
                try (var r = metadata.getSchemas()) { assertTrue(r.next()); assertEquals(c.getSchema(), r.getString("TABLE_SCHEM")); assertFalse(r.next()); }
                List<String> before = cacheTables(control);
                var fixed = List.of("_partition", "_offset", "_timestamp", "_key", "_value", "_headers", "_key_schema_id", "_value_schema_id", "_decode_error");
                try (var r = metadata.getColumns(null, null, base + "-no-such-topic", null)) { assertFalse(r.next()); }
                assertTrue(calls.isEmpty()); forbidKafka.set(true);
                try (var r = metadata.getColumns(null, null, append, "%")) { assertFalse(r.next()); }
                forbidKafka.set(false);
                assertEquals(before, cacheTables(control)); assertTrue(calls.isEmpty()); assertEquals(0, registryRequests.get());
                try (var r = metadata.getTables(null, null, base + "-%", new String[] {"TABLE"})) {
                    var names = new TreeSet<String>();
                    while (r.next()) {
                        names.add(r.getString("TABLE_NAME")); assertEquals("TABLE", r.getString("TABLE_TYPE"));
                        assertEquals("", r.getString("REMARKS"));
                    }
                    assertEquals(Set.of(append, compact), names);
                }
                assertEquals(List.of("listTopics"), calls); calls.clear(); forbidKafka.set(true);
                try (var r = metadata.getColumns(null, null, append, "%")) { assertEquals(fixed, columnNames(r)); }
                assertTrue(calls.isEmpty()); forbidKafka.set(false);
                try (var r = metadata.getTables(null, null, "%", null)) {
                    var names = new TreeSet<String>(); while (r.next()) names.add(r.getString("TABLE_NAME"));
                    assertEquals(Set.of(append, compact), names, "include/exclude filters and hidden internal tables");
                }
                assertEquals(before, cacheTables(control)); assertEquals(List.of("listTopics"), calls); calls.clear(); assertEquals(0, registryRequests.get());
                metadataNamesAndRefresh(metadata, c, control, calls, forbidKafka, base, fixed);
                forbidKafka.set(false);
                for (String topic : List.of(append, compact)) {
                    try (var s = c.createStatement(); var r = s.executeQuery("SELECT * FROM " + StorageDdl.quote(topic) + " WHERE _offset>=0")) {
                        var view = r.getMetaData();
                        assertTrue(r.next()); assertFalse(r.next());
                        calls.clear(); forbidKafka.set(true);
                        var afterQuery = cacheTables(control); int registryBefore = registryRequests.get();
                        try (var columns = metadata.getColumns(c.getCatalog(), c.getSchema(), topic, "%")) {
                            for (int i = 1; i <= view.getColumnCount(); i++) {
                                assertTrue(columns.next()); assertEquals(view.getColumnName(i), columns.getString("COLUMN_NAME"));
                                assertEquals(view.getColumnType(i), columns.getInt("DATA_TYPE"));
                                assertEquals(view.getColumnTypeName(i), columns.getString("TYPE_NAME"));
                                assertEquals(i, columns.getInt("ORDINAL_POSITION"));
                            }
                            assertFalse(columns.next());
                        }
                        try (var columns = metadata.getColumns(null, null, topic, "\\_off%")) {
                            assertEquals(List.of("_offset"), columnNames(columns));
                        }
                        try (var keys = metadata.getPrimaryKeys(null, null, topic)) {
                            var bySequence = new TreeMap<Integer, String>();
                            while (keys.next()) bySequence.put(keys.getInt("KEY_SEQ"), keys.getString("COLUMN_NAME"));
                            assertEquals(List.of("_partition", topic.equals(append) ? "_offset" : "_key"), new ArrayList<>(bySequence.values()));
                        }
                        forbidKafka.set(false);
                        try (var tables = metadata.getTables(null, null, topic, null)) {
                            assertTrue(tables.next()); String remarks = tables.getString("REMARKS");
                            assertTrue(remarks.contains(topic.equals(append) ? "append" : "compacted"), remarks);
                            assertTrue(remarks.contains(topic.equals(append) ? "2" : "1"), remarks); assertFalse(tables.next());
                        }
                        assertEquals(List.of("listTopics"), calls); calls.clear(); forbidKafka.set(true);
                        try (var indexes = metadata.getIndexInfo(null, null, topic, false, false);
                             var imported = metadata.getImportedKeys(null, null, topic); var exported = metadata.getExportedKeys(null, null, topic)) {
                            assertFalse(indexes.next()); assertFalse(imported.next()); assertFalse(exported.next());
                        }
                        assertEquals(afterQuery, cacheTables(control)); assertTrue(calls.isEmpty()); assertEquals(registryBefore, registryRequests.get());
                        forbidKafka.set(false);
                    }
                }
                // PLAN §12: append keys are (_partition, _offset), compacted keys (_partition, _key).
                // Navigator's null/pattern requests list all topics; uncached modes need one batched config call.
                String uncachedCompact = base + "-uncached-compact";
                TestStack.createTopic(uncachedCompact, 1, Map.of("cleanup.policy", "compact"));
                var listed = new TreeSet<String>();
                try (var tables = metadata.getTables(null, null, "%", null)) {
                    while (tables.next()) listed.add(tables.getString("TABLE_NAME"));
                }
                var beforeKeys = cacheTables(control); int registryBeforeKeys = registryRequests.get();
                for (String pattern : Arrays.asList("%", null, base + "-%", "_", "$%")) {
                    calls.clear(); configLookups.clear();
                    var keysByTable = new TreeMap<String, TreeMap<Integer, String>>();
                    try (var keys = metadata.getPrimaryKeys(null, null, pattern)) {
                        while (keys.next()) {
                            assertEquals("main", keys.getString("TABLE_CAT"));
                            assertNull(keysByTable.computeIfAbsent(keys.getString("TABLE_NAME"), ignored -> new TreeMap<>())
                                    .put(keys.getInt("KEY_SEQ"), keys.getString("COLUMN_NAME")), "each key appears once");
                        }
                    }
                    assertEquals(listed, keysByTable.keySet());
                    keysByTable.forEach((table, keys) -> assertEquals(List.of("_partition",
                            table.equals(compact) || table.equals(uncachedCompact) ? "_key" : "_offset"), new ArrayList<>(keys.values())));
                    assertEquals(List.of("listTopics", "describeConfigs"), calls);
                    assertEquals(List.of(Set.of(base + "-new", uncachedCompact)), configLookups,
                            "one lookup for both uncached modes, never a pattern or a cached topic");
                }
                assertEquals(beforeKeys, cacheTables(control)); assertEquals(registryBeforeKeys, registryRequests.get());
                assertTrue(registryRequests.get() > 0, "the recording registry served actual decoding requests");
                Properties shown = new Properties(); shown.putAll(p); shown.setProperty("metadata.show.internal", "true");
                calls.clear();
                var other = DriverManager.getConnection(URL, shown);
                try {
                    forbidKafka.set(true);
                    try (var r = other.getMetaData().getColumns(null, null, append, "n")) { assertEquals(List.of("n"), columnNames(r)); }
                    assertTrue(calls.isEmpty()); forbidKafka.set(false);
                    try (var r = other.getMetaData().getTables(null, null, "$%", null)) {
                        var names = new TreeSet<String>(); while (r.next()) names.add(r.getString("TABLE_NAME"));
                        assertTrue(names.containsAll(Set.of("$meta", "$topics", "$partitions", "$ranges", "$schemas")), names.toString());
                        assertTrue(names.stream().allMatch(n -> n.startsWith("$")));
                    }
                    assertEquals(List.of("listTopics"), calls);
                    calls.clear(); forbidKafka.set(true);
                    try (var r = other.getMetaData().getColumns(null, null, "$topics", "topic")) { assertEquals(List.of("topic"), columnNames(r)); }
                    assertTrue(calls.isEmpty()); forbidKafka.set(false);
                    try (var r = other.getMetaData().getTables("missing", null, "%", null)) { assertFalse(r.next()); }
                    try (var r = other.getMetaData().getTables(null, "missing", "%", null)) { assertFalse(r.next()); }
                    try (var r = other.getMetaData().getTables(null, null, "%", new String[] {"VIEW"})) { assertFalse(r.next()); }
                    var detached = other.getMetaData().getTables(null, null, "%", null);
                    other.close(); assertTrue(detached.isClosed()); detached.close(); assertEquals(Collections.nCopies(4, "listTopics"), calls);
                } finally { other.close(); }
            } finally { adminField.set(clients.clients(), original); }
        } finally { registry.stop(0); }
    }

    private static void emptyMetadata(ResultSet rows, List<String> names, JDBCType... types) throws SQLException {
        try (rows) {
            assertFalse(rows.next());
            var metadata = rows.getMetaData();
            assertEquals(names.size(), metadata.getColumnCount());
            assertEquals(names.size(), types.length);
            for (int i = 1; i <= names.size(); i++) {
                assertEquals(names.get(i - 1), metadata.getColumnName(i));
                assertEquals(names.get(i - 1), metadata.getColumnLabel(i));
                assertEquals(types[i - 1].getVendorTypeNumber(), metadata.getColumnType(i), names.get(i - 1));
                assertEquals(types[i - 1].getName(), metadata.getColumnTypeName(i), names.get(i - 1));
                assertEquals(i, rows.findColumn(names.get(i - 1)));
            }
        }
    }

    private static void metadataNamesAndRefresh(DatabaseMetaData metadata, Connection c, CacheContext.Lease control,
            List<String> calls, AtomicBoolean forbidKafka, String base, List<String> fixed) throws Exception {
        String lower = base + "-orders", upper = base + "-Orders";
        var topics = List.of(lower, upper, "sqlite_" + base, "sqlite-" + base);
        for (String topic : topics) { TestStack.createTopic(topic, 1, Map.of()); TestStack.sendString(topic, "key", topic, null); }
        String deleted = base + "-removed"; TestStack.createTopic(deleted, 1, Map.of());
        forbidKafka.set(true);
        try (var columns = metadata.getColumns(null, null, lower, null)) { assertFalse(columns.next(), "getColumns keeps the latest list until Refresh"); }
        assertTrue(calls.isEmpty()); forbidKafka.set(false);
        var before = cacheTables(control);
        var names = new TreeSet<String>();
        try (var rows = metadata.getTables(null, null, "%", null)) { while (rows.next()) names.add(rows.getString("TABLE_NAME")); }
        assertEquals(List.of("listTopics"), calls); calls.clear(); forbidKafka.set(true);
        assertEquals(7, names.size()); assertEquals(7, names.stream().map(n -> n.toLowerCase(Locale.ROOT)).distinct().count());
        try (var columns = metadata.getColumns(null, null, "%", null)) {
            var byTable = new TreeMap<String, List<String>>();
            while (columns.next()) byTable.computeIfAbsent(columns.getString("TABLE_NAME"), ignored -> new ArrayList<>()).add(columns.getString("COLUMN_NAME"));
            assertEquals(names, byTable.keySet()); byTable.values().forEach(columnsForTable -> assertEquals(fixed, columnsForTable));
        }
        assertTrue(calls.isEmpty()); assertEquals(before, cacheTables(control));
        forbidKafka.set(false);
        // Primary-key discovery also resolves advertised names before asking Kafka for their mode.
        for (String view : names.stream().filter(n -> n.contains("$")).toList()) {
            try (var keys = metadata.getPrimaryKeys(null, null, view)) {
                var bySequence = new TreeMap<Integer, String>();
                while (keys.next()) bySequence.put(keys.getInt("KEY_SEQ"), keys.getString("COLUMN_NAME"));
                assertEquals(List.of("_partition", "_offset"), new ArrayList<>(bySequence.values()));
            }
        }
        assertEquals(before, cacheTables(control));
        // Query suffixed names first, then their colliding partners: every name keeps its topic identity.
        var seen = new HashSet<String>();
        var views = names.stream().filter(n -> n.startsWith("sqlite-") || n.equalsIgnoreCase(lower) || n.equalsIgnoreCase(lower + "$2"))
                .sorted(Comparator.comparing((String n) -> !n.contains("$")).thenComparing(n -> n)).toList();
        assertEquals(4, views.size());
        for (String view : views) {
            try (var statement = c.createStatement(); var rows = statement.executeQuery("SELECT _value FROM " + StorageDdl.quote(view) + " WHERE _offset>=0")) {
                assertTrue(rows.next()); String topic = rows.getString(1).replace("\"", "");
                assertTrue(topics.contains(topic)); assertTrue(seen.add(topic), "distinct views must return distinct topics"); assertFalse(rows.next());
                String normalized = topic.startsWith("sqlite_") ? "sqlite-" + topic.substring(7) : topic;
                assertTrue(view.equals(normalized) || view.equals(normalized + "$2"), view + " -> " + topic);
            }
        }
        assertEquals(new HashSet<>(topics), seen);
        calls.clear();
        try (var rows = metadata.getTables(null, null, "%", null)) {
            var registeredNames = new TreeSet<String>(); while (rows.next()) registeredNames.add(rows.getString("TABLE_NAME"));
            assertEquals(names, registeredNames, "registration preserves the names advertised by metadata");
        }
        TestStack.deleteTopic(deleted);
        String added = base + "-new"; TestStack.createTopic(added, 1, Map.of());
        calls.clear();
        try (var rows = metadata.getTables(null, null, "%", null)) {
            var refreshed = new HashSet<String>(); while (rows.next()) refreshed.add(rows.getString("TABLE_NAME"));
            assertTrue(refreshed.contains(added)); assertFalse(refreshed.contains(deleted));
        }
        assertEquals(List.of("listTopics"), calls); calls.clear(); forbidKafka.set(true);
        try (var rows = metadata.getColumns(null, null, added, null)) { assertEquals(fixed, columnNames(rows)); }
        // A deleted, uncached topic disappears from columns on Refresh as well.
        try (var rows = metadata.getColumns(null, null, deleted, null)) { assertFalse(rows.next()); }
        assertTrue(calls.isEmpty()); forbidKafka.set(false);
    }

    private static List<String> columnNames(ResultSet rows) throws SQLException {
        var names = new ArrayList<String>(); while (rows.next()) names.add(rows.getString("COLUMN_NAME")); return names;
    }
    private static List<String> cacheTables(CacheContext.Lease cache) throws SQLException {
        return cache.read(reader -> {
            var names = new ArrayList<String>();
            try (var s = reader.createStatement(); var r = s.executeQuery("SELECT name || ':' || sql FROM sqlite_schema ORDER BY name")) {
                while (r.next()) names.add(r.getString(1));
            }
            return names;
        });
    }

    /** PLAN §16 tool compatibility / 0013a: Hikari validation and borrowing, JdbcTemplate queryForList,
     * and a Spring read-only transaction work with the real driver; §12.3 accepts Spring's transaction calls.
     */
    @Test void hikariAndSpringJdbcReadOnlyTransaction() throws Exception {
        String topic = topic("delete"); send(topic, 1, null); send(topic, 2, null);
        String sql = "SELECT n FROM " + StorageDdl.quote(topic) + " WHERE _offset>=0 ORDER BY _offset";
        var config = new HikariConfig();
        config.setDriverClassName(KafCacheQLDriver.class.getName());
        config.setJdbcUrl(URL); config.setDataSourceProperties(settings());
        config.setMaximumPoolSize(1); config.setMinimumIdle(1);
        try (var pool = new HikariDataSource(config)) {
            HikariPoolMXBean state = pool.getHikariPoolMXBean();
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                assertTrue(connection.isValid(5)); assertTrue(connection.isReadOnly());
                assertEquals(1, state.getActiveConnections());
                try (var rows = statement.executeQuery(sql)) { assertEquals(List.of(1, 2), values(rows)); }
            }
            assertReturned(state);
            var jdbc = new JdbcTemplate(pool);
            var expected = List.of(Map.of("n", 1L), Map.of("n", 2L));
            assertEquals(expected, jdbc.queryForList(sql));
            assertReturned(state);
            var transaction = new TransactionTemplate(new DataSourceTransactionManager(pool));
            transaction.setReadOnly(true);
            assertEquals(expected, transaction.execute(status -> {
                assertTrue(status.isNewTransaction());
                assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
                assertTrue(TransactionSynchronizationManager.isCurrentTransactionReadOnly());
                assertEquals(1, state.getActiveConnections());
                return jdbc.queryForList(sql);
            }));
            assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
            assertReturned(state);
            try (var connection = pool.getConnection(); var statement = connection.createStatement()) {
                assertTrue(connection.isValid(5)); assertTrue(connection.getAutoCommit()); assertTrue(connection.isReadOnly());
                try (var rows = statement.executeQuery(sql)) { assertEquals(List.of(1, 2), values(rows)); }
            }
            assertReturned(state);
        }
    }

    private static void assertReturned(HikariPoolMXBean state) {
        assertEquals(0, state.getActiveConnections()); assertEquals(1, state.getIdleConnections()); assertEquals(1, state.getTotalConnections());
    }

    /** PLAN §10.1 Readers: each result has its own snapshot; exhaustion, maxRows, close and parent close
     * close the SQLite cursor before COMMIT and return the reader to the pool.
     */
    @Test void resultSnapshotsAndEveryEndReturnReaders() throws Exception {
        String topic = topic("compact"); String sql = "SELECT n FROM " + StorageDdl.quote(topic) + " ORDER BY n";
        send(topic, 0, null); send(topic, 1, null);
        Properties p = settings();
        try (var control = CacheContext.acquire(DriverConfig.load(URL, p)); var c = DriverManager.getConnection(URL, p);
             var writer = DriverManager.getConnection(URL, p); var a = c.createStatement(); var b = c.createStatement()) {
            var first = a.executeQuery(sql);
            assertTrue(first.next()); assertEquals(0, first.getInt(1));
            send(topic, 2, null);
            try (var s = writer.createStatement(); var r = s.executeQuery(sql)) { assertEquals(List.of(0, 1, 2), values(r)); }
            var second = b.executeQuery(sql);
            assertTrue(second.next()); assertEquals(0, second.getInt(1));
            assertEquals(List.of(1), values(first));
            send(topic, 3, null);
            try (var s = writer.createStatement(); var r = s.executeQuery(sql)) { assertEquals(List.of(0, 1, 2, 3), values(r)); }
            assertEquals(List.of(1, 2), values(second)); first.close(); second.close();
            int nextValue = 4;
            for (String end : List.of("exhaustion", "maxRows", "close", "statement", "connection")) {
                var owner = DriverManager.getConnection(URL, p);
                var s = owner.createStatement();
                if (end.equals("maxRows")) s.setMaxRows(1);
                var r = s.executeQuery(sql);
                send(topic, nextValue++, null);
                try (var update = writer.createStatement(); var changed = update.executeQuery(sql)) { values(changed); }
                Set<Connection> before = pooled(control);
                assertTrue(r.next()); assertEquals(0, r.getInt(1));
                if (end.equals("maxRows")) {
                    assertEquals(before, pooled(control), "last row remains readable before next()");
                    assertFalse(r.next());
                } else if (end.equals("exhaustion")) { while (r.next()) {} }
                else if (end.equals("close")) r.close();
                else if (end.equals("statement")) s.close();
                else owner.close();
                Set<Connection> after = pooled(control);
                assertEquals(before.size() + 1, after.size(), end);
                assertTrue(after.containsAll(before), end);
                assertTrue(after.contains(control.read(x -> x)), "a returned reader is reused: " + end);
                assertTrue(checkpoint(control), "the ended cursor no longer pins the WAL: " + end);
                r.close(); s.close(); owner.close();
            }
        }
    }

    /** PLAN §10.1 deadlines and cancellation: committed batches survive cancellation; SQLite is interrupted
     * at the execution deadline; an already-dispatched callback cannot interrupt the next reader borrower.
     */
    @Test void cancelSyncResumeTimeoutSqlAndIgnoreLateCancel() throws Exception {
        String topic = topic("delete"); String sql = "SELECT n FROM " + StorageDdl.quote(topic) + " WHERE _offset>=0 ORDER BY _offset";
        for (int n = 0; n < 30; n++) send(topic, n, null);
        Properties p = settings();
        CountDownLatch committed = new CountDownLatch(1), release = new CountDownLatch(1);
        List<Long> stored = new CopyOnWriteArrayList<>();
        try (var control = CacheContext.acquire(DriverConfig.load(URL, p)); var c = DriverManager.getConnection(URL, p);
             var s = c.createStatement(); var executor = Executors.newSingleThreadExecutor()) {
            control.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch batch) throws SQLException {
                    batch.records().forEach(r -> stored.add(r.offset()));
                    if (stored.size() == 3) { committed.countDown(); await(release); }
                }
            });
            var cancelled = executor.submit(() -> { try (var r = s.executeQuery(sql)) { values(r); } return null; });
            assertTrue(committed.await(10, TimeUnit.SECONDS)); s.cancel(); release.countDown();
            var failure = assertThrows(ExecutionException.class, () -> cancelled.get(10, TimeUnit.SECONDS));
            assertInstanceOf(SQLException.class, failure.getCause()); assertEquals("57014", ((SQLException) failure.getCause()).getSQLState());
            assertEquals(List.of(0L, 1L, 2L), stored);
            try (var r = s.executeQuery(sql)) { assertEquals(30, values(r).size()); }
            assertEquals(java.util.stream.LongStream.range(0, 30).boxed().toList(), stored, "resume never refetches committed offsets");
            s.setQueryTimeout(1);
            long started = System.nanoTime();
            assertThrows(SQLTimeoutException.class, () -> s.executeQuery("WITH RECURSIVE x(n) AS "
                    + "(SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<1000000000) SELECT sum(n) FROM x"));
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5), "SQLite is interrupted, not merely checked afterwards");
            s.setQueryTimeout(0);
            var old = s.executeQuery("SELECT 1");
            Execution execution = (Execution) field(Proxy.getInvocationHandler(s), "execution");
            @SuppressWarnings("unchecked") Set<Runnable> listeners = (Set<Runnable>) field(execution, "listeners");
            Runnable late = listeners.iterator().next();
            old.close();
            CountDownLatch running = new CountDownLatch(1), finish = new CountDownLatch(1);
            Connection physical = control.read(reader -> {
                Function.create(reader, "test_wait", new Function() {
                    @Override protected void xFunc() throws SQLException { running.countDown(); await(finish); result(42); }
                });
                return reader;
            });
            var next = executor.submit(() -> {
                try (var r = s.executeQuery("WITH RECURSIVE x(n) AS (SELECT test_wait() UNION ALL "
                        + "SELECT n+1 FROM x WHERE n<1000) SELECT max(n) FROM x")) {
                    assertTrue(r.next()); return r.getInt(1);
                }
            });
            try {
                assertTrue(running.await(10, TimeUnit.SECONDS));
                assertFalse(pooled(control).contains(physical), "the next query borrowed the same reader");
                late.run(); finish.countDown();
                assertEquals(1000, next.get(10, TimeUnit.SECONDS));
            } finally { finish.countDown(); }
            try (var r = s.executeQuery("SELECT 7")) { assertEquals(List.of(7), values(r)); }
        } finally { release.countDown(); }

        // A fatal worker error (e.g. a replaced JAR losing a class) must survive on later JDBC queries,
        // including cache-only reads (§10.1: those do not go through the worker).
        Properties broken = settings(); broken.setProperty("cache.path", dir.resolve("dead-worker.db").toString());
        SQLException closeFailure = assertThrows(SQLException.class, () -> {
            try (var control = CacheContext.acquire(DriverConfig.load(URL, broken));
                 var c = DriverManager.getConnection(URL, broken); var s = c.createStatement()) {
                try (var rows = s.executeQuery(sql)) { assertEquals(30, values(rows).size()); }
                send(topic, 30, null);
                control.hooks(new SyncWorker.Hooks() {
                    @Override public void beforeCommit(SyncBatch batch) { throw new NoClassDefFoundError("demo.MissingJdbcClass"); }
                });
                assertThrows(NoClassDefFoundError.class, () -> s.executeQuery(sql));
                Thread worker = (Thread) field(field(field(control, "context"), "worker"), "thread");
                worker.join(10_000); assertFalse(worker.isAlive());
                for (String next : List.of(sql, "SELECT n FROM " + StorageDdl.quote(topic) + " WHERE _offset<30", "SELECT 1")) {
                    SQLException error = assertThrows(SQLException.class, () -> s.executeQuery(next));
                    assertTrue(error.getMessage().contains("demo.MissingJdbcClass"), error.toString());
                    assertNotNull(error.getCause());
                    assertTrue(error.getCause().toString().contains("demo.MissingJdbcClass"), error.getCause().toString());
                    Throwable root = error; while (root.getCause() != null) root = root.getCause();
                    assertTrue(root.toString().contains("NoClassDefFoundError"), root.toString());
                }
            }
        });
        assertTrue(closeFailure.getMessage().contains("demo.MissingJdbcClass"), closeFailure.toString());
    }
    static void await(CountDownLatch latch) throws SQLException {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new SQLException("test latch timed out"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException(e); }
    }
    static Object field(Object object, String name) throws Exception {
        Field f = object.getClass().getDeclaredField(name); f.setAccessible(true); return f.get(object);
    }
    static boolean checkpoint(CacheContext.Lease control) throws SQLException {
        return control.read(reader -> {
            // Checkpoint outside this test observer's transaction, so only the JDBC cursor can pin it.
            try (var s = reader.createStatement()) {
                s.execute("COMMIT"); s.execute("PRAGMA busy_timeout=0");
                try (var r = s.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
                    return r.next() && r.getInt(1) == 0 && r.getInt(2) == 0;
                } finally { s.execute("PRAGMA busy_timeout=5000"); s.execute("BEGIN"); }
            }
        });
    }
    static Set<Connection> pooled(CacheContext.Lease control) throws Exception {
        @SuppressWarnings("unchecked") var readers = (Deque<Connection>) field(field(control, "context"), "readers");
        synchronized (readers) { return Set.copyOf(readers); }
    }
}
