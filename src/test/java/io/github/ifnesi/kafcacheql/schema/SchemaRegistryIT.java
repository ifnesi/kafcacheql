package io.github.ifnesi.kafcacheql.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.StoredSchema;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Schema lookup is not exposed by the unfinished driver connection. Use real Kafka/registry records
 * and SQLite JDBC readers here; credential rejection also exercises the sync worker and decoder.
 */
class SchemaRegistryIT {
    private static final Schema ORDER = new Schema.Parser().parse("""
            {"type":"record","name":"SchemaLookupOrder","fields":[{"name":"id","type":"string"}]}
            """);

    @TempDir
    Path dir;

    @BeforeAll
    static void stack() {
        TestStack.requireUp();
    }

    /** 0005a acceptance, line 13: "harness records in prefix and in header mode".
     * PLAN §7.1: "The driver checks the header first and then the prefix"; §17 question 1.
     */
    @Test
    void harnessKeyAndValueReferencesResolveInBothWireModes() throws Exception {
        String topic = TestStack.uniqueTopic("schema-references");
        TestStack.createTopic(topic, 1, java.util.Map.of());
        GenericData.Record order = new GenericData.Record(ORDER);
        order.put("id", "one");
        TestStack.sendAvroKeyed(topic, "key", order, null, TestStack.SchemaIdIn.PREFIX);
        TestStack.sendAvroKeyed(topic, "key", order, null, TestStack.SchemaIdIn.HEADER);
        var records = TestStack.readAll(topic, true);
        assertEquals(2, records.size());
        try (CacheStore cache = cache(); Connection reader = cache.newReader();
             SchemaRegistry registry = registry(TestStack.REGISTRY)) {
            for (boolean key : List.of(true, false)) {
                JsonNode metadata = get("/subjects/" + topic + (key ? "-key" : "-value") + "/versions/latest");
                String id = metadata.get("id").asText();
                String guid = metadata.get("guid").asText();
                var prefix = records.get(0);
                var header = records.get(1);
                byte[] prefixBytes = key ? prefix.key() : prefix.value();
                assertEquals(0, prefixBytes[0]);
                assertEquals(Integer.parseInt(id), ByteBuffer.wrap(prefixBytes, 1, 4).getInt());
                byte[] headerBytes = header.headers().lastHeader(key ? "__key_schema_id" : "__value_schema_id").value();
                assertEquals(17, headerBytes.length);
                assertEquals(1, headerBytes[0], "header version is 0x01");
                UUID uuid = UUID.fromString(guid);
                ByteBuffer bytes = ByteBuffer.wrap(headerBytes, 1, 16);
                assertEquals(uuid.getMostSignificantBits(), bytes.getLong());
                assertEquals(uuid.getLeastSignificantBits(), bytes.getLong());
                assertEquals(Optional.of(id), SchemaReferences.find(prefix.headers(), prefixBytes, key));
                assertEquals(Optional.of(guid), SchemaReferences.find(header.headers(), key ? header.key() : header.value(), key));
                assertEquals(Optional.of(guid), SchemaReferences.find(header.headers(), prefixBytes, key), "header wins");

                // Independent HTTP evidence: the GUID response has the schema but no integer ID.
                JsonNode byGuid = get("/schemas/guids/" + guid);
                assertFalse(byGuid.has("id"), byGuid.toString());
                for (String ref : List.of(id, guid)) {
                    StoredSchema schema = registry.lookup(reader, ref).orElseThrow();
                    assertEquals(ref, schema.ref());
                    assertEquals("AVRO", schema.type());
                    assertEquals(json(metadata.get("schema").asText()), json(schema.schemaText()));
                    persist(cache, schema);
                }
            }
            try (Statement s = reader.createStatement(); ResultSet rs = s.executeQuery("""
                    SELECT count(*), count(DISTINCT ref), min(type), min(refs), min(fetched_ms) FROM "$schemas"
                    """)) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1), "integer IDs and GUIDs remain distinct references");
                assertEquals(4, rs.getInt(2));
                assertEquals("AVRO", rs.getString(3));
                assertEquals("[]", rs.getString(4));
                assertTrue(rs.getLong(5) > 0);
            }
        }
    }

    /** 0005a acceptance, lines 13–14: "a truncated prefix or a wrong magic byte gives no reference".
     * PLAN §7.1: "byte 0 is 0x00 and bytes 1–4 are the integer schema ID (big-endian)".
     */
    @Test
    void truncatedOrWrongMagicPrefixesHaveNoReference() {
        for (boolean key : List.of(true, false)) {
            for (byte[] bytes : List.of(new byte[0], new byte[] {0}, new byte[] {0, 0}, new byte[] {0, 0, 0},
                    new byte[] {0, 0, 0, 0}, new byte[] {1, 0, 0, 0, 7}, new byte[] {(byte) 255, 0, 0, 0, 7})) {
                assertTrue(SchemaReferences.find(new RecordHeaders(), bytes, key).isEmpty());
            }
            assertTrue(SchemaReferences.find(null, null, key).isEmpty());
        }
    }

    /** 0005a acceptance, lines 14–15: "a second lookup of a stored schema makes no registry call (counted)".
     * PLAN §7.3: "A schema is fetched by ID or GUID the first time it is seen and stored in $schemas for good."
     */
    @Test
    void storedSchemasAvoidRegistryCallsEvenWithFreshClientsAndAReopenedCache() throws Exception {
        String subject = TestStack.uniqueTopic("stored-schema") + "-value";
        int id;
        try (var writer = new CachedSchemaRegistryClient(TestStack.REGISTRY, 10)) {
            id = writer.register(subject, new AvroSchema(ORDER));
        }
        String guid = get("/subjects/" + subject + "/versions/latest").get("guid").asText();
        try (CountingRegistry counted = new CountingRegistry()) {
            List<StoredSchema> stored;
            try (CacheStore cache = cache(); Connection reader = cache.newReader();
                 SchemaRegistry registry = registry(counted.url())) {
                StoredSchema byId = registry.lookup(reader, Integer.toString(id)).orElseThrow();
                StoredSchema byGuid = registry.lookup(reader, guid).orElseThrow();
                assertEquals(2, counted.calls.get(), "one HTTP call per new reference");
                stored = List.of(byId, byGuid);
                for (StoredSchema schema : stored) {
                    persist(cache, schema);
                }
            }
            counted.calls.set(0);
            try (CacheStore cache = cache(); Connection reader = cache.newReader();
                 SchemaRegistry fresh = registry(counted.url())) {
                for (StoredSchema schema : stored) {
                    assertEquals(schema, fresh.lookup(reader, schema.ref()).orElseThrow());
                }
                assertEquals(0, counted.calls.get(), "disk cache, independent of Confluent's in-memory cache");
            }
        }
    }

    /** 0005a acceptance, line 16: "an unreachable registry fails with an SQLException naming the URL".
     * PLAN §4.5: settings reach the clients unchanged; "Errors, and the causes they carry, are redacted";
     * §4.7: "Never log them"; §13: "Credentials never go in URLs, logs or the cache."
     */
    @Test
    void unreachableRegistryNamesItsUrlWithoutExposingCredentials() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        String url = "http://localhost:" + port;
        Properties props = new Properties();
        props.setProperty("schema.registry.url", "http://REGISTRY-USER:REGISTRY-CANARY@localhost:" + port);
        props.setProperty("http.connect.timeout.ms", "1000");
        props.setProperty("http.read.timeout.ms", "1000");
        LogCapture.start();
        StringWriter trace = new StringWriter();
        try (CacheStore cache = cache(); Connection reader = cache.newReader();
             SchemaRegistry registry = SchemaRegistry.open(settings(props))) {
            for (String ref : List.of("1", "095d71cf-1255-6b9d-5e33-0ad575b3df5d")) {
                SQLException e = assertThrows(SQLException.class, () -> registry.lookup(reader, ref));
                assertTrue(e.getMessage().contains(url), e.getMessage());
                e.printStackTrace(new PrintWriter(trace));
            }
        } finally {
            trace.append(String.join("\n", LogCapture.stop()));
        }
        assertFalse(trace.toString().contains("REGISTRY-CANARY"), trace.toString());
        assertFalse(trace.toString().contains("REGISTRY-USER"), trace.toString());

        // A real sync sends header credentials unchanged; an HTTP 401 echoes both the complete
        // Authorization value and its credential alone, as a registry rejecting a token might do.
        String topic = TestStack.uniqueTopic("header-credentials");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendBytes(topic, TestStack.utf8("key"),
                ByteBuffer.allocate(6).put((byte) 0).putInt(12345).put((byte) 0).array(), null);
        String authorization = "HEADER-AUTH-CANARY", session = "HEADER-SESSION-CANARY";
        var receivedAuthorization = new AtomicReference<String>();
        var receivedSession = new AtomicReference<String>();
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            receivedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            receivedSession.set(exchange.getRequestHeaders().getFirst("X-Session"));
            try {
                byte[] response = TestStack.utf8("{\"error_code\":401,\"message\":\"Rejected "
                        + receivedAuthorization.get() + " credential " + authorization + " session "
                        + receivedSession.get() + "\"}");
                exchange.sendResponseHeaders(401, response.length);
                exchange.getResponseBody().write(response);
            } finally { exchange.close(); }
        });
        server.start();
        Properties headers = new Properties();
        headers.setProperty("cache.path", dir.resolve("header-credentials.db").toString());
        headers.setProperty("schema.registry.url", "http://localhost:" + server.getAddress().getPort());
        headers.setProperty("request.header.Authorization", "Bearer " + authorization);
        headers.setProperty("request.header.X-Session", session);
        StringWriter headerTrace = new StringWriter();
        List<String> logs;
        String rendered;
        LogCapture.start();
        try {
            DriverConfig config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, headers);
            rendered = config.toString() + "\n" + ClientSettings.of(config);
            try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(20))) {
                SQLException failure = assertThrows(SQLException.class, () -> cache.sync(cache.plan(topic, execution), execution));
                assertTrue(failure.getMessage().contains("Rejected"), failure.getMessage());
                assertTrue(failure.getCause() != null, "the real Registry failure retains its sanitized cause");
                failure.printStackTrace(new PrintWriter(headerTrace));
            }
        } finally {
            logs = LogCapture.stop();
            server.stop(0);
        }
        assertEquals("Bearer " + authorization, receivedAuthorization.get());
        assertEquals(session, receivedSession.get());
        assertTrue(rendered.contains("request.header.Authorization="));
        assertTrue(rendered.contains("request.header.X-Session="), "header names stay visible");
        assertAll(
                () -> assertFalse(rendered.contains(authorization) || rendered.contains(session), "config hides header values"),
                () -> assertFalse(headerTrace.toString().contains(authorization) || headerTrace.toString().contains(session),
                        "the exception, causes and suppressed exceptions hide echoed header values"),
                () -> assertFalse(logs.stream().anyMatch(line -> line.contains(authorization) || line.contains(session)),
                        "the full sync log hides header values"));
    }

    private CacheStore cache() throws SQLException {
        return CacheStore.open(dir.resolve("cache.db"), Optional.of("schema-test-cluster"));
    }

    private static ClientSettings settings(Properties props) throws SQLException {
        return ClientSettings.of(DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, props));
    }

    private static SchemaRegistry registry(String url) throws SQLException {
        Properties props = new Properties();
        props.setProperty("schema.registry.url", url);
        return SchemaRegistry.open(settings(props));
    }

    private static void persist(CacheStore cache, StoredSchema schema) throws SQLException {
        CacheStore.writeTransaction(cache.writer(), () -> {
            SchemaStore.store(cache.writer(), schema);
            return null;
        });
    }

    private static JsonNode json(String text) throws Exception {
        return JacksonMapper.INSTANCE.readTree(text);
    }

    private static JsonNode get(String path) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            var response = client.send(HttpRequest.newBuilder(URI.create(TestStack.REGISTRY + path)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode(), response.body());
            return json(response.body());
        }
    }

    /** Count actual HTTP requests while forwarding them to the running registry. */
    private static final class CountingRegistry implements AutoCloseable {
        private final AtomicInteger calls = new AtomicInteger();
        private final HttpClient client = HttpClient.newHttpClient();
        private final HttpServer server;

        CountingRegistry() throws Exception {
            server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/", exchange -> {
                calls.incrementAndGet();
                try (exchange) {
                    HttpResponse<byte[]> response;
                    try {
                        response = client.send(HttpRequest.newBuilder(
                                URI.create(TestStack.REGISTRY + exchange.getRequestURI())).build(),
                                HttpResponse.BodyHandlers.ofByteArray());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new java.io.IOException(e);
                    }
                    exchange.getResponseHeaders().set("Content-Type", "application/vnd.schemaregistry.v1+json");
                    exchange.sendResponseHeaders(response.statusCode(), response.body().length);
                    exchange.getResponseBody().write(response.body());
                }
            });
            server.start();
        }

        String url() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
            client.close();
        }
    }
}
