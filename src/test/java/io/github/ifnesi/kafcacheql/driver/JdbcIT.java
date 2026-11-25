package io.github.ifnesi.kafcacheql.driver;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.sqlite.Function;

class JdbcIT {
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

    /** PLAN §12.3: read-only, Spring transaction compatibility, SELECT only, warnings, SQLite bindings;
     * §8.3: timestamp operands are folded afresh for each execution; §7.2/0015d: schema-derived result
     * types for every mapping, while user-computed columns keep SQLite's types.
     */
    @Test void jdbcBehaviourAndPreparedWindows() throws Exception {
        String topic = topic("delete"); String table = StorageDdl.quote(topic);
        long base = System.currentTimeMillis() - 10000;
        for (int n = 0; n < 4; n++) send(topic, n, base + 1000 * n);
        Properties p = settings();
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
            adminField.set(clients.clients(), Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (self, method, args) -> {
                calls.add(method.getName());
                assertFalse(forbidKafka.get(), "unexpected Kafka call: " + method.getName());
                try { return method.invoke(original, args); }
                catch (InvocationTargetException e) { throw e.getCause(); }
            }));
            try {
                DatabaseMetaData metadata = c.getMetaData();
                assertSame(c, metadata.getConnection()); assertTrue(metadata.isReadOnly()); assertFalse(metadata.supportsTransactions());
                assertEquals(URL, metadata.getURL()); assertEquals("KafCacheQL", metadata.getDriverName());
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
