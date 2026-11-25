package io.github.ifnesi.kafcacheql.sync;

import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sql.QueryPipeline;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The three 0010a flows: real Kafka -> sync entry point -> views in the cache file. */
class TypedViewsIT {
    private static final List<String> METADATA = List.of("_partition", "_offset", "_timestamp", "_key", "_value",
            "_headers", "_key_schema_id", "_value_schema_id", "_decode_error");
    private static final Schema KEY = schema("""
            {"type":"record","name":"ViewKey","fields":[{"name":"id","type":"string"}]}
            """);
    private static final String COMMON = """
            {"name":"_offset","type":"int"}, {"name":"_offset_1","type":"string"},
            {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
            {"name":"address","type":{"type":"record","name":"Address","fields":[{"name":"city","type":"string"}]}},
            {"name":"tags","type":{"type":"array","items":"string"}},
            {"name":"labels","type":{"type":"map","values":"int"}},
            """;
    private static final Schema FIRST = schema("{\"type\":\"record\",\"name\":\"ViewValue\",\"fields\":[" + COMMON + """
            {"name":"removed","type":"string"}, {"name":"changed","type":"int"},
            {"name":"stable","type":["null","int"],"default":null}]}
            """);
    private static final Schema SECOND = schema("{\"type\":\"record\",\"name\":\"ViewValue\",\"fields\":[" + COMMON + """
            {"name":"added","type":"string"}, {"name":"changed","type":"string"},
            {"name":"stable","type":["null","double"],"default":null}]}
            """);

    @TempDir Path dir;
    @BeforeAll static void stack() { TestStack.requireUp(); }

    /** PLAN §6.6: "fields of every value schema ID present in the table, and only those";
     * "Fields from older versions stay visible"; collisions use "_1, _2, …";
     * conflicts show "each row's JSON value as written, with no conversion" and a warning;
     * safe widenings stay typed. §7.2: the decimal view column "is numeric, so queries need no cast".
     */
    @Test void evolvingRecordsKeepOldFieldsNestedJsonNumericDecimalsAndWarnOnConflicts() throws Exception {
        String topic = topic("typed-evolution");
        try (var registry = new CachedSchemaRegistryClient(TestStack.REGISTRY, 10)) {
            registry.updateCompatibility(topic + "-value", "NONE");
            try (var cache = CacheContext.acquire(config()); var execution = execution()) {
                // Registration alone must not expose a schema, even when one is already registered.
                registry.register(topic + "-value", new AvroSchema(FIRST));
                cache.resolveTable(topic, execution);
                assertEquals(METADATA, cache.read(c -> columns(c, topic)));
                TestStack.sendAvroKeyed(topic, record(KEY, "id", "a"), value(FIRST, "12.34"), null, TestStack.SchemaIdIn.PREFIX);
                long generation = cache.sync(cache.plan(topic, execution), execution);
                assertTrue(cache.read(c -> columns(c, topic)).contains("removed"));
                String storageBefore = cache.read(c -> tableSql(c, generation));
                TestStack.sendAvroKeyed(topic, record(KEY, "id", "b"), value(SECOND, "2.66"), null, TestStack.SchemaIdIn.HEADER);
                cache.sync(cache.plan(topic, execution), execution);
                Schema unused = schema("""
                        {"type":"record","name":"ViewValue","fields":[{"name":"never_cached","type":"string"}]}
                        """);
                registry.register(topic + "-value", new AvroSchema(unused));
                cache.sync(cache.plan(topic, execution), execution);
                List<String> names = cache.read(c -> columns(c, topic));
                assertTrue(names.containsAll(List.of("key_id", "_offset_1", "_offset_1_1", "removed", "added", "amount", "address", "tags", "labels")), names.toString());
                assertEquals(names.size(), names.stream().map(String::toLowerCase).distinct().count());
                assertFalse(names.contains("never_cached"));
                assertEquals(storageBefore, cache.read(c -> tableSql(c, generation)), "evolution never alters storage");
                cache.read(c -> {
                    try (var s = c.createStatement(); var rows = s.executeQuery("SELECT * FROM " + q(topic) + " ORDER BY _offset")) {
                        assertTrue(rows.next());
                        assertEquals("a", rows.getString("key_id"));
                        assertEquals(0, rows.getLong("_offset"));
                        assertEquals(41, rows.getInt("_offset_1"));
                        assertEquals("payload", rows.getString("_offset_1_1"));
                        assertEquals("old", rows.getString("removed"));
                        assertNull(rows.getString("added"));
                        assertEquals("{\"city\":\"London\"}", rows.getString("address"));
                        assertEquals("[\"one\",\"two\"]", rows.getString("tags"));
                        assertEquals("{\"a\":1}", rows.getString("labels"));
                        assertEquals("7", rows.getString("changed"));
                        assertEquals(Types.VARCHAR, rows.getMetaData().getColumnType(rows.findColumn("changed")));
                        assertTrue(rows.next());
                        assertNull(rows.getString("removed"));
                        assertEquals("new", rows.getString("added"));
                        assertEquals("\"N/A\"", rows.getString("changed"));
                        assertFalse(rows.next());
                    }
                    try (var s = c.createStatement(); var rows = s.executeQuery("SELECT sum(amount), sum(stable), "
                            + "min(typeof(amount)), max(typeof(changed)) FROM " + q(topic))) {
                        assertTrue(rows.next()); assertEquals(15.0, rows.getDouble(1), 0.000001);
                        assertEquals(4.5, rows.getDouble(2), 0.000001);
                        assertEquals("real", rows.getString(3)); assertEquals("text", rows.getString(4));
                    }
                    try (var s = c.createStatement(); var rows = s.executeQuery("SELECT _value ->> '$.amount' FROM " + q(topic) + " ORDER BY _offset")) {
                        assertTrue(rows.next()); assertEquals("12.34", rows.getString(1));
                        assertTrue(rows.next()); assertEquals("2.66", rows.getString(1));
                    }
                    return null;
                });
                List<String> refs = cache.read(c -> {
                    try (var s = c.createStatement(); var rows = s.executeQuery("SELECT _value_schema_id FROM " + q(topic) + " ORDER BY _offset")) {
                        var result = new ArrayList<String>(); while (rows.next()) result.add(rows.getString(1)); return result;
                    }
                });
                try (var logs = LogCapture.capture()) {
                    var result = QueryPipeline.execute(cache, "SELECT changed FROM " + q(topic) + " WHERE _offset >= 0", Map.of(), execution);
                    assertEquals(1, result.warnings().size());
                    String warning = result.warnings().getFirst().getMessage();
                    assertTrue(warning.contains("changed") && refs.stream().allMatch(warning::contains), warning);
                    assertTrue(logs.lines().stream().anyMatch(l -> l.startsWith("WARN ") && l.contains("changed")));
                }
                assertEquals(List.of("$meta", "$partitions", "$ranges", "$schemas", "$topics"), cache.read(c -> {
                    try (var s = c.createStatement(); var rows = s.executeQuery("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE '$%' AND name NOT LIKE '$t_' ORDER BY name")) {
                        var result = new ArrayList<String>(); while (rows.next()) result.add(rows.getString(1)); return result;
                    }
                }));
            }
        }
    }

    /** PLAN §6.6: "a primitive key becomes a single key column"; collisions get numeric suffixes;
     * "Topics with several record types ... only get the metadata and JSON columns".
     * Schema-less inference is phase 2 (0015b), so those topics keep the metadata/JSON view here.
     */
    @Test void primitiveKeysCollideSafelyAndSchemaLessOrMultipleRecordTypesKeepJsonViews() throws Exception {
        String primitive = topic("typed-primitive"), plain = topic("typed-plain"), mixed = topic("typed-mixed");
        Schema value = schema("""
                {"type":"record","name":"PrimitiveValue","fields":[{"name":"key","type":"string"},{"name":"key_1","type":"int"}]}
                """);
        try (var registry = new CachedSchemaRegistryClient(TestStack.REGISTRY, 10);
             var cache = CacheContext.acquire(config()); var execution = execution()) {
            registry.updateCompatibility(mixed + "-value", "NONE");
            TestStack.sendAvroKeyed(primitive, "a", record(value, "key", "payload", "key_1", 5), null, TestStack.SchemaIdIn.PREFIX);
            TestStack.sendJson(plain, "key", "{\"data\":1}", null);
            TestStack.sendAvroKeyed(mixed, "a", record(value, "key", "one", "key_1", 1), null, TestStack.SchemaIdIn.PREFIX);
            Schema other = schema("{\"type\":\"record\",\"name\":\"OtherValue\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}");
            TestStack.sendAvroKeyed(mixed, "b", record(other, "n", 2), null, TestStack.SchemaIdIn.PREFIX);
            for (String topic : List.of(primitive, plain, mixed)) cache.sync(cache.plan(topic, execution), execution);
            var expected = new ArrayList<>(METADATA); expected.addAll(List.of("key", "key_1", "key_1_1"));
            assertEquals(expected, cache.read(c -> columns(c, primitive)));
            assertEquals(METADATA, cache.read(c -> columns(c, plain)));
            assertEquals(METADATA, cache.read(c -> columns(c, mixed)));
            cache.read(c -> {
                try (var s = c.createStatement(); var rows = s.executeQuery("SELECT key, key_1, key_1_1 FROM " + q(primitive))) {
                    assertTrue(rows.next()); assertEquals("a", rows.getString(1));
                    assertEquals("payload", rows.getString(2)); assertEquals(5, rows.getInt(3));
                }
                for (String topic : List.of(plain, mixed)) {
                    try (var s = c.createStatement(); var rows = s.executeQuery("SELECT count(*), min(json_valid(_value)) FROM " + q(topic))) {
                        assertTrue(rows.next()); assertEquals(topic.equals(plain) ? 1 : 2, rows.getInt(1)); assertEquals(1, rows.getInt(2));
                    }
                }
                return null;
            });
        }
    }

    /** PLAN §6.6: "DROP + CREATE, inside the sync transaction"; §10.1: "each query has its own
     * read transaction" and "a statement prepared outside a transaction runs against whatever schema is current".
     */
    @Test void activeReaderKeepsItsRowsAndColumnsAcrossViewRegeneration() throws Exception {
        String topic = topic("typed-snapshot");
        Schema first = schema("{\"type\":\"record\",\"name\":\"SnapshotValue\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}");
        Schema next = schema("{\"type\":\"record\",\"name\":\"SnapshotValue\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"},{\"name\":\"added\",\"type\":\"string\",\"default\":\"\"}]}");
        TestStack.sendAvro(topic, "a", record(first, "n", 1), null, TestStack.SchemaIdIn.PREFIX);
        TestStack.sendAvro(topic, "b", record(first, "n", 2), null, TestStack.SchemaIdIn.PREFIX);
        try (var reader = CacheContext.acquire(config()); var writer = CacheContext.acquire(config()); var execution = execution()) {
            reader.sync(reader.plan(topic, execution), execution);
            reader.read(c -> {
                try (var s = c.createStatement(); var rows = s.executeQuery("SELECT * FROM " + q(topic) + " ORDER BY _offset")) {
                    assertTrue(rows.next()); assertEquals(1, rows.getInt("n"));
                    int count = rows.getMetaData().getColumnCount();
                    TestStack.sendAvro(topic, "c", record(next, "n", 3, "added", "new"), null, TestStack.SchemaIdIn.PREFIX);
                    writer.sync(writer.plan(topic, execution), execution);
                    assertEquals(count, rows.getMetaData().getColumnCount());
                    assertThrows(SQLException.class, () -> rows.findColumn("added"));
                    assertTrue(rows.next()); assertEquals(2, rows.getInt("n")); assertFalse(rows.next());
                }
                assertFalse(columns(c, topic).contains("added"), "the whole read transaction keeps its schema snapshot");
                return null;
            });
            assertTrue(reader.read(c -> columns(c, topic)).contains("added"));
            reader.read(c -> {
                try (var s = c.createStatement(); var rows = s.executeQuery("SELECT n, added FROM " + q(topic) + " ORDER BY _offset")) {
                    assertTrue(rows.next()); assertNull(rows.getString(2));
                    assertTrue(rows.next()); assertNull(rows.getString(2));
                    assertTrue(rows.next()); assertEquals(3, rows.getInt(1)); assertEquals("new", rows.getString(2)); assertFalse(rows.next());
                }
                return null;
            });
        }
    }

    private DriverConfig config() throws Exception {
        Properties p = new Properties(); p.setProperty("cache.path", dir.resolve("views.db").toString());
        p.setProperty("schema.registry.url", TestStack.REGISTRY); p.setProperty("sync.batch.records", "1");
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }
    private static Execution execution() { return new Execution(Duration.ofSeconds(30)); }
    private static String topic(String base) { String topic = TestStack.uniqueTopic(base); TestStack.createTopic(topic, 1, Map.of()); return topic; }
    private static String q(String name) { return StorageDdl.quote(name); }
    private static Schema schema(String json) { return new Schema.Parser().parse(json); }
    private static GenericData.Record record(Schema schema, Object... values) {
        var record = new GenericData.Record(schema);
        for (int i = 0; i < values.length; i += 2) record.put((String) values[i], values[i + 1]);
        return record;
    }
    private static GenericData.Record value(Schema schema, String amount) {
        var decimal = schema.getField("amount").schema();
        var result = record(schema, "_offset", 41, "_offset_1", "payload",
                "amount", new Conversions.DecimalConversion().toBytes(new BigDecimal(amount), decimal, (LogicalTypes.Decimal) decimal.getLogicalType()),
                "address", record(schema.getField("address").schema(), "city", "London"),
                "tags", List.of("one", "two"), "labels", Map.of("a", 1));
        if (schema.getField("removed") != null) { result.put("removed", "old"); result.put("changed", 7); result.put("stable", 2); }
        else { result.put("added", "new"); result.put("changed", "N/A"); result.put("stable", 2.5); }
        return result;
    }
    private static List<String> columns(Connection c, String topic) throws SQLException {
        try (var s = c.createStatement(); var rows = s.executeQuery("SELECT * FROM " + q(topic) + " LIMIT 0")) {
            var metadata = rows.getMetaData(); var columns = new ArrayList<String>();
            for (int i = 1; i <= metadata.getColumnCount(); i++) columns.add(metadata.getColumnLabel(i)); return columns;
        }
    }
    private static String tableSql(Connection c, long id) throws SQLException {
        try (var p = c.prepareStatement("SELECT sql FROM sqlite_schema WHERE name=?")) {
            p.setString(1, "$t" + id); try (var rows = p.executeQuery()) { assertTrue(rows.next()); return rows.getString(1); }
        }
    }
}
