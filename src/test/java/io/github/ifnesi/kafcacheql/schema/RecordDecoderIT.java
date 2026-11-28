package io.github.ifnesi.kafcacheql.schema;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.NonRecordContainer;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Whole producer -> broker -> production sync worker/writer -> SQLite flows. JDBC statements are a later item. */
class RecordDecoderIT {
    private static final BigDecimal DECIMAL = new BigDecimal("-12345678901234567890.123400");
    private static final String DECIMAL_JSON = "\"-12345678901234567890.123400\"";

    @TempDir
    Path dir;

    @BeforeAll
    static void stack() {
        TestStack.requireUp();
    }

    /** PLAN §7.1 L444–455: headers precede prefixes, auto detection works per record, key/value are
     * valid JSON (or SQL NULL), and a failed decode does not stop later records. §7.2 L463–473: every
     * Avro row, nested unions and logical types, including the fixed precision decided in 0005b L28–32.
     * Maintainer's whole-flow rule replaces 0005b's one-test-per-row acceptance.
     */
    @Test
    void mixedTopicDecodesAllAvroTypesAndContinuesPastBadRecords() throws Exception {
        String topic = topic("decoding-auto-flow");
        Schema schema = new Schema.Parser().parse(resource("all-types.avsc"));
        GenericRecord body = allTypes(schema);
        String json = resource("all-types.json").strip();
        var headers = new RecordHeaders();
        byte[] key = TestStack.serializeAvro(topic, headers, body, TestStack.SchemaIdIn.PREFIX, true);
        byte[] value = TestStack.serializeAvro(topic, headers, body, TestStack.SchemaIdIn.PREFIX, false);
        SchemaMetadata keySchema = metadata(topic, true);
        SchemaMetadata valueSchema = metadata(topic, false);
        String keyId = Integer.toString(keySchema.getId());
        String valueId = Integer.toString(valueSchema.getId());
        List<Expected> expected = new ArrayList<>();

        TestStack.send(topic, 0, null, key, value, headers);
        expected.add(new Expected(json, keyId, json, valueId, null));
        TestStack.sendJson(topic, "\"json-key\"", " {\"n\":9007199254740993,\"nested\":[true,null,\"é\"]} ", null);
        expected.add(plain("\"json-key\"", " {\"n\":9007199254740993,\"nested\":[true,null,\"é\"]} "));
        TestStack.sendString(topic, "key", "hello\n\"é\\", null);
        expected.add(plain("\"key\"", "\"hello\\n\\\"é\\\\\""));
        // These are UTF-8 strings, not single valid JSON values, even if a permissive parser accepts them.
        for (String text : List.of("{} {}", "true false", "01", "NaN", "", "{bad}")) {
            TestStack.sendString(topic, "key", text, null);
            expected.add(plain("\"key\"", '"' + text + '"'));
        }
        TestStack.sendBytes(topic, new byte[] {(byte) 255}, new byte[] {(byte) 0xc0, (byte) 0xaf}, null);
        expected.add(plain("\"/w==\"", "\"wK8=\""));
        TestStack.sendTombstone(topic, "delete-key", null);
        expected.add(plain("\"delete-key\"", null));
        TestStack.sendJson(topic, null, "null", null);
        expected.add(plain(null, "null"));

        byte[] corruptKey = Arrays.copyOf(key, 5);
        byte[] corruptValue = Arrays.copyOf(value, 5);
        TestStack.send(topic, 0, null, corruptKey, value, null);
        expected.add(new Expected(base64(corruptKey), keyId, json, valueId, "key"));
        TestStack.send(topic, 0, null, key, corruptValue, null);
        expected.add(new Expected(json, keyId, base64(corruptValue), valueId, "value"));
        byte[] unknown = unknownSchema();
        TestStack.sendBytes(topic, unknown, unknown, null);
        // §7.1's "and the registry knows that schema": auto falls through; explicit avro is below.
        expected.add(plain(base64(unknown), base64(unknown)));
        for (byte[] bad : List.of(new byte[0], new byte[] {1, 2}, new byte[] {2, 0, 0, 0, 0})) {
            var malformed = new RecordHeaders().add("__key_schema_id", bad).add("__value_schema_id", bad);
            TestStack.send(topic, 0, null, key, value, malformed);
            expected.add(new Expected(base64(key), null, base64(value), null, "key,value"));
        }
        TestStack.sendAvroKeyed(topic, body, body, null, TestStack.SchemaIdIn.HEADER);
        expected.add(new Expected(json, keySchema.getGuid(), json, valueSchema.getGuid(), null));

        // PLAN §7.1 / 0015a: JSON Schema now decodes; malformed JSON is still an error row and sync continues.
        int jsonId;
        try (var registry = new io.confluent.kafka.schemaregistry.client.rest.RestService(TestStack.REGISTRY)) {
            jsonId = registry.registerSchema("{\"type\":\"object\"}", "JSON", List.of(),
                    topic + "-json", false).getId();
        }
        byte[] framedJson = ByteBuffer.allocate(7).put((byte) 0).putInt(jsonId).put(TestStack.utf8("{}")).array();
        TestStack.sendBytes(topic, TestStack.utf8("json-sr"), framedJson, null);
        expected.add(new Expected("\"json-sr\"", null, "{}", Integer.toString(jsonId), null));
        byte[] corruptJson = Arrays.copyOf(framedJson, 6);
        TestStack.sendBytes(topic, null, corruptJson, null);
        expected.add(new Expected(null, null, base64(corruptJson), Integer.toString(jsonId), "value"));
        TestStack.sendString(topic, "after-json", "still here", null);
        expected.add(plain("\"after-json\"", "\"still here\""));

        verifyFlow(topic, Format.AUTO, Format.AUTO, expected);
        try (var cache = CacheStore.open(dir.resolve(topic + ".db"), Optional.empty()); var reader = cache.newReader()) {
            var jsonSchema = SchemaStore.find(reader, Integer.toString(jsonId)).orElseThrow();
            assertEquals("JSON", jsonSchema.type());
            assertEquals("{\"type\":\"object\"}", jsonSchema.schemaText());
        }
    }

    /** PLAN §7.1 L443–455: formats are set separately for key/value; strings and bytes remain JSON,
     * failures become base64 JSON with _decode_error and later records still decode. 0005b L17–19:
     * unknown schema ID is a row error in explicit avro. §7.2 L468–469: exact decimals and logical types.
     */
    @Test
    void configuredFormatsDecodeWholeTopicsAndRecoverFromInvalidRecords() throws Exception {
        String avroTopic = topic("decoding-avro-flow");
        Schema decimal = new Schema.Parser().parse("""
                {"type":"bytes","logicalType":"decimal","precision":30,"scale":6}
                """);
        var datum = new NonRecordContainer(decimal, new Conversions.DecimalConversion()
                .toBytes(DECIMAL, decimal, decimal.getLogicalType()));
        var headers = new RecordHeaders();
        byte[] key = TestStack.serializeAvro(avroTopic, headers, "key", TestStack.SchemaIdIn.PREFIX, true);
        byte[] value = TestStack.serializeAvro(avroTopic, headers, datum, TestStack.SchemaIdIn.PREFIX, false);
        SchemaMetadata keySchema = metadata(avroTopic, true);
        SchemaMetadata valueSchema = metadata(avroTopic, false);
        String keyId = Integer.toString(keySchema.getId());
        String valueId = Integer.toString(valueSchema.getId());
        byte[] unknown = unknownSchema();
        byte[] corruptKey = Arrays.copyOf(key, 5);
        byte[] corruptValue = Arrays.copyOf(value, 5);
        TestStack.send(avroTopic, 0, null, key, value, headers);
        TestStack.sendBytes(avroTopic, unknown, unknown, null);
        TestStack.send(avroTopic, 0, null, corruptKey, value, null);
        TestStack.send(avroTopic, 0, null, key, corruptValue, null);
        var malformed = new RecordHeaders().add("__key_schema_id", new byte[] {1, 2})
                .add("__value_schema_id", new byte[] {1, 2});
        TestStack.send(avroTopic, 0, null, key, value, malformed);
        TestStack.sendAvroKeyed(avroTopic, null, datum, null, TestStack.SchemaIdIn.HEADER);
        TestStack.sendAvroKeyed(avroTopic, "key", null, null, TestStack.SchemaIdIn.PREFIX);
        TestStack.sendAvroKeyed(avroTopic, "key", datum, null, TestStack.SchemaIdIn.HEADER);
        verifyFlow(avroTopic, Format.AVRO, Format.AVRO, List.of(
                new Expected("\"key\"", keyId, DECIMAL_JSON, valueId, null),
                new Expected(base64(unknown), "2147483647", base64(unknown), "2147483647", "key,value"),
                new Expected(base64(corruptKey), keyId, DECIMAL_JSON, valueId, "key",
                        "key: Avro, schema " + keyId + ": the value ends before the record does"),
                new Expected("\"key\"", keyId, base64(corruptValue), valueId, "value"),
                new Expected(base64(key), null, base64(value), null, "key,value"),
                new Expected(null, null, DECIMAL_JSON, valueSchema.getGuid(), null),
                new Expected("\"key\"", keyId, null, null, null),
                new Expected("\"key\"", keySchema.getGuid(), DECIMAL_JSON, valueSchema.getGuid(), null)));

        String jsonTopic = topic("decoding-json-flow");
        TestStack.sendJson(jsonTopic, "key", "{\"n\":9007199254740993}", null);
        TestStack.sendString(jsonTopic, "key", "NaN", null);
        TestStack.sendBytes(jsonTopic, TestStack.utf8("key"), new byte[] {(byte) 255}, null);
        TestStack.sendJson(jsonTopic, "key", "[true,null,\"é\"]", null);
        verifyFlow(jsonTopic, Format.STRING, Format.JSON, List.of(
                plain("\"key\"", "{\"n\":9007199254740993}"),
                new Expected("\"key\"", null, "\"TmFO\"", null, "value"),
                new Expected("\"key\"", null, "\"/w==\"", null, "value"),
                plain("\"key\"", "[true,null,\"é\"]")));

        String stringTopic = topic("decoding-string-flow");
        TestStack.sendString(stringTopic, "k", "{}", null);
        TestStack.sendBytes(stringTopic, TestStack.utf8("k"), new byte[] {(byte) 255}, null);
        TestStack.sendString(stringTopic, "k", "later\n", null);
        verifyFlow(stringTopic, Format.BYTES, Format.STRING, List.of(
                plain("\"aw==\"", "\"{}\""),
                new Expected("\"aw==\"", null, "\"/w==\"", null, "value"),
                plain("\"aw==\"", "\"later\\n\"")));

        String bytesTopic = topic("decoding-bytes-flow");
        TestStack.sendJson(bytesTopic, "123", "{}", null);
        TestStack.sendBytes(bytesTopic, TestStack.utf8("null"), new byte[] {0, (byte) 255, 1}, null);
        TestStack.sendJson(bytesTopic, "true", "later", null);
        verifyFlow(bytesTopic, Format.JSON, Format.BYTES, List.of(
                plain("123", "\"e30=\""), plain("null", "\"AP8B\""), plain("true", "\"bGF0ZXI=\"")));
    }

    private static GenericRecord allTypes(Schema schema) {
        Schema child = schema.getField("byName").schema().getValueType();
        Schema alternative = schema.getField("choices").schema().getElementType().getTypes().get(1);
        Schema decimal = schema.getField("decimal").schema();
        Schema money = schema.getField("money").schema();
        var conversion = new Conversions.DecimalConversion();
        return new GenericRecordBuilder(schema)
                .set("booleans", List.of(true, false))
                .set("ints", List.of(Integer.MIN_VALUE, Integer.MAX_VALUE))
                .set("longs", List.of(Long.MIN_VALUE, Long.MAX_VALUE, 9007199254740993L))
                .set("floats", List.of(1.25f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY))
                .set("doubles", List.of(-2.5d, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY))
                .set("text", "a\n\"\\é")
                .set("state", new GenericData.EnumSymbol(schema.getField("state").schema(), "OPEN"))
                .set("uuid", "123e4567-e89b-12d3-a456-426614174000")
                .set("bytes", ByteBuffer.wrap(new byte[] {0, (byte) 255, 1}))
                .set("fixed", new GenericData.Fixed(schema.getField("fixed").schema(), new byte[] {0, (byte) 255, 1}))
                .set("decimal", conversion.toBytes(DECIMAL, decimal, decimal.getLogicalType()))
                .set("money", conversion.toFixed(DECIMAL, money, money.getLogicalType()))
                .set("dates", List.of(20781, -1))
                .set("timeMillis", List.of(50709123, 0))
                .set("timeMicros", List.of(50709123456L, 0L))
                .set("timestampMillis", List.of(1795529109123L, -1L, 0L))
                .set("timestampMicros", List.of(1795529109123456L, -1L, 0L))
                .set("localMillis", List.of(1795529109000L, -1L, 0L))
                .set("localMicros", List.of(1795529109000000L, -1L, 0L))
                .set("byName", Map.of("a\"b", new GenericRecordBuilder(child).set("n", 7L).build()))
                .set("arrays", List.of(List.of(1, 2), List.of()))
                .set("nullable", Arrays.asList(20781, null))
                .set("absent", null).set("present", "yes")
                .set("choices", List.of("text", new GenericRecordBuilder(alternative).set("value", 9).build(),
                        new GenericRecordBuilder(alternative).set("value", Map.of("flag", true)).build(),
                        new GenericRecordBuilder(alternative).set("value", Map.of("label", "yes")).build()))
                .build();
    }

    private record Expected(String key, String keyRef, String value, String valueRef, String errors, String detail) {
        Expected(String key, String keyRef, String value, String valueRef, String errors) {
            this(key, keyRef, value, valueRef, errors, null);
        }
    }

    private static Expected plain(String key, String value) {
        return new Expected(key, null, value, null, null);
    }

    /** Only the deliberately unknown reference is handcrafted; all valid wire data uses Confluent. */
    private static byte[] unknownSchema() {
        return ByteBuffer.allocate(6).put((byte) 0).putInt(Integer.MAX_VALUE).put((byte) 0).array();
    }

    private static String base64(byte[] bytes) {
        return '"' + Base64.getEncoder().encodeToString(bytes) + '"';
    }

    private static SchemaMetadata metadata(String topic, boolean key) throws Exception {
        try (var registry = new CachedSchemaRegistryClient(TestStack.REGISTRY, 10)) {
            return registry.getSchemaMetadata(topic + (key ? "-key" : "-value"), -1);
        }
    }

    private static String resource(String name) throws IOException {
        try (var input = Objects.requireNonNull(RecordDecoderIT.class.getResourceAsStream("/decoding/" + name), name)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String topic(String base) {
        String topic = TestStack.uniqueTopic(base);
        TestStack.createTopic(topic, 1, Map.of());
        return topic;
    }

    private void verifyFlow(String topic, Format keyFormat, Format valueFormat, List<Expected> expected) throws Exception {
        // TestStack.readAll owns a real byte-array consumer and reads to the captured end offset.
        var records = TestStack.readAll(topic, true);
        assertEquals(expected.size(), records.size(), topic);
        var props = new java.util.Properties();
        props.setProperty("schema.registry.url", TestStack.REGISTRY);
        props.setProperty("topic." + topic + ".key.format", keyFormat.value());
        props.setProperty("topic." + topic + ".value.format", valueFormat.value());
        props.setProperty("cache.path", dir.resolve(topic + ".db").toString());
        DriverConfig config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, props);
        String table;
        try (var context = io.github.ifnesi.kafcacheql.sync.CacheContext.acquire(config);
             var execution = new io.github.ifnesi.kafcacheql.sync.Execution(java.time.Duration.ofSeconds(30))) {
            table = "$t" + context.sync(context.plan(topic, execution), execution);
        }
        try (CacheStore cache = CacheStore.open(dir.resolve(topic + ".db"), Optional.empty());
             Connection reader = cache.newReader()) {
            var schemaRefs = new java.util.HashSet<String>();
            try (var s = reader.createStatement(); var rs = s.executeQuery("SELECT _offset,_key_raw,_key,_key_schema_id,"
                    + "_value,_value_schema_id,_decode_error,json_valid(_key),json_valid(_value) FROM \""
                    + table + "\" ORDER BY _offset")) {
                for (int i = 0; i < expected.size(); i++) {
                    String context = topic + " record " + i;
                    assertTrue(rs.next(), context);
                    Expected row = expected.get(i);
                    for (String ref : new String[] {row.keyRef(), row.valueRef()}) {
                        if (ref != null && !ref.equals("2147483647")) schemaRefs.add(ref); // The fixture's unknown ID has no schema.
                    }
                    assertEquals(records.get(i).offset(), rs.getLong(1), context);
                    assertArrayEquals(records.get(i).key(), rs.getBytes(2), context);
                    assertEquals(row.key(), rs.getString(3), context);
                    assertEquals(row.keyRef(), rs.getString(4), context);
                    assertEquals(row.value(), rs.getString(5), context);
                    assertEquals(row.valueRef(), rs.getString(6), context);
                    assertEquals(row.key() == null ? 0 : 1, rs.getInt(8), context + " key json_valid");
                    assertEquals(row.value() == null ? 0 : 1, rs.getInt(9), context + " value json_valid");
                    String error = rs.getString(7);
                    if (row.detail() != null) assertEquals(row.detail(), error, context);
                    if (row.errors() == null) {
                        assertNull(error, context);
                    } else {
                        assertNotNull(error, context);
                        assertFalse(error.isBlank(), context);
                        for (String side : List.of("key", "value")) {
                            assertEquals(row.errors().contains(side), error.contains(side + ':'), context + ": " + error);
                        }
                    }
                }
                assertFalse(rs.next(), "no extra rows");
            }
            for (String ref : schemaRefs) assertTrue(SchemaStore.find(reader, ref).isPresent(), "persisted schema " + ref);
            // These fixtures are non-transactional, single-partition topics: produced offsets are [0,n).
            // PLAN §10 L588: the committed range covers all processed records in the same transaction.
            try (var s = reader.createStatement(); var ranges = s.executeQuery("SELECT start_offset,end_offset FROM \"$ranges\"")) {
                assertTrue(ranges.next());
                assertEquals(0, ranges.getLong(1));
                assertEquals(expected.size(), ranges.getLong(2));
                assertFalse(ranges.next());
            }
            try (var s = reader.createStatement(); var rs = s.executeQuery("SELECT decoder_version FROM \"$topics\"")) {
                assertTrue(rs.next());
                assertEquals(AvroJsonConverter.VERSION, rs.getInt(1));
            }
        }
    }
}
