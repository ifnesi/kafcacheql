package io.github.ifnesi.jdbckafka.testkit;

import static io.github.ifnesi.jdbckafka.testkit.TestStack.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Smoke test per helper of {@link TestStack}. Needs the Docker Compose stack. */
class TestStackIT {

    private static final Schema ORDER = new Schema.Parser().parse("""
            {"type": "record", "name": "Order", "namespace": "test",
             "fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]}
            """);

    @BeforeAll
    static void stack() {
        TestStack.requireUp();
    }

    @Test
    void uniqueTopicNamesDiffer() {
        assertNotEquals(TestStack.uniqueTopic("t"), TestStack.uniqueTopic("t"));
        assertTrue(TestStack.uniqueTopic("orders").startsWith("orders-"));
    }

    @Test
    void createTopicWithConfigs() {
        String topic = TestStack.uniqueTopic("configs");
        Map<String, String> configs = Map.of(
                "cleanup.policy", "compact",
                "delete.retention.ms", "1000",
                "message.timestamp.type", "LogAppendTime",
                "segment.ms", "1000",
                "min.cleanable.dirty.ratio", "0.01");
        TestStack.createTopic(topic, 3, configs);
        assertEquals(3, TestStack.partitionCount(topic));
        assertEquals(configs, TestStack.topicConfigs(topic));
    }

    @Test
    void avroWithPrefixSchemaId() {
        String topic = TestStack.uniqueTopic("avro-prefix");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendAvro(topic, "k1", order("o1", 9.5), null, TestStack.SchemaIdIn.PREFIX);

        ConsumerRecord<byte[], byte[]> r = only(TestStack.readAll(topic, true));
        assertEquals("k1", utf8(r.key()));
        assertEquals(0, r.value()[0], "magic byte");
        assertTrue(ByteBuffer.wrap(r.value(), 1, 4).getInt() > 0, "schema ID in the prefix");
        assertArrayEquals(plainAvro(order("o1", 9.5)), java.util.Arrays.copyOfRange(r.value(), 5, r.value().length));
        assertNull(r.headers().lastHeader("__value_schema_id"));
    }

    @Test
    void avroWithHeaderSchemaId() {
        String topic = TestStack.uniqueTopic("avro-header");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendAvro(topic, "k1", order("o1", 9.5), null, TestStack.SchemaIdIn.HEADER);

        ConsumerRecord<byte[], byte[]> r = only(TestStack.readAll(topic, true));
        Header header = r.headers().lastHeader("__value_schema_id");
        assertNotNull(header, "schema reference in the header");
        assertEquals(17, header.value().length, "a version byte plus a 16-byte GUID");
        assertArrayEquals(plainAvro(order("o1", 9.5)), r.value(), "payload without a prefix");
    }

    @Test
    void avroKeyAndValue() {
        String topic = TestStack.uniqueTopic("avro-keyed");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendAvroKeyed(topic, "key-as-avro-string", order("o1", 1), null, TestStack.SchemaIdIn.HEADER);

        ConsumerRecord<byte[], byte[]> r = only(TestStack.readAll(topic, true));
        assertNotNull(r.headers().lastHeader("__key_schema_id"));
        assertNotNull(r.headers().lastHeader("__value_schema_id"));
    }

    @Test
    void jsonStringBytesAndTombstoneWithChosenKeysAndTimestamps() {
        String topic = TestStack.uniqueTopic("plain");
        TestStack.createTopic(topic, 1, Map.of());
        byte[] raw = {(byte) 0xff, 0, 1, 2};
        TestStack.sendJson(topic, "j", "{\"a\":1}", 1_000L);
        TestStack.sendString(topic, "s", "hello", 2_000L);
        TestStack.sendBytes(topic, raw, raw, 3_000L);
        TestStack.sendTombstone(topic, "j", 4_000L);

        List<ConsumerRecord<byte[], byte[]>> rs = TestStack.readAll(topic, true);
        assertEquals(4, rs.size());
        assertEquals("{\"a\":1}", utf8(rs.get(0).value()));
        assertEquals(1_000L, rs.get(0).timestamp());
        assertEquals("hello", utf8(rs.get(1).value()));
        assertEquals("s", utf8(rs.get(1).key()));
        assertArrayEquals(raw, rs.get(2).key());
        assertArrayEquals(raw, rs.get(2).value());
        assertEquals("j", utf8(rs.get(3).key()));
        assertNull(rs.get(3).value(), "tombstone");
        assertEquals(4_000L, rs.get(3).timestamp());
    }

    @Test
    void transactionsCommitAndAbort() {
        String topic = TestStack.uniqueTopic("tx");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.inTransaction(true, p -> p.send(new ProducerRecord<>(topic, utf8("k"), utf8("committed"))));
        TestStack.inTransaction(false, p -> p.send(new ProducerRecord<>(topic, utf8("k"), utf8("aborted"))));

        List<String> committed = TestStack.readAll(topic, true).stream().map(r -> utf8(r.value())).toList();
        List<String> all = TestStack.readAll(topic, false).stream().map(r -> utf8(r.value())).toList();
        assertEquals(List.of("committed"), committed);
        assertEquals(List.of("committed", "aborted"), all);
    }

    @Test
    void deleteAndRecreateGivesANewTopicId() {
        String topic = TestStack.uniqueTopic("recreate");
        TestStack.createTopic(topic, 1, Map.of());
        Uuid before = TestStack.topicId(topic);
        TestStack.recreateTopic(topic, 1, Map.of());
        assertNotEquals(before, TestStack.topicId(topic));
        TestStack.deleteTopic(topic);
        assertTrue(!TestStack.topicExists(topic));
    }

    @Test
    void addPartitions() {
        String topic = TestStack.uniqueTopic("grow");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.addPartitions(topic, 3);
        assertEquals(3, TestStack.partitionCount(topic));
    }

    @Test
    void sendToAChosenPartition() {
        String topic = TestStack.uniqueTopic("partitioned");
        TestStack.createTopic(topic, 3, Map.of());
        assertEquals(2, TestStack.send(topic, 2, null, utf8("k"), utf8("v"), null).partition());
    }

    private static GenericRecord order(String id, double amount) {
        GenericRecord r = new GenericData.Record(ORDER);
        r.put("id", id);
        r.put("amount", amount);
        return r;
    }

    private static byte[] plainAvro(GenericRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static <T> T only(List<T> list) {
        assertEquals(1, list.size(), "records");
        return list.get(0);
    }
}
