package io.github.ifnesi.jdbckafka.kafka;

import static io.github.ifnesi.jdbckafka.testkit.TestStack.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.jdbckafka.config.DriverConfig;
import io.github.ifnesi.jdbckafka.config.Mode;
import io.github.ifnesi.jdbckafka.testkit.LogCapture;
import io.github.ifnesi.jdbckafka.testkit.TestStack;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;
import javax.management.ObjectName;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Needs the Docker Compose stack. */
class KafkaClientsIT {

    @BeforeAll
    static void stack() {
        TestStack.requireUp();
    }

    /** Each test its own client.id base, so tests in one JVM don't share clients by accident. */
    private static DriverConfig config(String... kv) throws SQLException {
        Properties p = new Properties();
        p.setProperty("client.id", "it-" + UUID.randomUUID().toString().substring(0, 8));
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return DriverConfig.load("jdbc:kafka://" + TestStack.BOOTSTRAP, p);
    }

    private static boolean mbean(String type, String id) throws Exception {
        return !ManagementFactory.getPlatformMBeanServer()
                .queryNames(new ObjectName("kafka." + type + ":type=app-info,id=" + id), null).isEmpty();
    }

    // ------------------------------------------------------------------ lifecycle

    @Test
    void connectionsToOneCacheShareTheClientsAndTheLastCloses() throws Exception {
        DriverConfig config = config();
        KafkaClients a = KafkaClients.acquire(config);
        KafkaClients b = KafkaClients.acquire(config);
        assertSame(a, b);
        assertEquals(config.clientId(), a.clientId());
        assertTrue(mbean("consumer", a.clientId()));
        assertTrue(mbean("admin.client", a.clientId() + "-admin"));

        a.release();
        assertTrue(mbean("consumer", b.clientId()), "still open for the second connection");
        b.release();
        assertFalse(mbean("consumer", b.clientId()), "closed with the last");
        assertFalse(mbean("admin.client", b.clientId() + "-admin"));

        KafkaClients c = KafkaClients.acquire(config);
        assertNotSame(a, c, "created again after closing");
        c.release();
    }

    @Test
    void twoCachesGetDistinctClientIdsAndNoJmxWarning() throws Exception {
        DriverConfig one = config("cache.path", "/tmp/jdbc-kafka-it-one.db");
        Properties p = new Properties();
        p.setProperty("client.id", one.clientId());
        p.setProperty("cache.path", "/tmp/jdbc-kafka-it-two.db");
        DriverConfig two = DriverConfig.load("jdbc:kafka://" + TestStack.BOOTSTRAP, p);

        LogCapture.start();
        KafkaClients a = KafkaClients.acquire(one);
        KafkaClients b = KafkaClients.acquire(two);
        String logs = String.join("\n", LogCapture.stop());
        try {
            assertNotSame(a, b);
            assertEquals(one.clientId(), a.clientId());
            assertEquals(one.clientId() + "-2", b.clientId());
            assertTrue(mbean("consumer", b.clientId()));
            assertTrue(mbean("admin.client", b.clientId() + "-admin"));
            assertFalse(logs.contains("Error registering AppInfo mbean"), logs);
        } finally {
            a.release();
            b.release();
        }
    }

    @Test
    void theClusterIdIsTheBrokers() throws Exception {
        KafkaClients c = KafkaClients.acquire(config());
        try {
            assertEquals(TestStack.admin().describeCluster().clusterId().get(), c.clusterId());
        } finally {
            c.release();
        }
    }

    @Test
    void anUnreachableClusterFailsWithoutLeakingClients() throws Exception {
        int before = KafkaClients.openCount();
        Properties p = new Properties();
        p.setProperty("default.api.timeout.ms", "3000");
        p.setProperty("request.timeout.ms", "2000");
        SQLException e = assertThrows(SQLException.class,
                () -> KafkaClients.acquire(DriverConfig.load("jdbc:kafka://localhost:1", p)));
        assertTrue(e.getMessage().startsWith("Reading the cluster ID from localhost:1 failed"), e.getMessage());
        assertEquals(before, KafkaClients.openCount());
    }

    // ------------------------------------------------------------------ topic metadata

    @Test
    void appendAndCompactedTopics() throws Exception {
        String append = TestStack.uniqueTopic("meta-append");
        String compacted = TestStack.uniqueTopic("meta-compacted");
        String both = TestStack.uniqueTopic("meta-both");
        TestStack.createTopic(append, 2, Map.of());
        TestStack.createTopic(compacted, 3, Map.of("cleanup.policy", "compact", "delete.retention.ms", "1000",
                "message.timestamp.type", "LogAppendTime"));
        TestStack.createTopic(both, 1, Map.of("cleanup.policy", "compact,delete"));

        DriverConfig config = config("topic." + both + ".mode", "append");
        KafkaClients c = KafkaClients.acquire(config);
        try {
            assertTrue(c.listTopics().containsAll(java.util.List.of(append, compacted, both)));

            TopicInfo a = c.describeTopic(append, config).orElseThrow();
            assertEquals(Mode.APPEND, a.mode());
            assertEquals(2, a.partitions());
            assertEquals(TestStack.topicId(append), a.topicId());
            assertEquals(TimestampType.CREATE_TIME, a.timestampType());
            assertEquals(TopicInfo.DEFAULT_DELETE_RETENTION_MS, a.deleteRetentionMs());
            assertTrue(a.configsFromBroker());

            TopicInfo k = c.describeTopic(compacted, config).orElseThrow();
            assertEquals(Mode.COMPACTED, k.mode());
            assertEquals(3, k.partitions());
            assertEquals(1000, k.deleteRetentionMs());
            assertEquals(TimestampType.LOG_APPEND_TIME, k.timestampType());

            assertEquals(Mode.APPEND, c.describeTopic(both, config).orElseThrow().mode(), "the override wins");
            assertEquals(Optional.empty(), c.describeTopic(TestStack.uniqueTopic("missing"), config));
        } finally {
            c.release();
        }
    }

    // ------------------------------------------------------------------ offsets

    @Test
    void offsetsOfAnEmptyTopicAndAfterAppends() throws Exception {
        String topic = TestStack.uniqueTopic("offsets");
        TestStack.createTopic(topic, 2, Map.of());
        KafkaClients c = KafkaClients.acquire(config());
        try {
            assertEquals(Map.of(0, 0L, 1, 0L), c.logStartOffsets(topic, 2));
            assertEquals(Map.of(0, 0L, 1, 0L), c.endOffsets(topic, 2).offsets());

            for (int i = 0; i < 3; i++) {
                TestStack.send(topic, 0, null, utf8("k"), utf8("v" + i), null);
            }
            TestStack.send(topic, 1, null, utf8("k"), utf8("w"), null);
            long before = System.currentTimeMillis();
            EndOffsets end = c.endOffsets(topic, 2);
            long after = System.currentTimeMillis();
            assertEquals(Map.of(0, 3L, 1, 1L), end.offsets());
            assertTrue(end.capturedAtMs() >= before && end.capturedAtMs() <= after, "captured just before the request");
            assertEquals(Map.of(0, 0L, 1, 0L), c.logStartOffsets(topic, 2));
        } finally {
            c.release();
        }
    }

    @Test
    void offsetsForTimesBeforeBetweenAndAfterRecords() throws Exception {
        String topic = TestStack.uniqueTopic("times");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "k", "a", 1_000L);
        TestStack.sendString(topic, "k", "b", 2_000L);
        TestStack.sendString(topic, "k", "c", 3_000L);
        KafkaClients c = KafkaClients.acquire(config());
        try {
            assertEquals(Map.of(0, OptionalLong.of(0)), c.offsetsForTimes(topic, 1, 500L), "before");
            assertEquals(Map.of(0, OptionalLong.of(1)), c.offsetsForTimes(topic, 1, 1_500L), "between");
            assertEquals(Map.of(0, OptionalLong.of(2)), c.offsetsForTimes(topic, 1, 3_000L), "at");
            assertEquals(Map.of(0, OptionalLong.empty()), c.offsetsForTimes(topic, 1, 3_001L), "after");
        } finally {
            c.release();
        }
    }

    @Test
    void anOpenTransactionHoldsTheEndOffsetBack() throws Exception {
        String topic = TestStack.uniqueTopic("lso");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "k", "committed", null);
        KafkaClients c = KafkaClients.acquire(config());
        try (KafkaProducer<byte[], byte[]> tx = TestStack.transactionalProducer()) {
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, utf8("k"), utf8("open-1")));
            tx.send(new ProducerRecord<>(topic, utf8("k"), utf8("open-2")));
            tx.flush();
            assertEquals(Map.of(0, 1L), c.endOffsets(topic, 1).offsets(), "the last stable offset");
            tx.commitTransaction();
            assertEquals(Map.of(0, 4L), c.endOffsets(topic, 1).offsets(), "two records and the commit marker");
        } finally {
            c.release();
        }
    }

    @Test
    void theLogStartMovesWhenRetentionDeletesASegment() throws Exception {
        String topic = TestStack.uniqueTopic("retention");
        TestStack.createTopic(topic, 1, Map.of("retention.ms", "1000", "segment.ms", "200"));
        long old = System.currentTimeMillis() - 3_600_000L;
        for (int i = 0; i < 3; i++) {
            TestStack.sendString(topic, "k", "old" + i, old);
        }
        KafkaClients c = KafkaClients.acquire(config());
        try {
            TestStack.await(() -> {
                // A newer record rolls the old segment once segment.ms has passed; retention then deletes it.
                TestStack.sendString(topic, "k", "new", null);
                try {
                    return c.logStartOffsets(topic, 1).get(0) >= 3;
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }, "retention to delete the old segment");
        } finally {
            c.release();
        }
    }
}
