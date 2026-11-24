package io.github.ifnesi.jdbckafka.kafka;

import static io.github.ifnesi.jdbckafka.testkit.TestStack.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.jdbckafka.config.DriverConfig;
import io.github.ifnesi.jdbckafka.config.Mode;
import io.github.ifnesi.jdbckafka.testkit.LogCapture;
import io.github.ifnesi.jdbckafka.testkit.TestStack;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import org.apache.kafka.clients.admin.Admin;
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
        return configFor("it-" + UUID.randomUUID().toString().substring(0, 8), TestStack.BOOTSTRAP, kv);
    }

    private static DriverConfig configFor(String clientId, String bootstrap, String... kv) throws SQLException {
        Properties p = new Properties();
        p.setProperty("client.id", clientId);
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return DriverConfig.load("jdbc:kafka://" + bootstrap, p);
    }

    private static boolean adminBean(String clientId) throws Exception {
        return ManagementFactory.getPlatformMBeanServer()
                .isRegistered(new ObjectName("kafka.admin.client:type=app-info,id=" + clientId + "-admin"));
    }

    // ------------------------------------------------------------------ lifecycle

    @Test
    void connectionsWithTheSameSettingsShareOnePairAndTheLastLeaseCloses() throws Exception {
        DriverConfig config = config();
        KafkaClients.Lease a = KafkaClients.acquire(config);
        KafkaClients.Lease b = KafkaClients.acquire(config);
        KafkaClients clients = a.clients();
        assertSame(clients, b.clients());
        assertEquals(config.clientId(), clients.clientId());
        assertTrue(adminBean(clients.clientId()));

        a.close();
        a.close(); // a second close of one lease doesn't take another's
        assertTrue(adminBean(clients.clientId()), "still open for the second lease");
        assertThrows(IllegalStateException.class, a::clients);
        b.close();
        assertFalse(adminBean(clients.clientId()), "closed with the last lease");

        try (KafkaClients.Lease c = KafkaClients.acquire(config)) {
            assertNotSame(clients, c.clients(), "opened again after closing");
        }
    }

    /** Any client setting, not only the consumer's, tells pairs apart: no pair is reused with other settings. */
    @Test
    void differentSettingsGetDifferentPairsWithDistinctClientIds() throws Exception {
        String base = "it-" + UUID.randomUUID().toString().substring(0, 8);
        DriverConfig one = configFor(base, TestStack.BOOTSTRAP, "retries", "1");
        DriverConfig two = configFor(base, TestStack.BOOTSTRAP, "retries", "9"); // an admin client setting

        LogCapture.start();
        try (KafkaClients.Lease a = KafkaClients.acquire(one); KafkaClients.Lease b = KafkaClients.acquire(two)) {
            String logs = String.join("\n", LogCapture.stop());
            assertNotSame(a.clients(), b.clients());
            assertEquals(base, a.clients().clientId());
            assertEquals(base + "-2", b.clients().clientId());
            assertTrue(adminBean(base) && adminBean(base + "-2"));
            assertFalse(logs.contains("Error registering AppInfo mbean"), logs);
        }
    }

    /**
     * A pair opened while the last one with its settings is still closing never takes the closing pair's
     * client.id. The close is held open, deterministically, by an admin client that waits inside close().
     */
    @Test
    void aClientIdIsntReusedWhileItsClientCloses() throws Exception {
        DriverConfig config = config();
        KafkaClients.Lease old = KafkaClients.acquire(config);
        KafkaClients clients = old.clients();
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        Field field = KafkaClients.class.getDeclaredField("admin");
        field.setAccessible(true);
        Admin real = (Admin) field.get(clients);
        field.set(clients, Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        closing.countDown();
                        proceed.await();
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }));
        Thread closer = new Thread(old::close);
        closer.start();
        try {
            assertTrue(closing.await(10, TimeUnit.SECONDS), "the old pair is inside close()");
            try (KafkaClients.Lease replacement = KafkaClients.acquire(config)) {
                assertNotSame(clients, replacement.clients());
                assertNotEquals(clients.clientId(), replacement.clients().clientId(), "the closing pair's ID stays reserved");
                proceed.countDown();
                closer.join(30_000);
                assertTrue(adminBean(replacement.clients().clientId()), "the replacement keeps its bean");
            }
        } finally {
            proceed.countDown();
            closer.join(30_000);
        }
    }

    /** Interrupting one caller frees that caller alone; the others get the opening's real outcome. */
    @Test
    void anInterruptedCallerGivesUpAlone() throws Exception {
        DriverConfig bad = configFor("it-" + UUID.randomUUID().toString().substring(0, 8), "localhost:1",
                "default.api.timeout.ms", "4000", "request.timeout.ms", "1000");
        int before = KafkaClients.openCount();
        LogCapture.start();
        try {
            Caller opener = new Caller(bad);
            opener.start();
            awaitWaiting(opener);
            Caller waiter = new Caller(bad);
            waiter.start();
            awaitWaiting(waiter);
            Caller interrupted = new Caller(bad);
            interrupted.start();
            awaitWaiting(interrupted);

            interrupted.interrupt();
            interrupted.join(1_000);
            assertFalse(interrupted.isAlive(), "an interrupted waiter returns at once");
            assertTrue(interrupted.error.getMessage().startsWith("Interrupted while opening"), interrupted.error.getMessage());
            assertTrue(interrupted.stillInterrupted, "and keeps its interrupt flag");

            opener.interrupt(); // the caller whose request started the opening
            opener.join(1_000);
            assertFalse(opener.isAlive());
            assertTrue(opener.error.getMessage().startsWith("Interrupted while opening"), opener.error.getMessage());

            waiter.join(15_000);
            assertTrue(waiter.error.getMessage().startsWith("Reading the cluster ID from localhost:1 failed"),
                    "the others get the real outcome: " + waiter.error.getMessage());
            assertFalse(waiter.error.getMessage().contains("Interrupted"), waiter.error.getMessage());
            assertEquals(before, KafkaClients.openCount(), "nothing left open");
        } finally {
            LogCapture.stop();
        }
    }

    @Test
    void aPairEveryCallerGaveUpOnIsClosedOnceOpen() throws Exception {
        DriverConfig config = config();
        int before = KafkaClients.openCount();
        for (int i = 0; i < 5; i++) {
            Caller caller = new Caller(config);
            caller.start();
            caller.interrupt(); // whether before, during or after the opening
            caller.join(30_000);
        }
        TestStack.await(() -> KafkaClients.openCount() == before, "abandoned pairs to close");
        TestStack.await(() -> {
            try {
                return !adminBean(config.clientId()) && !adminBean(config.clientId() + "-2");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, "their admin clients to close");
    }

    /**
     * Login modules load from the driver, whatever the caller's context class loader sees: a pair opened
     * first under one that can't see PlainLoginModule leaves no opening thread that can't either.
     */
    @Test
    void saslLoginsDontDependOnTheCallersContextClassLoader() throws Exception {
        Thread caller = Thread.currentThread();
        ClassLoader normal = caller.getContextClassLoader();
        caller.setContextClassLoader(new ClassLoader(normal) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.endsWith(".security.plain.PlainLoginModule")) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        });
        try {
            try (KafkaClients.Lease warm = KafkaClients.acquire(config())) {
                warm.clients();
            }
            // A JAAS value of its own, so no login cached by another test answers for it.
            try (KafkaClients.Lease sasl = KafkaClients.acquire(configFor(
                    "it-" + UUID.randomUUID().toString().substring(0, 8),
                    System.getProperty("test.kafka.sasl.bootstrap", "localhost:9094"),
                    "security.protocol", "SASL_PLAINTEXT", "sasl.mechanism", "PLAIN",
                    "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"test\" password=\"test-secret\" nonce=\"" + UUID.randomUUID() + "\";"))) {
                assertEquals(TestStack.admin().describeCluster().clusterId().get(), sasl.clients().clusterId());
            }
        } finally {
            caller.setContextClassLoader(normal);
        }
    }

    /** Acquires and closes a lease on its own thread, keeping the error it met. */
    private static final class Caller extends Thread {
        private final DriverConfig config;
        volatile SQLException error;
        volatile boolean stillInterrupted;

        Caller(DriverConfig config) {
            this.config = config;
        }

        @Override
        public void run() {
            try (KafkaClients.Lease lease = KafkaClients.acquire(config)) {
                lease.clients();
            } catch (SQLException e) {
                error = e;
                stillInterrupted = Thread.currentThread().isInterrupted();
            }
        }
    }

    /** Until the caller waits for the pair to open. */
    private static void awaitWaiting(Thread t) {
        TestStack.await(() -> (t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING)
                && java.util.Arrays.stream(t.getStackTrace()).anyMatch(f -> f.getClassName().contains("CompletableFuture")),
                t.getName() + " waiting for the pair");
    }

    @Test
    void theClusterIdIsTheBrokers() throws Exception {
        try (KafkaClients.Lease c = KafkaClients.acquire(config())) {
            assertEquals(TestStack.admin().describeCluster().clusterId().get(), c.clients().clusterId());
        }
    }

    @Test
    void anUnreachableClusterFailsWithoutLeakingClientsOrCredentials() throws Exception {
        int before = KafkaClients.openCount();
        LogCapture.start();
        SQLException e = assertThrows(SQLException.class, () -> KafkaClients.acquire(configFor(
                "it-" + UUID.randomUUID().toString().substring(0, 8), "localhost:1",
                "default.api.timeout.ms", "3000", "request.timeout.ms", "2000",
                "security.protocol", "SASL_PLAINTEXT", "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"test\" password=\"S3CR3T-unreachable\";")));
        String logs = String.join("\n", LogCapture.stop());
        StringWriter trace = new StringWriter();
        e.printStackTrace(new PrintWriter(trace));

        assertTrue(e.getMessage().startsWith("Reading the cluster ID from localhost:1 failed"), e.getMessage());
        assertTrue(trace.toString().contains("Caused by: "), "the cause is kept: " + trace);
        assertFalse(trace.toString().contains("S3CR3T-unreachable"), trace.toString());
        assertTrue(logs.contains("could not be established"), "the client's own warnings, captured: " + logs);
        assertFalse(logs.contains("S3CR3T-unreachable"), logs);
        assertEquals(before, KafkaClients.openCount());
    }

    @Test
    void anUnreachableClusterDoesntHoldUpOthers() throws Exception {
        DriverConfig bad = configFor("it-" + UUID.randomUUID().toString().substring(0, 8), "localhost:1",
                "default.api.timeout.ms", "4000", "request.timeout.ms", "2000");
        LogCapture.start();
        CompletableFuture<Void> stuck = CompletableFuture.runAsync(() -> {
            try {
                KafkaClients.acquire(bad).close();
            } catch (SQLException expected) {
                // fails after about 4 s
            }
        });
        try {
            Thread.sleep(300);
            long start = System.nanoTime();
            try (KafkaClients.Lease good = KafkaClients.acquire(config())) {
                good.clients();
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                assertTrue(millis < 2_000, "the healthy cluster's pair opened in " + millis + " ms");
                assertFalse(stuck.isDone(), "the unreachable one was still opening");
            }
        } finally {
            stuck.get(30, TimeUnit.SECONDS);
            LogCapture.stop();
        }
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
        try (KafkaClients.Lease lease = KafkaClients.acquire(config)) {
            KafkaClients c = lease.clients();
            assertTrue(c.listTopics().containsAll(List.of(append, compacted, both)));

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
        }
    }

    /** The connection's topics.include and topics.exclude decide what to show, Kafka's internal topics included. */
    @Test
    void kafkasInternalTopicsAreListed() throws Exception {
        String topic = TestStack.uniqueTopic("internal");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.inTransaction(topic, true, p -> p.send(new ProducerRecord<>(topic, utf8("k"), utf8("v"))));
        try (KafkaClients.Lease c = KafkaClients.acquire(config())) {
            assertTrue(c.clients().listTopics().contains("__transaction_state"));
        }
    }

    // ------------------------------------------------------------------ offsets

    @Test
    void offsetsOfAnEmptyTopicAndAfterAppends() throws Exception {
        String topic = TestStack.uniqueTopic("offsets");
        TestStack.createTopic(topic, 2, Map.of());
        try (KafkaClients.Lease lease = KafkaClients.acquire(config())) {
            KafkaClients c = lease.clients();
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
        }
    }

    @Test
    void offsetsForTimesBeforeBetweenAndAfterRecords() throws Exception {
        String topic = TestStack.uniqueTopic("times");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "k", "a", TestStack.recent(1_000L));
        TestStack.sendString(topic, "k", "b", TestStack.recent(2_000L));
        TestStack.sendString(topic, "k", "c", TestStack.recent(3_000L));
        try (KafkaClients.Lease lease = KafkaClients.acquire(config())) {
            KafkaClients c = lease.clients();
            assertEquals(Map.of(0, OptionalLong.of(0)), c.offsetsForTimes(topic, 1, TestStack.recent(500L)), "before");
            assertEquals(Map.of(0, OptionalLong.of(1)), c.offsetsForTimes(topic, 1, TestStack.recent(1_500L)), "between");
            assertEquals(Map.of(0, OptionalLong.of(2)), c.offsetsForTimes(topic, 1, TestStack.recent(3_000L)), "at");
            assertEquals(Map.of(0, OptionalLong.empty()), c.offsetsForTimes(topic, 1, TestStack.recent(3_001L)), "after");
        }
    }

    /** ListOffsets reads -1, -2, -3... as special requests; a timestamp before 1970 means "from the start". */
    @Test
    void negativeTimestampsMeanTheLogStart() throws Exception {
        String topic = TestStack.uniqueTopic("negative");
        TestStack.createTopic(topic, 2, Map.of());
        TestStack.send(topic, 0, null, utf8("k"), utf8("one"), null);
        TestStack.send(topic, 0, null, utf8("k"), utf8("two"), null);
        try (KafkaClients.Lease lease = KafkaClients.acquire(config())) {
            for (long t : List.of(-1L, -2L, -3L, -4L, -5L, -100L, Long.MIN_VALUE)) {
                assertEquals(Map.of(0, OptionalLong.of(0), 1, OptionalLong.empty()),
                        lease.clients().offsetsForTimes(topic, 2, t), "timestamp " + t);
            }
            assertEquals(Map.of(0, OptionalLong.of(0), 1, OptionalLong.empty()),
                    lease.clients().offsetsForTimes(topic, 2, 0L), "and 0, as before");
        }
    }

    @Test
    void anOpenTransactionHoldsTheEndOffsetBack() throws Exception {
        String topic = TestStack.uniqueTopic("lso");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "k", "committed", null);
        try (KafkaClients.Lease lease = KafkaClients.acquire(config());
             KafkaProducer<byte[], byte[]> tx = TestStack.transactionalProducer()) {
            KafkaClients c = lease.clients();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, utf8("k"), utf8("open-1")));
            tx.send(new ProducerRecord<>(topic, utf8("k"), utf8("open-2")));
            tx.flush();
            assertEquals(Map.of(0, 1L), c.endOffsets(topic, 1).offsets(), "the last stable offset");
            tx.commitTransaction();
            // The broker writes the commit marker after commitTransaction() returns.
            TestStack.await(() -> endOffset(c, topic) == 4L, "two records and the commit marker");
        }
    }

    @Test
    void offsetLookupsLogNoWarnings() throws Exception {
        String topic = TestStack.uniqueTopic("quiet");
        TestStack.createTopic(topic, 3, Map.of());
        TestStack.sendString(topic, "k", "v", null);
        try (KafkaClients.Lease lease = KafkaClients.acquire(config())) {
            KafkaClients c = lease.clients();
            LogCapture.start();
            c.logStartOffsets(topic, 3);
            c.endOffsets(topic, 3);
            c.offsetsForTimes(topic, 3, TestStack.recent(0));
            c.offsetsForTimes(topic, 3, -1L);
            String logs = String.join("\n", LogCapture.stop());
            assertFalse(logs.contains("WARN"), logs);
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
        try (KafkaClients.Lease lease = KafkaClients.acquire(config())) {
            KafkaClients c = lease.clients();
            TestStack.await(() -> {
                // A newer record rolls the old segment once segment.ms has passed; retention then deletes it.
                TestStack.sendString(topic, "k", "new", null);
                try {
                    return c.logStartOffsets(topic, 1).get(0) >= 3;
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }, "retention to delete the old segment");
        }
    }

    private static long endOffset(KafkaClients c, String topic) {
        try {
            return c.endOffsets(topic, 1).offsets().get(0);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void clientIdsAreReservedUntilReleased() {
        String base = "ids-" + UUID.randomUUID().toString().substring(0, 8);
        String a = ClientIds.reserve(base);
        String b = ClientIds.reserve(base);
        assertEquals(base, a);
        assertEquals(base + "-2", b);
        ClientIds.release(a);
        assertEquals(base, ClientIds.reserve(base), "free again once released");
        assertNotEquals(b, ClientIds.reserve(base));
    }
}
