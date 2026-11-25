package io.github.ifnesi.kafcacheql.sync;

import static io.github.ifnesi.kafcacheql.sync.SyncWorkerIT.values;
import static org.junit.jupiter.api.Assertions.*;

import io.github.ifnesi.kafcacheql.cache.RecordStore;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sql.QueryPipeline;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Whole validity flows through the sync entry point until JDBC connections exist. */
class CacheValidityIT {
    @TempDir Path dir;
    @BeforeAll static void stack() { TestStack.requireUp(); }

    DriverConfig config(String... settings) throws SQLException {
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("cache.db").toString());
        p.setProperty("sync.batch.records", "1");
        for (int i = 0; i < settings.length; i += 2) p.setProperty(settings[i], settings[i + 1]);
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }

    static long sync(CacheContext.Lease cache, String topic) throws SQLException {
        try (var execution = new Execution(Duration.ofSeconds(20))) {
            return execution.run(false, () -> cache.sync(cache.plan(topic, execution), execution));
        }
    }

    static List<String> rows(CacheContext.Lease cache, String sql) throws SQLException {
        return cache.read(c -> values(c, sql));
    }

    static List<String> payloads(CacheContext.Lease cache, String topic) throws SQLException {
        return rows(cache, "SELECT _value FROM \"" + topic + "\" ORDER BY _partition,_offset");
    }

    static void assertRemoved(CacheContext.Lease cache, long id) throws SQLException {
        assertEquals(List.of("0"), rows(cache, "SELECT count(*) FROM sqlite_master WHERE name='$t" + id + "'"));
        for (String table : List.of("$ranges", "$partitions")) {
            assertEquals(List.of("0"), rows(cache, "SELECT count(*) FROM \"" + table + "\" WHERE topic_ref=" + id));
        }
    }

    private static boolean rebuild(String line, String topic, String reason) {
        return line.startsWith("WARN io.github.ifnesi.kafcacheql.cache.TopicRegistry: Rebuilding topic " + topic + ": " + reason);
    }

    private static void assertRebuild(LogCapture.Capture logs, String topic, String reason) {
        assertTrue(logs.lines().stream().anyMatch(line -> rebuild(line, topic, reason)), String.join("\n", logs.lines()));
    }

    /** PLAN §9: "Topic ID changed ... Rebuild"; "Topic no longer exists ... Drop its storage table and view".
     * §10.1: the retained deleted generation rejects an older observation; other topics stay intact.
     */
    @Test void recreatedAndDeletedTopicsReplaceOnlyTheirOwnGeneration() throws Exception {
        String topic = TestStack.uniqueTopic("validity-id"), other = TestStack.uniqueTopic("untouched");
        for (String t : List.of(topic, other)) {
            TestStack.createTopic(t, 1, Map.of());
            TestStack.sendString(t, "key", "old", null);
        }
        try (var logs = LogCapture.capture();
             var cache = CacheContext.acquire(config()); var execution = new Execution(Duration.ofSeconds(20))) {
            long old = sync(cache, topic), untouched = sync(cache, other);
            TestStack.recreateTopic(topic, 1, Map.of());
            TestStack.sendString(topic, "key", "new", null);
            long replacement = sync(cache, topic);
            assertTrue(replacement > old);
            assertRemoved(cache, old);
            assertEquals(List.of("\"new\""), payloads(cache, topic));
            assertEquals(untouched, sync(cache, other));
            assertEquals(List.of("\"old\""), payloads(cache, other));

            SyncPlan stale = cache.plan(topic, execution);
            TestStack.deleteTopic(topic);
            SQLException missing = assertThrows(SQLException.class, () -> sync(cache, topic));
            assertEquals("42S02", missing.getSQLState());
            assertTrue(missing.getMessage().contains(topic));
            assertRemoved(cache, replacement);
            assertEquals(List.of("1"), rows(cache, "SELECT deleted_ms IS NOT NULL FROM \"$topics\" WHERE id=" + replacement));
            assertEquals(List.of("0"), rows(cache, "SELECT count(*) FROM sqlite_master WHERE name='" + topic + "'"));
            assertThrows(StaleGeneration.class, () -> cache.sync(stale, execution));
            TestStack.createTopic(topic, 1, Map.of());
            TestStack.sendString(topic, "key", "back", null);
            assertTrue(sync(cache, topic) > replacement);
            assertEquals(List.of("\"back\""), payloads(cache, topic));
            assertEquals(List.of("\"old\""), payloads(cache, other));
            assertRebuild(logs, topic, "topic ID changed from ");
        }
    }

    /** PLAN §9: changed mode, format override or decoder_version rebuilds; WARN names the setting and both values. */
    @Test void changedSettingsAndDecoderRebuildAndExplainBothValues() throws Exception {
        String topic = TestStack.uniqueTopic("validity-settings");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendJson(topic, "a", "{\"n\":1}", null);
        TestStack.sendJson(topic, "a", "{\"n\":2}", null);
        long id;
        try (var cache = CacheContext.acquire(config())) {
            id = sync(cache, topic);
            assertEquals(2, payloads(cache, topic).size());
        }
        List<String[]> changes = List.of(
                new String[] {"mode", "append", "compacted"},
                new String[] {"key.format", "auto", "bytes"},
                new String[] {"value.format", "auto", "string"});
        Properties settings = new Properties();
        for (String[] change : changes) {
            String key = "topic." + topic + "." + change[0];
            settings.setProperty(key, change[2]);
            var kv = settings.stringPropertyNames().stream().sorted()
                    .flatMap(k -> java.util.stream.Stream.of(k, settings.getProperty(k))).toArray(String[]::new);
            String logs;
            LogCapture.start();
            try (var cache = CacheContext.acquire(config(kv))) {
                long next = sync(cache, topic);
                assertTrue(next > id, change[0]);
                assertRemoved(cache, id);
                id = next;
                assertEquals(List.of(change[0].equals("value.format") ? "\"{\\\"n\\\":2}\"" : "{\"n\":2}"), payloads(cache, topic));
                if (change[0].equals("key.format")) assertEquals(List.of("\"YQ==\""), rows(cache, "SELECT _key FROM \"" + topic + "\""));
            } finally { logs = String.join("\n", LogCapture.stop()); }
            assertTrue(logs.lines().anyMatch(l -> l.startsWith("WARN ") && l.contains(topic)
                    && l.contains(change[0]) && l.contains(change[1]) && l.contains(change[2])), logs);
        }
        var kv = settings.stringPropertyNames().stream()
                .flatMap(k -> java.util.stream.Stream.of(k, settings.getProperty(k))).toArray(String[]::new);
        String logs;
        LogCapture.start();
        try (var cache = CacheContext.acquire(config(kv)); var execution = new Execution(Duration.ofSeconds(20))) {
            long before = id;
            cache.write(topic, before, execution, c -> {
                try (var s = c.createStatement()) { s.execute("UPDATE \"$topics\" SET decoder_version=0 WHERE id=" + before); }
                return null;
            });
            assertTrue(sync(cache, topic) > before);
            assertRemoved(cache, before);
            assertEquals(List.of("\"{\\\"n\\\":2}\""), payloads(cache, topic));
        } finally { logs = String.join("\n", LogCapture.stop()); }
        assertTrue(logs.lines().anyMatch(l -> l.startsWith("WARN ") && l.contains("decoder_version")
                && l.contains("0") && l.contains(Integer.toString(io.github.ifnesi.kafcacheql.schema.AvroJsonConverter.VERSION))), logs);
    }

    /** PLAN §9: cached offsets below log start prune append topics and rebuild compacted topics. */
    @Test void advancingLogStartsPrunesAppendAndRebuildsCompacted() throws Exception {
        try (var cache = CacheContext.acquire(config())) {
            for (boolean compacted : List.of(false, true)) {
                String topic = TestStack.uniqueTopic("validity-retention");
                TestStack.createTopic(topic, 1, Map.of("cleanup.policy", compacted ? "compact,delete" : "delete"));
                TestStack.sendString(topic, "gone", "old", null);
                TestStack.sendString(topic, "keep", "current", null);
                long old = sync(cache, topic);
                TestStack.admin().deleteRecords(Map.of(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(1))).all().get();
                try (var logs = LogCapture.capture()) {
                    long next = sync(cache, topic);
                    assertEquals(compacted, next > old);
                    assertEquals(List.of("\"current\""), payloads(cache, topic));
                    if (compacted) assertRemoved(cache, old);
                    else assertEquals(List.of("1"), rows(cache, "SELECT pruned_below FROM \"$partitions\" WHERE topic_ref=" + old));
                    if (compacted) assertRebuild(logs, topic, "cached offset 0 below log start 1 in partition 0");
                }
            }
        }
    }

    /** PLAN §9: tombstones may have been removed while idle, so an expired compacted cache rebuilds. */
    @Test void tombstoneRemovedWhileIdleCannotLeaveADeletedKeyCached() throws Exception {
        for (boolean partial : List.of(false, true)) {
            String topic = TestStack.uniqueTopic("validity-tombstone");
            TestStack.createTopic(topic, 1, Map.of("cleanup.policy", "compact", "segment.ms", "200",
                    "min.cleanable.dirty.ratio", "0.01", "delete.retention.ms", "1000", "max.compaction.lag.ms", "1000"));
            TestStack.sendString(topic, "gone", "old", null);
            TestStack.sendString(topic, "keep", "current", null);
            try (var cache = CacheContext.acquire(config()); var execution = new Execution(Duration.ofSeconds(20))) {
                long old;
                if (partial) {
                    assertThrows(SQLException.class, () -> cache.sync(cache.plan(topic, execution), execution, (c, batch) -> {
                        RecordStore.write(c, batch);
                        if (batch.start() > 0) throw new SQLException("interrupted initial fill");
                    }));
                    old = Long.parseLong(rows(cache, "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'").getFirst());
                    assertEquals(List.of("null"), rows(cache, "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + old));
                } else old = sync(cache, topic);
                TestStack.sendTombstone(topic, "gone", null);
                TestStack.await(() -> {
                    TestStack.sendString(topic, "keep", "current", null);
                    return TestStack.readAll(topic, true).stream().noneMatch(r -> "gone".equals(TestStack.utf8(r.key())));
                }, "the cleaner to remove the tombstone and its old value");
                assertEquals(partial ? 1 : 2, payloads(cache, topic).size(), "cache stayed idle while Kafka removed the deletion");
                String reason = partial ? "first added " : "last full sync ";
                try (var logs = LogCapture.capture()) {
                    assertTrue(sync(cache, topic) > old);
                    assertRemoved(cache, old);
                    assertEquals(List.of("\"keep\"|\"current\""), rows(cache, "SELECT _key,_value FROM \"" + topic + "\""));
                    assertRebuild(logs, topic, reason);
                    assertTrue(logs.lines().stream().anyMatch(line -> rebuild(line, topic, reason)
                            && line.contains(" ago, longer than delete.retention.ms (1s)")), logs.lines().toString());
                }
            }
        }
    }

    /** PLAN §9–10: the original caught-up/creation baseline must stay safe through sync completion;
     * §10.1: an expired generation rebuilds through the query's single replan.
     */
    @Test void tombstoneRemovedDuringSlowSyncCannotLeaveADeletedKeyCached() throws Exception {
        String topic = TestStack.uniqueTopic("validity-slow-tombstone");
        TestStack.createTopic(topic, 1, Map.of("cleanup.policy", "compact", "segment.ms", "200",
                "min.cleanable.dirty.ratio", "0.01", "delete.retention.ms", "5000", "max.compaction.lag.ms", "1000"));
        TestStack.sendString(topic, "keep", "current", null);
        TestStack.sendString(topic, "gone", "old", null);
        long old;
        try (var cache = CacheContext.acquire(config())) { old = sync(cache, topic); }
        TestStack.sendTombstone(topic, "gone", null);
        TestStack.sendString(topic, "roll", "new", null);
        // Reopen so a consumer's prefetched tombstone cannot hide the deletion race.
        try (var logs = LogCapture.capture();
             var cache = CacheContext.acquire(config()); var execution = new Execution(Duration.ofSeconds(30))) {
            AtomicBoolean delayed = new AtomicBoolean();
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void beforeFetch(SyncPlan plan) {
                    if (!delayed.compareAndSet(false, true)) return;
                    TestStack.await(() -> {
                        TestStack.sendString(topic, "roll", "new", null);
                        return TestStack.readAll(topic, true).stream().noneMatch(r -> "gone".equals(TestStack.utf8(r.key())));
                    }, "the cleaner to remove a tombstone during sync");
                }
            });
            var result = QueryPipeline.execute(cache, "SELECT _key,_value FROM \"" + topic + "\" ORDER BY _key",
                    Map.of(), execution);
            assertTrue(delayed.get());
            assertEquals(List.of(List.of("\"keep\"", "\"current\""), List.of("\"roll\"", "\"new\"")), result.rows());
            assertEquals(List.of(Long.toString(old + 1)), rows(cache, "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'"));
            assertRemoved(cache, old);
            assertRebuild(logs, topic, "last full sync ");
            assertTrue(logs.lines().stream().anyMatch(line -> rebuild(line, topic, "last full sync ")
                    && line.contains(" ago, longer than delete.retention.ms (5s)")), logs.lines().toString());
        }
    }

    /** PLAN §10.1: each compacted partition is a "clean prefix", including batches a concurrent reader
     * sees during sync; §9: expiry rebuilds the unsafe generation through the existing single replan.
     */
    @Test void readersBetweenCompactedBatchesNeverSeeAnExpiredPartialCommit() throws Exception {
        String topic = TestStack.uniqueTopic("validity-partial-expiry");
        TestStack.createTopic(topic, 1, Map.of("cleanup.policy", "compact", "segment.ms", "200",
                "min.cleanable.dirty.ratio", "0.01", "delete.retention.ms", "5000", "max.compaction.lag.ms", "1000"));
        TestStack.sendString(topic, "sentinel", "stable", null);
        TestStack.sendString(topic, "gone", "old", null);
        long old;
        try (var cache = CacheContext.acquire(config())) { old = sync(cache, topic); }
        TestStack.sendTombstone(topic, "gone", null);
        TestStack.sendString(topic, "fresh-1", "new", null);
        TestStack.sendString(topic, "fresh-2", "new", null);
        // A fresh consumer must fetch after cleaning, rather than use a prefetched tombstone.
        try (var logs = LogCapture.capture();
             var cache = CacheContext.acquire(config()); var reader = CacheContext.acquire(config());
             var execution = new Execution(Duration.ofSeconds(30)); var readers = Executors.newSingleThreadExecutor()) {
            AtomicBoolean delayed = new AtomicBoolean();
            var snapshots = new ArrayList<List<String>>();
            String sql = "SELECT _key,_value FROM \"" + topic + "\" ORDER BY _key";
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void beforeFetch(SyncPlan plan) {
                    if (!delayed.compareAndSet(false, true)) return;
                    TestStack.await(() -> {
                        TestStack.sendString(topic, "roll", "new", null);
                        return TestStack.readAll(topic, true).stream().noneMatch(r -> "gone".equals(TestStack.utf8(r.key())));
                    }, "the cleaner to remove a tombstone before intermediate batches");
                }
                @Override public void afterCommit(SyncBatch batch) throws SQLException {
                    if (!batch.reachedEnd()) {
                        // The worker pauses outside its transaction while another connection reads its commit.
                        snapshots.add(execution.await(readers.submit(() -> rows(reader, sql))));
                    }
                }
            });
            var result = QueryPipeline.execute(cache, sql, Map.of(), execution);
            assertEquals(List.of(List.of("\"fresh-1\"", "\"new\""), List.of("\"fresh-2\"", "\"new\""),
                    List.of("\"roll\"", "\"new\""), List.of("\"sentinel\"", "\"stable\"")), result.rows());
            assertTrue(delayed.get());
            assertTrue(snapshots.stream().anyMatch(rows -> rows.contains("\"fresh-1\"|\"new\"")),
                    "The reader must see a committed row after the tombstone, before sync finishes");
            for (var snapshot : snapshots) assertFalse(snapshot.contains("\"gone\"|\"old\""), snapshot.toString());
            assertEquals(List.of(Long.toString(old + 1)), rows(cache, "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'"));
            assertRemoved(cache, old);
            assertRebuild(logs, topic, "last full sync ");
            assertTrue(logs.lines().stream().anyMatch(line -> rebuild(line, topic, "last full sync ")
                    && line.contains(" ago, longer than delete.retention.ms (5s)")), logs.lines().toString());
        }
    }

    /** PLAN §9/§10.1: two processes rebuild once, and an older Kafka observation cannot replace a newer generation. */
    @Test void competingProcessesRebuildOnceAndOlderObservationsReplan() throws Exception {
        String topic = TestStack.uniqueTopic("validity-race");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "key", "old", null);
        try (var logs = LogCapture.capture();
             var cache = CacheContext.acquire(config())) {
            long old = sync(cache, topic);
            TestStack.recreateTopic(topic, 1, Map.of());
            TestStack.sendString(topic, "key", "replacement", null);
            Process a = child(cache.path(), topic), b = child(cache.path(), topic);
            try {
                assertEquals("READY", SyncWorkerIT.line(a)); assertEquals("READY", SyncWorkerIT.line(b));
                release(a); release(b);
                assertEquals("PREPARING", SyncWorkerIT.line(a)); assertEquals("PREPARING", SyncWorkerIT.line(b));
                release(a); release(b);
                long first = result(a), second = result(b);
                assertEquals(old + 1, first, "one replacement allocation commits");
                assertEquals(first, second);
                assertEquals(List.of("\"replacement\""), payloads(cache, topic));

                TestStack.recreateTopic(topic, 1, Map.of());
                TestStack.sendString(topic, "key", "older-observation", null);
                Process stale = child(cache.path(), topic);
                try {
                    assertEquals("READY", SyncWorkerIT.line(stale));
                    release(stale);
                    assertEquals("PREPARING", SyncWorkerIT.line(stale));
                    TestStack.recreateTopic(topic, 1, Map.of());
                    TestStack.sendString(topic, "key", "newest", null);
                    long newest = sync(cache, topic);
                    release(stale);
                    assertEquals(newest, result(stale));
                    assertEquals(List.of(TestStack.topicId(topic).toString()), rows(cache, "SELECT topic_uuid FROM \"$topics\" WHERE id=" + newest));
                    assertEquals(List.of("\"newest\""), payloads(cache, topic));
                } finally { stale.destroyForcibly(); }
            } finally { a.destroyForcibly(); b.destroyForcibly(); }
            assertEquals(2, logs.lines().stream().filter(line -> rebuild(line, topic, "topic ID changed from ")).count());
        }
    }

    /** PLAN §9: replacement failure preserves the old generation; refill failure keeps the new committed batches. */
    @Test void replacementRollsBackButRefillFailureKeepsItsCommittedPrefix() throws Exception {
        String topic = TestStack.uniqueTopic("validity-rollback"), other = TestStack.uniqueTopic("validity-other");
        for (String t : List.of(topic, other)) {
            TestStack.createTopic(t, 1, t.equals(topic) ? Map.of("cleanup.policy", "compact") : Map.of());
            TestStack.sendString(t, "key", "old", null);
        }
        try (var logs = LogCapture.capture();
             var cache = CacheContext.acquire(config()); var execution = new Execution(Duration.ofSeconds(30))) {
            long old = sync(cache, topic), untouched = sync(cache, other);
            var rangesBefore = rows(cache, "SELECT * FROM \"$ranges\" WHERE topic_ref=" + old);
            var partitionsBefore = rows(cache, "SELECT * FROM \"$partitions\" WHERE topic_ref=" + old);
            cache.write(topic, old, execution, c -> {
                try (var s = c.createStatement()) {
                    s.execute("CREATE TEMP TRIGGER fail_rebuild AFTER INSERT ON \"$topics\" WHEN NEW.topic='" + topic
                            + "' BEGIN SELECT RAISE(ABORT, 'planted replacement failure'); END");
                }
                return null;
            });
            TestStack.recreateTopic(topic, 1, Map.of("cleanup.policy", "compact"));
            for (int i = 0; i < 3; i++) TestStack.sendString(topic, "k" + i, "new" + i, null);
            SQLException replacementFailure = assertThrows(SQLException.class, () -> sync(cache, topic));
            assertTrue(replacementFailure.getMessage().contains("planted replacement failure"));
            assertEquals(List.of("\"old\""), payloads(cache, topic));
            assertEquals(List.of(Long.toString(old)), rows(cache, "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'"));
            assertEquals(rangesBefore, rows(cache, "SELECT * FROM \"$ranges\" WHERE topic_ref=" + old));
            assertEquals(partitionsBefore, rows(cache, "SELECT * FROM \"$partitions\" WHERE topic_ref=" + old));
            cache.write(topic, old, execution, c -> { try (var s = c.createStatement()) { s.execute("DROP TRIGGER fail_rebuild"); } return null; });
            SyncPlan refill = cache.plan(topic, execution);
            assertThrows(SQLException.class, () -> cache.sync(refill, execution, (c, batch) -> {
                RecordStore.write(c, batch);
                if (batch.start() > 0) throw new SQLException("planted refill failure");
            }));
            long next = Long.parseLong(rows(cache, "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'").getFirst());
            assertTrue(next > old);
            assertRemoved(cache, old);
            assertEquals(List.of("\"new0\""), payloads(cache, topic));
            assertEquals(List.of("0|1"), rows(cache, "SELECT start_offset,end_offset FROM \"$ranges\" WHERE topic_ref=" + next));
            assertEquals(List.of("null"), rows(cache, "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + next));
            var fetched = new java.util.concurrent.atomic.AtomicInteger();
            assertEquals(next, cache.sync(cache.plan(topic, execution), execution, (c, batch) -> {
                fetched.addAndGet(batch.records().size()); RecordStore.write(c, batch);
            }));
            assertEquals(2, fetched.get(), "a fresh partial compacted fill continues its committed prefix");
            assertEquals(List.of("\"new0\"", "\"new1\"", "\"new2\""), payloads(cache, topic));
            assertEquals(untouched, sync(cache, other));
            assertEquals(List.of("\"old\""), payloads(cache, other));
            assertRebuild(logs, topic, "topic ID changed from ");
        }
    }

    Process child(Path file, String topic) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                System.getProperty("java.class.path"), ValidityProcess.class.getName(), file.toString(), topic)
                .start();
    }
    static void release(Process process) throws Exception {
        process.getOutputStream().write('\n'); process.getOutputStream().flush();
    }
    static long result(Process process) throws Exception {
        String result = SyncWorkerIT.line(process);
        assertTrue(process.waitFor(20, TimeUnit.SECONDS));
        String stderr = new String(process.getErrorStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        stderr.lines().forEach(line -> {
            if (line.startsWith("WARN ") || line.startsWith("ERROR ")) LogCapture.record(line);
            else System.err.println(line);
        });
        assertEquals(0, process.exitValue());
        assertTrue(result != null && result.startsWith("RESULT "), result);
        return Long.parseLong(result.substring(7));
    }
}
