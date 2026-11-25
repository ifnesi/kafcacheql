package io.github.ifnesi.kafcacheql.sync;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.cache.RecordStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.StoredSchema;
import io.github.ifnesi.kafcacheql.cache.SchemaStore.SubjectSchema;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.schema.SchemaRegistry;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Kafka -> worker -> real batch writer -> SQLite. JDBC statements are a later item. */
class SyncWorkerIT {
    @TempDir Path dir;

    @BeforeAll static void stack() { TestStack.requireUp(); }

    DriverConfig config(String... settings) throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("cache.db").toString());
        p.setProperty("sync.batch.records", "2");
        for (int i = 0; i < settings.length; i += 2) p.setProperty(settings[i], settings[i + 1]);
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }

    /** PLAN §10: "The end of a range is consumer.position(), not last offset + 1."
     * §10.1: "two queries asking for the same window cause one fetch"; requests run "in order".
     */
    @Test void sharedWorkerFetchesCommittedWindowsOnceAndStopsAtTheCapturedEnd() throws Exception {
        String topic = TestStack.uniqueTopic("worker-tx");
        String other = TestStack.uniqueTopic("worker-other");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.createTopic(other, 1, Map.of());
        TestStack.inTransaction(topic, true, p -> p.send(new ProducerRecord<>(topic, TestStack.utf8("k"), TestStack.utf8("yes"))));
        TestStack.inTransaction(topic, false, p -> p.send(new ProducerRecord<>(topic, TestStack.utf8("k"), TestStack.utf8("no"))));
        TestStack.sendString(other, "k", "other", null);
        AtomicInteger fetched = new AtomicInteger();
        AtomicInteger rangesFetched = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        BatchWriter writer = (c, b) -> {
            fetched.addAndGet(b.records().size());
            RecordStore.write(c, b);
        };
        try (var first = CacheContext.acquire(config()); var second = CacheContext.acquire(config());
             var execution = new Execution(Duration.ofSeconds(20))) {
            assertEquals(first.path(), second.path());
            SyncPlan plan = first.plan(topic, execution);
            first.hooks(new SyncWorker.Hooks() {
                @Override public void beforeFetch(SyncPlan p) throws java.sql.SQLException {
                    if (p.topic().name().equals(topic)) { rangesFetched.incrementAndGet(); pause(entered, release); }
                }
            });
            var a = CompletableFuture.supplyAsync(() -> sync(first, plan, execution, writer));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            var b = CompletableFuture.supplyAsync(() -> sync(second, plan, execution, writer));
            SyncPlan otherPlan = second.plan(other, execution);
            var d = CompletableFuture.supplyAsync(() -> sync(second, otherPlan, execution, writer));
            release.countDown();
            long id = a.get(15, TimeUnit.SECONDS);
            assertEquals(id, b.get(15, TimeUnit.SECONDS));
            d.get(15, TimeUnit.SECONDS);
            assertEquals(2, fetched.get());
            assertEquals(1, rangesFetched.get());
            assertEquals(List.of("\"yes\""), first.read(c -> values(c, "SELECT _value FROM \"$t" + id + "\"")));
            assertEquals(List.of("0|" + plan.ends().offsets().get(0)), first.read(c -> values(c,
                    "SELECT start_offset, end_offset FROM \"$ranges\" WHERE topic_ref=" + id)));
        } finally { release.countDown(); }
    }

    /** PLAN §10.1: "a query whose windows are cached never waits behind another query's sync";
     * "Cancelling a request still in the queue takes it off"; "a late cancel can't stop the next request".
     * §10: "Batches already committed are kept". Background copies: "at most 1,000 entries wait".
     */
    @Test void cancelledPartialSyncLeavesCachedReadsFreeAndTheNextRequestFinishesItsWindow() throws Exception {
        String topic = TestStack.uniqueTopic("partial"), cached = TestStack.uniqueTopic("cached"), cancelled = TestStack.uniqueTopic("queued");
        for (String name : List.of(topic, cached, cancelled)) TestStack.createTopic(name, 1, Map.of());
        for (int i = 0; i < 7; i++) TestStack.sendString(topic, "k" + i, "v" + i, null);
        TestStack.sendString(cached, "k", "cached", null);
        TestStack.sendString(cancelled, "k", "never", null);
        CountDownLatch committed = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger fetched = new AtomicInteger();
        BatchWriter writer = (c, b) -> { fetched.addAndGet(b.records().size()); RecordStore.write(c, b); };
        try (var first = CacheContext.acquire(config()); var second = CacheContext.acquire(config());
             var initial = new Execution(Duration.ofSeconds(20)); var next = new Execution(Duration.ofSeconds(20));
             var queueToken = new Execution(Duration.ofSeconds(20))) {
            second.sync(second.plan(cached, next), next, RecordStore::write);
            SyncPlan hot = second.plan(cached, next);
            SyncPlan plan = first.plan(topic, initial), queuedPlan = second.plan(cancelled, queueToken);
            first.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch b) throws SQLException {
                    if (committed.getCount() != 0 && b.plan().topic().name().equals(topic)) pause(committed, release);
                }
            });
            var a = CompletableFuture.supplyAsync(() -> sync(first, plan, initial, writer));
            assertTrue(committed.await(10, TimeUnit.SECONDS));
            assertEquals(List.of("null"), first.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\" p JOIN \"$topics\" t ON t.id=p.topic_ref WHERE t.topic='" + topic + "'")),
                    "PLAN L576: a partial sync does not advance the captured time");
            var queued = CompletableFuture.supplyAsync(() -> sync(second, queuedPlan, queueToken, writer));
            TestStack.await(() -> second.queuedRequests() == 1, "request queued behind the held sync");
            // A cached read finishes while the worker cannot leave the hook, with schema work waiting.
            for (int i = 0; i < 1001; i++) first.storeSchema(new StoredSchema("test-" + i, "AVRO", "\"string\"", "[]", i));
            first.storeSubject(new SubjectSchema("dropped-value", 1, new StoredSchema("dropped", "AVRO", "\"int\"", "[]", 1)));
            assertEquals(1000, first.pendingBackground());
            long hotId = CompletableFuture.supplyAsync(() -> sync(second, hot, next, writer)).get(2, TimeUnit.SECONDS);
            assertEquals(List.of("\"cached\""), second.read(c -> values(c, "SELECT _value FROM \"$t" + hotId + "\"")));
            queueToken.cancel();
            assertCancelled(queued);
            TestStack.await(() -> second.queuedRequests() == 0, "cancel removes queued work");
            try (var deadline = new Execution(Duration.ofMillis(400))) {
                var expired = CompletableFuture.supplyAsync(() -> sync(second, queuedPlan, deadline, writer));
                TestStack.await(() -> second.queuedRequests() == 1, "deadline request queued");
                var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> expired.get(2, TimeUnit.SECONDS));
                Throwable cause = failure.getCause();
                if (cause instanceof java.util.concurrent.CompletionException) cause = cause.getCause();
                assertInstanceOf(SQLTimeoutException.class, cause);
                TestStack.await(() -> second.queuedRequests() == 0, "deadline removes queued work");
            }
            initial.cancel();
            assertCancelled(a);
            release.countDown();
            // PLAN §10, L598 and §10.1, L647: cancellation during transaction work rolls that
            // transaction back, including its ranges and captured time, while earlier batches remain.
            long partialId = first.read(c -> Long.parseLong(values(c,
                    "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'").getFirst()));
            CountDownLatch wrote = new CountDownLatch(1), finishTransaction = new CountDownLatch(1);
            try (var inTransaction = new Execution(Duration.ofSeconds(20));
                 var finalBatch = CacheContext.acquire(config("sync.batch.records", "5"))) {
                SyncPlan remaining = finalBatch.plan(topic, inTransaction);
                var cancelledCommit = CompletableFuture.supplyAsync(() -> sync(finalBatch, remaining, inTransaction, (c, batch) -> {
                    RecordStore.write(c, batch);
                    assertTrue(batch.reachedEnd(), "the interrupted transaction also wrote caught_up_at_ms");
                    pause(wrote, finishTransaction);
                }));
                try {
                    assertTrue(wrote.await(10, TimeUnit.SECONDS));
                    inTransaction.cancel();
                    assertCancelled(cancelledCommit);
                } finally { finishTransaction.countDown(); }
                // The caller's exception can precede rollback; wait for the worker, then inspect
                // immediately, before any successful sync can obscure the cancelled commit.
                second.write(topic, partialId, next, c -> null);
                assertEquals(List.of("0", "1"), second.read(c -> values(c,
                        "SELECT _offset FROM \"$t" + partialId + "\" ORDER BY _offset")));
                assertEquals(List.of("0|2"), second.read(c -> values(c,
                        "SELECT start_offset,end_offset FROM \"$ranges\" WHERE topic_ref=" + partialId)));
                assertEquals(List.of("null"), second.read(c -> values(c,
                        "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + partialId)));
            }
            // The cancellation's sticky wakeup is still pending when this request first polls.
            SyncPlan resumed = second.plan(topic, next);
            long id = second.sync(resumed, next, writer);
            assertEquals(7, fetched.get(), "committed records are not fetched again");
            assertEquals(List.of("7"), second.read(c -> values(c, "SELECT count(*) FROM \"$t" + id + "\"")));
            assertEquals(List.of("0|7"), second.read(c -> values(c, "SELECT start_offset,end_offset FROM \"$ranges\" WHERE topic_ref=" + id)));
            assertEquals(List.of(Long.toString(resumed.ends().capturedAtMs())), second.read(c -> values(c,
                    "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + id)));
            assertEquals(List.of("0"), second.read(c -> values(c, "SELECT count(*) FROM \"$topics\" WHERE topic='" + cancelled + "'")));
            assertEquals(List.of("0"), second.read(c -> values(c, "SELECT count(*) FROM \"$schemas\" WHERE ref='dropped'")));
        } finally { release.countDown(); }
    }

    /** PLAN §10.1: different client or cache-shaping settings are refused; "A refused connection leaves
     * the open context as it was"; "A cache file with more than one hard link is refused".
     */
    @Test void contextsFollowRealPathsAndRefusalsLeaveTheLiveCacheUsable() throws Exception {
        String topic = TestStack.uniqueTopic("context");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "k", "one", null);
        try (var first = CacheContext.acquire(config()); var token = new Execution(Duration.ofSeconds(20))) {
            long id = first.sync(first.plan(topic, token), token, RecordStore::write);
            Path link = Files.createSymbolicLink(dir.resolve("alias.db"), first.path());
            try (var alias = CacheContext.acquire(config("cache.path", link.toString(), "max.staleness.ms", "1000", "sync.batch.records", "1"))) {
                assertEquals(first.path(), alias.path());
                for (var mismatch : List.of(
                        Map.entry("topic." + topic + ".value.format", "string"),
                        Map.entry("topic." + topic + ".key.format", "json"),
                        Map.entry("topic." + topic + ".mode", "compacted"),
                        Map.entry("cache.prune.expired", "false"), Map.entry("fetch.min.bytes", "2"))) {
                    SQLException e = assertThrows(SQLException.class, () -> CacheContext.acquire(config(mismatch.getKey(), mismatch.getValue())));
                    assertTrue(e.getMessage().contains(mismatch.getKey().equals("fetch.min.bytes") ? "client settings" : mismatch.getKey()), e.getMessage());
                }
                TestStack.sendString(topic, "k", "two", null);
                TestStack.sendString(topic, "k", "three", null);
                alias.sync(alias.plan(topic, token), token, (c, batch) -> {
                    assertEquals(1, batch.records().size(), "batch size belongs to the requesting connection");
                    RecordStore.write(c, batch);
                });
            }
            TestStack.sendString(topic, "k", "four", null);
            assertEquals(id, first.sync(first.plan(topic, token), token, RecordStore::write));
            assertEquals(List.of("4"), first.read(c -> values(c, "SELECT count(*) FROM \"$t" + id + "\"")));
        }
        Path file = dir.resolve("cache.db"), hard = Files.createLink(dir.resolve("hard.db"), file);
        byte[] before = Files.readAllBytes(file);
        for (Path name : List.of(file, hard)) {
            SQLException e = assertThrows(SQLException.class, () -> CacheContext.acquire(config("cache.path", name.toString())));
            assertTrue(e.getMessage().contains("hard link"));
            assertFalse(Files.exists(Path.of(name + "-wal")));
        }
        assertArrayEquals(before, Files.readAllBytes(file));
    }

    /** PLAN §10.1 background writes: "Only the newest pending value per key is kept"; generations
     * no longer current are dropped; "Schema rows belong to no topic generation"; pending copies
     * "are dropped when the context closes"; "a failed one is logged and dropped".
     */
    @Test void backgroundRegistryCopiesCoalesceAndAdvanceIndependentlyOfTopicGenerations() throws Exception {
        String topic = TestStack.uniqueTopic("background"), blocking = TestStack.uniqueTopic("background-block");
        TestStack.createTopic(topic, 1, Map.of("cleanup.policy", "compact"));
        TestStack.createTopic(blocking, 1, Map.of());
        var avro = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Background\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}");
        var row = new org.apache.avro.generic.GenericData.Record(avro); row.put("n", 1);
        TestStack.sendAvro(topic, "a", row, null, TestStack.SchemaIdIn.PREFIX);
        DriverConfig config = config("schema.registry.url", TestStack.REGISTRY);
        CountDownLatch held = new CountDownLatch(1), release = new CountDownLatch(1);
        CacheContext.Lease cache = CacheContext.acquire(config);
        try (var execution = new Execution(Duration.ofSeconds(30)); var registry = SchemaRegistry.open(ClientSettings.of(config))) {
            SubjectSchema older = registry.latest(topic, false).orElseThrow();
            avro = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Background\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"},{\"name\":\"s\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
            row = new org.apache.avro.generic.GenericData.Record(avro); row.put("n", 2);
            TestStack.sendAvro(topic, "b", row, null, TestStack.SchemaIdIn.PREFIX);
            SubjectSchema newer = registry.latest(topic, false).orElseThrow();
            long id = cache.sync(cache.plan(topic, execution), execution, RecordStore::write);
            SyncPlan checked = cache.plan(topic, execution);
            TestStack.sendAvro(blocking, "k", row, null, TestStack.SchemaIdIn.PREFIX);
            SubjectSchema copy = registry.latest(blocking, false).orElseThrow();
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch b) throws SQLException { if (held.getCount() != 0) pause(held, release); }
            });
            SyncPlan blockPlan = cache.plan(blocking, execution);
            var sync = CompletableFuture.supplyAsync(() -> sync(cache, blockPlan, execution, RecordStore::write));
            assertTrue(held.await(10, TimeUnit.SECONDS));
            cache.storeSubject(older); cache.storeSubject(newer); cache.storeSubject(older);
            cache.sync(checked, execution, RecordStore::write);
            assertEquals(2, cache.pendingBackground(), "one subject-with-schema and one generation/partition entry");
            Process process = child("replace", cache.path(), topic);
            long replacement;
            try {
                String result = line(process); assertTrue(result.startsWith("REPLACED "), result);
                replacement = Long.parseLong(result.substring(9));
                assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, process.exitValue());
            } finally { process.destroyForcibly(); }
            String caughtUp = cache.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + replacement)).getFirst();
            release.countDown(); sync.get(10, TimeUnit.SECONDS);
            TestStack.await(() -> cache.pendingBackground() == 0, "background queue drained");
            cache.write(topic, replacement, execution, c -> null);
            assertNotEquals(id, replacement);
            assertEquals(List.of(Integer.toString(newer.version())), cache.read(c -> values(c,
                    "SELECT version FROM \"$subjects\" WHERE subject='" + newer.subject() + "'")));
            assertTrue((boolean) cache.read(c -> io.github.ifnesi.kafcacheql.cache.SchemaStore.find(c, newer.schema().ref()).isPresent()));
            assertEquals(List.of(caughtUp), cache.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + replacement)));
            // Once an entry failed under a real competing writer, it is dropped, not replayed later.
            io.github.ifnesi.kafcacheql.testkit.LogCapture.start();
            Process holder = child("lock", cache.path(), topic);
            try {
                assertEquals("LOCKED", line(holder));
                cache.storeSubject(copy);
                TestStack.await(() -> cache.pendingBackground() == 0, "failed copy taken");
                var barrier = CompletableFuture.supplyAsync(() -> {
                    try { return cache.write(topic, replacement, execution, c -> null); }
                    catch (SQLException e) { throw new java.util.concurrent.CompletionException(e); }
                });
                assertThrows(java.util.concurrent.TimeoutException.class, () -> barrier.get(250, TimeUnit.MILLISECONDS));
                unlock(holder); barrier.get(5, TimeUnit.SECONDS);
                assertTrue(io.github.ifnesi.kafcacheql.testkit.LogCapture.stop().stream().anyMatch(l -> l.contains("Dropped background cache write")));
                assertEquals(List.of("0"), cache.read(c -> values(c, "SELECT count(*) FROM \"$subjects\" WHERE subject='" + blocking + "-value'")));
            } finally { holder.destroyForcibly(); io.github.ifnesi.kafcacheql.testkit.LogCapture.stop(); }
            CountDownLatch closingHold = new CountDownLatch(1), closingRelease = new CountDownLatch(1);
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch b) throws SQLException { pause(closingHold, closingRelease); }
            });
            TestStack.sendString(blocking, "k", "second", null);
            SyncPlan pending = cache.plan(blocking, execution);
            var pendingSync = CompletableFuture.supplyAsync(() -> sync(cache, pending, execution, RecordStore::write));
            try {
                assertTrue(closingHold.await(10, TimeUnit.SECONDS));
                cache.storeSubject(copy);
                var close = CompletableFuture.runAsync(() -> { try { cache.close(); } catch (SQLException e) { throw new java.util.concurrent.CompletionException(e); } });
                TestStack.await(() -> cache.pendingBackground() == 0, "close drops pending copies");
                closingRelease.countDown(); close.get(10, TimeUnit.SECONDS);
                assertThrows(java.util.concurrent.ExecutionException.class, () -> pendingSync.get(2, TimeUnit.SECONDS));
            } finally { closingRelease.countDown(); }
        } finally { release.countDown(); cache.close(); }
        try (var reopened = io.github.ifnesi.kafcacheql.cache.CacheStore.open(dir.resolve("cache.db"), java.util.Optional.empty()); var reader = reopened.newReader()) {
            assertEquals(List.of("0"), values(reader, "SELECT count(*) FROM \"$subjects\" WHERE subject='" + blocking + "-value'"));
        }
    }

    static void assertCancelled(CompletableFuture<?> task) throws Exception {
        var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> task.get(2, TimeUnit.SECONDS));
        Throwable cause = failure.getCause();
        if (cause instanceof java.util.concurrent.CompletionException) cause = cause.getCause();
        assertInstanceOf(SQLException.class, cause);
        assertEquals("57014", ((SQLException) cause).getSQLState());
    }

    /** PLAN §10.1: "another process holding the write lock ... delays a batch"; "a deadline expiring
     * and a cancel arriving while it waits both end it promptly". No transaction waits on Kafka.
     */
    @Test void anotherProcessesWriteLockWaitsOnlyUntilTheRequestsDeadlineOrCancellation() throws Exception {
        String topic = TestStack.uniqueTopic("lock-wait");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendString(topic, "k", "v0", null);
        CountDownLatch fetched = new CountDownLatch(1), release = new CountDownLatch(1);
        try (var cache = CacheContext.acquire(config()); var token = new Execution(Duration.ofSeconds(20))) {
            long id = cache.sync(cache.plan(topic, token), token, RecordStore::write);
            TestStack.sendString(topic, "k", "v1", null);
            SyncPlan plan = cache.plan(topic, token);
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void beforeCommit(SyncBatch batch) throws SQLException { pause(fetched, release); }
            });
            var normal = CompletableFuture.supplyAsync(() -> sync(cache, plan, token, RecordStore::write));
            assertTrue(fetched.await(10, TimeUnit.SECONDS));
            Process lock = child("lock", cache.path(), topic);
            try {
                assertEquals("LOCKED", line(lock));
                release.countDown();
                assertThrows(java.util.concurrent.TimeoutException.class, () -> normal.get(350, TimeUnit.MILLISECONDS));
                unlock(lock);
                normal.get(5, TimeUnit.SECONDS);
            } finally { lock.destroyForcibly(); }
            cache.hooks(new SyncWorker.Hooks() {});
            for (boolean deadline : List.of(true, false)) {
                TestStack.sendString(topic, "k", deadline ? "deadline" : "cancel", null);
                SyncPlan nextPlan = cache.plan(topic, token);
                Process holder = child("lock", cache.path(), topic);
                try {
                    assertEquals("LOCKED", line(holder));
                    try (var request = new Execution(deadline ? Duration.ofMillis(400) : Duration.ofSeconds(20))) {
                        CountDownLatch atCommit = new CountDownLatch(1);
                        cache.hooks(new SyncWorker.Hooks() {
                            @Override public void beforeCommit(SyncBatch b) { atCommit.countDown(); }
                        });
                        var waiting = CompletableFuture.supplyAsync(() -> sync(cache, nextPlan, request, RecordStore::write));
                        assertTrue(atCommit.await(5, TimeUnit.SECONDS));
                        if (!deadline) request.cancel();
                        var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> waiting.get(2, TimeUnit.SECONDS));
                        Throwable cause = failure.getCause();
                        if (cause instanceof java.util.concurrent.CompletionException) cause = cause.getCause();
                        assertInstanceOf(SQLException.class, cause);
                        if (deadline) assertInstanceOf(SQLTimeoutException.class, cause);
                        assertEquals(deadline ? "HYT00" : "57014", ((SQLException) cause).getSQLState());
                        unlock(holder);
                    }
                } finally { holder.destroyForcibly(); }
            }
            cache.hooks(new SyncWorker.Hooks() {});
            cache.sync(cache.plan(topic, token), token, RecordStore::write);
            assertEquals(List.of("4"), cache.read(c -> values(c, "SELECT count(*) FROM \"$t" + id + "\"")));
        } finally { release.countDown(); }
    }

    /** PLAN §10.1: "a batch from before a rebuild never lands, whatever the rebuild's reason";
     * "One replan per query ... whichever topic caused it ... A second time ... naming the topic".
     */
    @Test void aSecondProcessReplacesTheGenerationBetweenBatchesAndTheWholeQueryReplansOnce() throws Exception {
        String topic = TestStack.uniqueTopic("generation");
        String prefix = TestStack.uniqueTopic("generation-prefix");
        TestStack.createTopic(topic, 1, Map.of());
        TestStack.createTopic(prefix, 1, Map.of());
        TestStack.sendString(prefix, "k", "prefix", null);
        for (int i = 0; i < 7; i++) TestStack.sendString(topic, "old" + i, "old-generation", null);
        CountDownLatch committed = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger plans = new AtomicInteger();
        AtomicInteger prefixFetches = new AtomicInteger();
        try (var cache = CacheContext.acquire(config()); var token = new Execution(Duration.ofSeconds(30))) {
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void beforeFetch(SyncPlan p) {
                    if (p.topic().name().equals(prefix)) prefixFetches.incrementAndGet();
                }
                @Override public void afterCommit(SyncBatch b) throws SQLException {
                    if (b.plan().topic().name().equals(topic) && committed.getCount() != 0) pause(committed, release);
                }
            });
            var query = CompletableFuture.supplyAsync(() -> {
                try {
                    return token.run(false, () -> {
                        plans.incrementAndGet();
                        cache.sync(cache.plan(prefix, token), token, RecordStore::write);
                        return cache.sync(cache.plan(topic, token), token, RecordStore::write);
                    });
                } catch (SQLException e) { throw new java.util.concurrent.CompletionException(e); }
            });
            assertTrue(committed.await(10, TimeUnit.SECONDS));
            long old = cache.read(c -> Long.parseLong(values(c, "SELECT id FROM \"$topics\" WHERE topic='" + topic + "'").getFirst()));
            Process process = child("replace", cache.path(), topic);
            long replacement;
            try {
                String result = line(process);
                assertTrue(result.startsWith("REPLACED "), result);
                replacement = Long.parseLong(result.substring(9));
                assertTrue(process.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, process.exitValue());
            } finally { process.destroyForcibly(); }
            release.countDown();
            assertEquals(replacement, query.get(10, TimeUnit.SECONDS));
            assertNotEquals(old, replacement);
            assertEquals(2, plans.get());
            assertEquals(1, prefixFetches.get(), "the entire query replans while its first window stays cached");
            assertEquals(List.of("\"new-generation\""), cache.read(c -> values(c, "SELECT _value FROM \"$t" + replacement + "\"")));
            assertEquals(List.of("0|1"), cache.read(c -> values(c, "SELECT start_offset,end_offset FROM \"$ranges\" WHERE topic_ref=" + replacement)));
            assertEquals(List.of("1"), cache.read(c -> values(c, "SELECT caught_up_at_ms >= (SELECT _timestamp FROM \"$t" + replacement
                    + "\") FROM \"$partitions\" WHERE topic_ref=" + replacement)), "PLAN L797: progress belongs to the replacement generation");
            assertEquals(List.of("0"), cache.read(c -> values(c, "SELECT count(*) FROM sqlite_master WHERE name='$t" + old + "'")));
            SQLException second = assertThrows(SQLException.class, () -> token.run(false, () -> { throw new StaleGeneration(topic); }));
            assertTrue(second.getMessage().contains(topic));
            try (var offline = new Execution(Duration.ZERO)) {
                AtomicInteger attempts = new AtomicInteger();
                assertThrows(SQLException.class, () -> offline.run(true, () -> { attempts.incrementAndGet(); throw new StaleGeneration(topic); }));
                assertEquals(1, attempts.get());
            }
            // §10.1: a never-registered observation cannot resurrect a subsequently deleted generation.
            String absent = TestStack.uniqueTopic("never-registered");
            TestStack.createTopic(absent, 1, Map.of());
            TestStack.sendString(absent, "k", "deleted-generation", null);
            SyncPlan neverRegistered = cache.plan(absent, token);
            Process deletion = child("register-delete", cache.path(), absent);
            long deleted;
            try {
                String result = line(deletion); assertTrue(result.startsWith("DELETED "), result);
                deleted = Long.parseLong(result.substring(8));
                assertTrue(deletion.waitFor(10, TimeUnit.SECONDS)); assertEquals(0, deletion.exitValue());
            } finally { deletion.destroyForcibly(); }
            try (var request = new Execution(Duration.ofSeconds(10))) {
                AtomicInteger attempts = new AtomicInteger();
                SQLException unknown = assertThrows(SQLException.class, () -> request.run(false, () -> {
                    SyncPlan p = attempts.incrementAndGet() == 1 ? neverRegistered : cache.plan(absent, request);
                    return cache.sync(p, request, RecordStore::write);
                }));
                assertEquals(2, attempts.get()); assertTrue(unknown.getMessage().contains(absent));
                assertEquals(List.of(deleted + "|1"), cache.read(c -> values(c, "SELECT id,deleted_ms FROM \"$topics\" WHERE topic='" + absent + "'")));
                assertEquals(List.of("0"), cache.read(c -> values(c, "SELECT count(*) FROM \"$ranges\" WHERE topic_ref=" + deleted)));
            }
        } finally { release.countDown(); }
    }

    Process child(String mode, Path path, String topic) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                System.getProperty("java.class.path"), SyncProcess.class.getName(), mode, path.toString(), topic, TestStack.BOOTSTRAP)
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
    }

    static String line(Process child) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try { return new java.io.BufferedReader(child.inputReader()).readLine(); }
            catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
        }).get(15, TimeUnit.SECONDS);
    }

    static void unlock(Process child) throws Exception {
        child.getOutputStream().write('\n'); child.getOutputStream().flush();
        assertTrue(child.waitFor(5, TimeUnit.SECONDS)); assertEquals(0, child.exitValue());
    }

    static long sync(CacheContext.Lease cache, SyncPlan plan, Execution execution, BatchWriter writer) {
        try { return cache.sync(plan, execution, writer); }
        catch (java.sql.SQLException e) { throw new java.util.concurrent.CompletionException(e); }
    }

    static void pause(CountDownLatch entered, CountDownLatch release) throws java.sql.SQLException {
        entered.countDown();
        try { if (!release.await(20, TimeUnit.SECONDS)) throw new java.sql.SQLException("hook not released"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new java.sql.SQLException(e); }
    }

    static List<String> values(Connection c, String sql) throws java.sql.SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    if (i > 1) row.append('|');
                    row.append(rs.getString(i));
                }
                out.add(row.toString());
            }
        }
        return out;
    }
}
