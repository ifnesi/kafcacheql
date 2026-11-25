package io.github.ifnesi.kafcacheql.sync;

import static io.github.ifnesi.kafcacheql.sync.SyncWorkerIT.values;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Whole Kafka -> sync -> cache-file flows; the driver's JDBC statements belong to 0011b. */
class BatchCommitIT {
    @TempDir Path dir;
    static final BatchWriter WRITER = io.github.ifnesi.kafcacheql.cache.RecordStore::write;

    @BeforeAll static void stack() { TestStack.requireUp(); }

    DriverConfig config(Path file, String... settings) throws Exception {
        Properties p = new Properties();
        p.setProperty("cache.path", file.toString());
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        p.setProperty("sync.batch.records", "2");
        for (int i = 0; i < settings.length; i += 2) p.setProperty(settings[i], settings[i + 1]);
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }

    /** PLAN L584–591: "Each batch is one transaction" including rows, ranges and schemas;
     * L405: append tombstones are "Stored as a row with _value NULL"; L477: schemas stored "for good";
     * L576: "Partial or cancelled syncs don't advance it." L379–388 specifies all record columns.
     */
    @Test void appendBatchesRollBackTogetherThenPersistRecordsSchemasAndTheCapturedTime() throws Exception {
        String topic = TestStack.uniqueTopic("batch-atomic");
        TestStack.createTopic(topic, 1, Map.of());
        Path file = dir.resolve("atomic.db");
        long id;
        try (var cache = CacheContext.acquire(config(file, "sync.batch.records", "10"));
             var execution = new Execution(Duration.ofSeconds(30))) {
            id = cache.sync(cache.plan(topic, execution), execution, WRITER);
            var before = cache.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\""));
            var schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"BatchValue\",\"fields\":[{\"name\":\"n\",\"type\":\"int\"}]}");
            var body = new GenericData.Record(schema); body.put("n", 7);
            var headers = new RecordHeaders().add("tag", new byte[] {0, (byte) 255}).add("tag", null);
            byte[] value = TestStack.serializeAvro(topic, headers, body, TestStack.SchemaIdIn.PREFIX, false);
            long timestamp = System.currentTimeMillis();
            TestStack.send(topic, 0, timestamp, new byte[] {1, 2}, value, headers);
            TestStack.sendTombstone(topic, "gone", null);
            TestStack.sendJson(topic, null, "null", null);
            SyncPlan plan = cache.plan(topic, execution);
            SQLException failure = assertThrows(SQLException.class, () -> cache.sync(plan, execution, (c, b) -> {
                WRITER.write(c, b);
                throw new SQLException("planted failure after all batch writes");
            }));
            assertTrue(failure.getMessage().contains("planted failure"));
            assertEquals(List.of("0"), cache.read(c -> values(c, "SELECT count(*) FROM \"$t" + id + "\"")));
            assertEquals(List.of("0"), cache.read(c -> values(c, "SELECT count(*) FROM \"$ranges\"")));
            assertEquals(List.of("0"), cache.read(c -> values(c, "SELECT count(*) FROM \"$schemas\"")));
            assertEquals(before, cache.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\"")));
            cache.sync(plan, execution, WRITER);
            // PLAN L451–453: {1,2} is valid UTF-8, so auto decodes a JSON string, not base64.
            assertEquals(List.of("0|0|" + timestamp + "|0102|\"\\u0001\\u0002\"|{\"n\":7}|null"), cache.read(c -> values(c,
                    "SELECT _partition,_offset,_timestamp,hex(_key_raw),_key,_value,_decode_error FROM \"$t" + id + "\" WHERE _offset=0")));
            assertEquals(List.of("1|\"gone\"|null", "2|null|null"), cache.read(c -> values(c,
                    "SELECT _offset,_key,_value FROM \"$t" + id + "\" WHERE _offset>0 ORDER BY _offset")));
            assertEquals(List.of("1|1", "2|0"), cache.read(c -> values(c,
                    "SELECT _offset,_value IS NULL FROM \"$t" + id + "\" WHERE _offset>0 ORDER BY _offset")));
            assertEquals(List.of("1"), cache.read(c -> values(c,
                    "SELECT count(*) FROM \"$schemas\" s JOIN \"$t" + id + "\" t ON s.ref=t._value_schema_id WHERE s.type='AVRO'")));
            assertEquals(List.of("2|tag|AP8=|tag|null"), cache.read(c -> values(c,
                    "SELECT json_array_length(_headers),json_extract(_headers,'$[0].key'),json_extract(_headers,'$[0].value'),"
                            + "json_extract(_headers,'$[1].key'),json_extract(_headers,'$[1].value') FROM \"$t" + id + "\" WHERE _offset=0")));
            assertEquals(List.of("0|3"), cache.read(c -> values(c, "SELECT start_offset,end_offset FROM \"$ranges\"")));
            assertEquals(List.of(Long.toString(plan.ends().capturedAtMs())), cache.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\"")));
        }
        try (var reopened = CacheStore.open(file, java.util.Optional.empty()); var reader = reopened.newReader()) {
            assertEquals(List.of("3"), values(reader, "SELECT count(*) FROM \"" + topic + "\""));
            assertEquals(List.of("1"), values(reader, "SELECT count(*) FROM \"$schemas\""));
        }
    }

    /** PLAN L628–630: batches re-check ranges under the lock; "each offset is stored once";
     * L594: "Compacted writes never go backwards"; L479: "The same logical key under two schema IDs
     * is two keys"; L576: caught_up_at_ms is the capture time, not the commit time.
     */
    @Test void competingProcessesKeepAppendOffsetsAndCompactedTombstonesAndRawKeyIdentity() throws Exception {
        Path file = dir.resolve("overlap.db");
        try (var cache = CacheContext.acquire(config(file)); var execution = new Execution(Duration.ofSeconds(60))) {
            for (String policy : List.of("delete", "compact")) {
                String topic = TestStack.uniqueTopic("batch-overlap");
                TestStack.createTopic(topic, 1, Map.of("cleanup.policy", policy));
                // Two Avro schemas encode the same logical key, with distinct wire-prefix IDs.
                var key1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"KeyOne\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
                var key2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"KeyTwo\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
                byte[] a, b;
                try (var registry = new io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient(TestStack.REGISTRY, 10)) {
                    registry.updateCompatibility(topic + "-key", "NONE");
                    var row = new GenericData.Record(key1); row.put("id", 1);
                    a = TestStack.serializeAvro(topic, new RecordHeaders(), row, TestStack.SchemaIdIn.PREFIX, true);
                    row = new GenericData.Record(key2); row.put("id", 1);
                    b = TestStack.serializeAvro(topic, new RecordHeaders(), row, TestStack.SchemaIdIn.PREFIX, true);
                }
                assertNotEquals(java.util.HexFormat.of().formatHex(a), java.util.HexFormat.of().formatHex(b));
                TestStack.sendBytes(topic, a, TestStack.utf8("old-a"), null);
                TestStack.sendBytes(topic, b, TestStack.utf8("old-b"), null);
                Process stale = child("buffer", file, topic);
                try {
                    assertEquals("BUFFERED", SyncWorkerIT.line(stale));
                    TestStack.sendBytes(topic, a, null, null);
                    TestStack.sendBytes(topic, b, TestStack.utf8("new-b"), null);
                    SyncPlan plan = cache.plan(topic, execution);
                    long id = cache.sync(plan, execution, WRITER);
                    var expected = cache.read(c -> values(c, "SELECT _offset,_value FROM \"$t" + id + "\" ORDER BY _offset"));
                    assertEquals(policy.equals("compact") ? List.of("3|\"new-b\"")
                            : List.of("0|\"old-a\"", "1|\"old-b\"", "2|null", "3|\"new-b\""), expected);
                    SyncWorkerIT.unlock(stale);
                    assertEquals(expected, cache.read(c -> values(c, "SELECT _offset,_value FROM \"$t" + id + "\" ORDER BY _offset")));
                    assertEquals(List.of("0|4"), cache.read(c -> values(c, "SELECT start_offset,end_offset FROM \"$ranges\" WHERE topic_ref=" + id)));
                    assertEquals(List.of(Long.toString(plan.ends().capturedAtMs())), cache.read(c -> values(c, "SELECT caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=" + id)));
                    // Restore a with its distinct raw key; both logical keys coexist.
                    TestStack.sendBytes(topic, a, TestStack.utf8("restored-a"), null);
                    cache.sync(cache.plan(topic, execution), execution, WRITER);
                    assertEquals(List.of(policy.equals("compact") ? "2|1|2" : "5|1|2"), cache.read(c -> values(c,
                            "SELECT count(*),count(DISTINCT _key),count(DISTINCT _key_raw) FROM \"$t" + id + "\"")));
                } finally { stale.destroyForcibly(); }
            }
        }
    }

    /** PLAN §6.5: "one row per key per partition" and a tombstone "deletes only its own partition's
     * row"; §9: a partition increase picks up new partitions for compacted topics without a rebuild.
     */
    @Test void compactedKeysAndTombstonesStayInTheirPartitionsWhenTheTopicGrows() throws Exception {
        String topic = TestStack.uniqueTopic("partition-keys");
        TestStack.createTopic(topic, 2, Map.of("cleanup.policy", "compact"));
        byte[] key = TestStack.utf8("shared");
        TestStack.send(topic, 0, null, TestStack.utf8("filler"), TestStack.utf8("first"), null);
        TestStack.send(topic, 0, null, TestStack.utf8("filler"), TestStack.utf8("second"), null);
        TestStack.send(topic, 0, null, key, TestStack.utf8("p0"), null);
        TestStack.send(topic, 1, null, key, TestStack.utf8("p1"), null);
        Path file = dir.resolve("partition-keys.db");
        try (var cache = CacheContext.acquire(config(file)); var execution = new Execution(Duration.ofSeconds(30))) {
            long id = cache.sync(cache.plan(topic, execution), execution);
            String shared = "SELECT _partition,_offset,_value FROM \"$t" + id + "\" WHERE _key='\"shared\"' ORDER BY _partition";
            assertEquals(List.of("0|2|\"p0\"", "1|0|\"p1\""), cache.read(c -> values(c, shared)));
            TestStack.send(topic, 0, null, key, null, null);
            assertEquals(id, cache.sync(cache.plan(topic, execution), execution));
            assertEquals(List.of("1|0|\"p1\""), cache.read(c -> values(c, shared)));
            TestStack.addPartitions(topic, 3);
            TestStack.send(topic, 2, null, key, TestStack.utf8("p2"), null);
            assertEquals(id, cache.sync(cache.plan(topic, execution), execution), "adding partitions keeps the generation");
            assertEquals(List.of("1|0|\"p1\"", "2|0|\"p2\""), cache.read(c -> values(c, shared)));
            assertEquals(List.of("3"), cache.read(c -> values(c, "SELECT partition_count FROM \"$topics\" WHERE id=" + id)));
            assertEquals(List.of("0", "1", "2"), cache.read(c -> values(c,
                    "SELECT partition_no FROM \"$partitions\" WHERE topic_ref=" + id + " ORDER BY partition_no")));
        }
        try (var cache = CacheStore.open(file, java.util.Optional.empty()); var reader = cache.newReader()) {
            assertEquals(List.of("1|\"p1\"", "2|\"p2\""), values(reader,
                    "SELECT _partition,_value FROM \"" + topic + "\" WHERE _key='\"shared\"' ORDER BY _partition"));
        }
    }

    /** PLAN §10: pruning is always on; "each sync raises each partition's pruning floor ... and trims
     * $ranges ... in one transaction"; rows deleted "in chunks of sync.batch.records".
     * §10.1: "A batch fetched before a prune can't bring pruned records back."
     */
    @Test void retentionPrunesCachedWindowsAndAnOlderProcessesBufferCannotRestoreThem() throws Exception {
        String topic = TestStack.uniqueTopic("batch-retention");
        TestStack.createTopic(topic, 1, Map.of("retention.ms", "-1", "segment.ms", "1000", "file.delete.delay.ms", "0"));
        Path file = dir.resolve("prune.db");
        for (int i = 0; i < 7; i++) TestStack.sendString(topic, "k" + i, "old" + i, TestStack.recent(i));
        TestStack.sendString(topic, "tail", "kept", System.currentTimeMillis() + 120_000);
        try (var cache = CacheContext.acquire(config(file));
             var execution = new Execution(Duration.ofSeconds(60))) {
            Process stale = child("buffer", file, topic);
            try {
                assertEquals("BUFFERED", SyncWorkerIT.line(stale));
                long id = cache.sync(cache.plan(topic, execution), execution, WRITER);
                var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                TestStack.admin().incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                        new ConfigEntry("retention.ms", "1000"), AlterConfigOp.OpType.SET)))).all().get(10, TimeUnit.SECONDS);
                TestStack.await(() -> {
                    try { return TestStack.admin().listOffsets(Map.of(new TopicPartition(topic, 0), OffsetSpec.earliest()),
                            new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).all().get(10, TimeUnit.SECONDS)
                            .get(new TopicPartition(topic, 0)).offset() == 7; }
                    catch (Exception e) { throw new IllegalStateException(e); }
                }, "Kafka retention removes the old segment");
                var wanted = Map.of(0, com.google.common.collect.ImmutableRangeSet.of(com.google.common.collect.Range.closedOpen(0L, 8L)));
                SyncPlan expired = cache.plan(topic, wanted, execution);
                assertEquals(1, expired.warnings().size());
                assertTrue(expired.warnings().getFirst().getMessage().contains(topic));
                assertTrue(expired.warnings().getFirst().getMessage().contains("expired"));
                CountDownLatch chunk = new CountDownLatch(1), release = new CountDownLatch(1);
                List<Integer> chunks = new ArrayList<>();
                cache.hooks(new SyncWorker.Hooks() {
                    @Override public void afterPrune(SyncPlan p, long generation, int partition, int deleted) throws SQLException {
                        chunks.add(deleted);
                        if (chunk.getCount() != 0) SyncWorkerIT.pause(chunk, release);
                    }
                });
                try (var cancelled = new Execution(Duration.ofSeconds(20))) {
                    var pruning = CompletableFuture.supplyAsync(() -> SyncWorkerIT.sync(cache, expired, cancelled, WRITER));
                    try {
                        assertTrue(chunk.await(10, TimeUnit.SECONDS));
                        assertEquals(List.of("7"), cache.read(c -> values(c, "SELECT pruned_below FROM \"$partitions\"")));
                        assertEquals(List.of("7|8"), cache.read(c -> values(c, "SELECT start_offset,end_offset FROM \"$ranges\"")));
                        assertEquals(List.of("5"), cache.read(c -> values(c, "SELECT count(*) FROM \"$t" + id + "\" WHERE _offset<7")));
                        Process lock = child("lock", file, topic);
                        try {
                            assertEquals("LOCKED", SyncWorkerIT.line(lock));
                            SyncWorkerIT.unlock(lock);
                        } finally { lock.destroyForcibly(); }
                        cancelled.cancel();
                        SyncWorkerIT.assertCancelled(pruning);
                    } finally { release.countDown(); }
                }
                cache.sync(cache.plan(topic, execution), execution);
                assertEquals(List.of(2, 2, 2, 1), chunks, "each deletion commits at most sync.batch.records, resuming after cancellation");
                assertEquals(List.of("7|\"kept\""), cache.read(c -> values(c, "SELECT _offset,_value FROM \"$t" + id + "\"")));
                assertEquals(List.of("7|8"), cache.read(c -> values(c, "SELECT start_offset,end_offset FROM \"$ranges\"")));
                assertEquals(List.of("7"), cache.read(c -> values(c, "SELECT pruned_below FROM \"$partitions\"")));
                SyncWorkerIT.unlock(stale);
                assertEquals(List.of("7"), cache.read(c -> values(c, "SELECT _offset FROM \"$t" + id + "\"")));
            } finally { stale.destroyForcibly(); }
        }
    }

    /** PLAN L642: the writer sets journal_size_limit (64 MiB); an open snapshot prevents checkpoint
     * completion; after "a sync that wrote" the worker tries TRUNCATE; unused space may stay until
     * "the next sync that writes". This includes one-batch syncs.
     */
    @Test void aHeldSnapshotPinsTheWalUntilAnotherWritingSyncAfterItsRelease() throws Exception {
        String topic = TestStack.uniqueTopic("batch-wal");
        TestStack.createTopic(topic, 1, Map.of());
        Path file = dir.resolve("wal.db"), wal = Path.of(file + "-wal");
        try (var cache = CacheContext.acquire(config(file, "sync.batch.records", "5000"));
             var execution = new Execution(Duration.ofSeconds(90))) {
            TestStack.sendString(topic, "initial", "initial", null);
            TestStack.sendString(topic, "second", "second", null);
            long id = cache.sync(cache.plan(topic, execution), execution, WRITER);
            List<SyncWorker.Checkpoint> checkpoints = new ArrayList<>();
            List<Integer> batches = new ArrayList<>();
            cache.hooks(new SyncWorker.Hooks() {
                @Override public void afterCommit(SyncBatch batch) { batches.add(batch.records().size()); }
                @Override public void afterCheckpoint(SyncWorker.Checkpoint result) { checkpoints.add(result); }
            });
            try (var pinnedCache = CacheStore.open(file, java.util.Optional.empty()); var reader = pinnedCache.newReader();
                 var snapshot = reader.createStatement()) {
                snapshot.execute("BEGIN");
                byte[] large = TestStack.utf8("x".repeat(600_000));
                try (var cursor = reader.createStatement();
                     var rows = cursor.executeQuery("SELECT _value FROM \"$t" + id + "\" ORDER BY _offset")) {
                    assertTrue(rows.next()); // Leave this cursor open, with its second row still unread.
                    for (int i = 0; i < 120; i++) TestStack.sendBytes(topic, TestStack.utf8("large" + i), large, null);
                    cache.sync(cache.plan(topic, execution), execution, WRITER);
                    assertEquals(List.of(120), batches, "a one-batch sync still checkpoints");
                    assertTrue(Files.size(wal) > 64L * 1024 * 1024);
                    assertEquals("\"initial\"", rows.getString(1));
                    assertEquals(List.of("2"), values(reader, "SELECT count(*) FROM \"$t" + id + "\""));
                    assertEquals(List.of("67108864"), cache.write(topic, id, execution, c -> values(c, "PRAGMA journal_size_limit")));
                    assertEquals(1, checkpoints.size());
                    assertTrue(checkpoints.getFirst().contended());
                    assertTrue(checkpoints.getFirst().checkpointed() < checkpoints.getFirst().frames(), "open result set stops checkpoint progress");
                }
                snapshot.execute("COMMIT");
                long size = Files.size(wal);
                cache.sync(cache.plan(topic, execution), execution, WRITER);
                assertEquals(size, Files.size(wal), "a sync that writes nothing does not checkpoint");
                assertEquals(1, checkpoints.size());
                TestStack.sendString(topic, "last", "last", null);
                cache.sync(cache.plan(topic, execution), execution, WRITER);
                assertEquals(2, checkpoints.size());
                assertTrue(!checkpoints.getLast().contended());
                assertEquals(0, Files.size(wal), "the worker's checkpoint after a one-batch writing sync truncates the WAL");
                assertEquals(List.of("123"), cache.read(c -> values(c, "SELECT count(*) FROM \"$t" + id + "\"")));
                // Isolate writer contention: release the snapshot before the worker checkpoints,
                // then let a real second process hold the write lock instead.
                CountDownLatch ready = new CountDownLatch(1), release = new CountDownLatch(1);
                cache.hooks(new SyncWorker.Hooks() {
                    @Override public void beforeCheckpoint() throws SQLException { SyncWorkerIT.pause(ready, release); }
                    @Override public void afterCheckpoint(SyncWorker.Checkpoint result) { checkpoints.add(result); }
                });
                Process holder = null;
                try {
                    snapshot.execute("BEGIN");
                    CompletableFuture<Long> sync;
                    try (var cursor = reader.createStatement();
                         var rows = cursor.executeQuery("SELECT _value FROM \"$t" + id + "\"")) {
                        assertTrue(rows.next());
                        for (int i = 0; i < 120; i++) TestStack.sendBytes(topic, TestStack.utf8("more" + i), large, null);
                        SyncPlan next = cache.plan(topic, execution);
                        sync = CompletableFuture.supplyAsync(() -> SyncWorkerIT.sync(cache, next, execution, WRITER));
                        assertTrue(ready.await(10, TimeUnit.SECONDS));
                    }
                    snapshot.execute("COMMIT");
                    holder = child("lock", file, topic);
                    assertEquals("LOCKED", SyncWorkerIT.line(holder));
                    release.countDown();
                    sync.get(5, TimeUnit.SECONDS);
                    assertEquals(3, checkpoints.size());
                    assertTrue(checkpoints.getLast().contended(), "another process's writer is checkpoint contention");
                    assertTrue(Files.size(wal) > 64L * 1024 * 1024);
                    SyncWorkerIT.unlock(holder);
                } finally {
                    release.countDown();
                    if (holder != null) holder.destroyForcibly();
                }
            }
        }
    }

    Process child(String mode, Path path, String topic) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                System.getProperty("java.class.path"), BatchProcess.class.getName(), mode, path.toString(), topic)
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
    }
}
