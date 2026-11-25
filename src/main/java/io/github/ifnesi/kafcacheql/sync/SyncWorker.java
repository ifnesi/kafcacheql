package io.github.ifnesi.kafcacheql.sync;

import com.google.common.collect.Range;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.PartitionStore;
import io.github.ifnesi.kafcacheql.cache.RangeStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.kafka.ClientIds;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.schema.AvroJsonConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConnection;

/** One ordered writer/consumer owner per cache. Callers do all read-side planning independently. */
public final class SyncWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SyncWorker.class);
    private static final Duration POLL = Duration.ofMillis(100);

    /** Deterministic interleavings for integration tests; hooks run outside write transactions. */
    public interface Hooks {
        default void beforeFetch(SyncPlan plan) throws SQLException {}
        default void beforeCommit(SyncBatch batch) throws SQLException {}
        default void afterCommit(SyncBatch batch) throws SQLException {}
        default void afterPrune(SyncPlan plan, long generation, int partition, int deleted) throws SQLException {}
        default void beforeCheckpoint() throws SQLException {}
        default void afterCheckpoint(Checkpoint result) throws SQLException {}
    }

    /** SQLite's checkpoint result distinguishes contention from a complete checkpoint. */
    public record Checkpoint(int busy, int frames, int checkpointed) {
        public boolean contended() { return busy != 0 || checkpointed < frames; }
    }

    @FunctionalInterface interface Work<T> { T run() throws SQLException; }
    private static final class Job<T> {
        final Execution execution;
        final Work<T> work;
        final CompletableFuture<T> result = new CompletableFuture<>();
        Job(Execution execution, Work<T> work) { this.execution = execution; this.work = work; }
        void run() {
            try { execution.check(); result.complete(work.run()); }
            catch (SQLException | RuntimeException failure) { result.completeExceptionally(failure); }
            catch (Error failure) { result.completeExceptionally(failure); throw failure; }
        }
    }

    private sealed interface Background permits SchemaCopy, SubjectCopy, CaughtUp {
        Object key();
        long version();
        void write(Connection writer) throws SQLException;
    }
    private record SchemaCopy(SchemaStore.StoredSchema schema) implements Background {
        public Object key() { return List.of("schema", schema.ref()); }
        public long version() { return schema.fetchedMs(); }
        public void write(Connection writer) throws SQLException { SchemaStore.store(writer, schema); }
    }
    private record SubjectCopy(SchemaStore.SubjectSchema subject) implements Background {
        public Object key() { return List.of("subject", subject.subject()); }
        public long version() { return subject.version(); }
        public void write(Connection writer) throws SQLException { SchemaStore.store(writer, subject); }
    }
    private record CaughtUp(String topic, long generation, int partition, long capturedAt) implements Background {
        public Object key() { return List.of("caught-up", generation, partition); }
        public long version() { return capturedAt; }
        public void write(Connection writer) throws SQLException {
            var current = TopicRegistry.observe(writer, topic);
            if (current.isPresent() && current.get().id() == generation && !current.get().deleted()) {
                PartitionStore.caughtUp(writer, generation, partition, capturedAt);
            }
        }
    }

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    private final Map<Object, Background> background = new LinkedHashMap<>();
    private final Set<Job<?>> pending = new HashSet<>();
    private final Runnable backgroundTurn = this::backgroundTurn;
    private final Thread thread;
    private final DriverConfig config;
    private final ClientSettings settings;
    private final CompletableFuture<CacheStore> opened = new CompletableFuture<>();
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private volatile Hooks hooks = new Hooks() {};
    private boolean closing;
    private Job<?> active;
    private KafkaConsumer<byte[], byte[]> consumer;
    private String consumerId;
    private CacheStore cache;
    private BatchDecoder decoder;
    // Worker-confined; reset for each foreground sync and set only after a transaction commits.
    private boolean syncWrote;

    SyncWorker(Path path, Optional<String> clusterId, DriverConfig config, ClientSettings settings) {
        this.config = config;
        this.settings = settings;
        thread = Thread.ofPlatform().daemon().name("kafcacheql-sync-" + path.getFileName()).unstarted(() -> own(path, clusterId));
        thread.setContextClassLoader(DriverLoader.LOADER);
        thread.start();
    }

    CacheStore opened() throws SQLException {
        try {
            CacheStore store = opened.get();
            synchronized (this) { if (closing) throw new SQLException("Sync worker is closed"); }
            return store;
        }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException("Interrupted opening cache", e); }
        catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof SQLException sql) throw sql;
            throw failure("Opening sync worker", e.getCause());
        }
    }

    void hooks(Hooks hooks) { this.hooks = hooks; }

    private void own(Path path, Optional<String> clusterId) {
        SQLException failure = null;
        try (CacheStore store = CacheStore.open(path, clusterId); Connection reader = store.newReader();
             BatchDecoder decoding = new BatchDecoder(settings, reader, "kafcacheql-decode-" + path.getFileName())) {
            cache = store;
            decoder = decoding;
            try (Statement s = cache.writer().createStatement()) { s.execute("PRAGMA busy_timeout = 100"); }
            opened.complete(cache);
            while (true) {
                Runnable next = queue.take();
                synchronized (this) { if (closing) break; }
                next.run();
                next = null; // An idle worker must not retain the last caller's batch writer.
            }
        } catch (Throwable e) {
            failure = failure("Sync worker", e);
            opened.completeExceptionally(failure);
            synchronized (this) {
                closing = true;
                for (Job<?> job : pending) job.result.completeExceptionally(failure);
                pending.clear(); queue.clear(); background.clear();
            }
        } finally {
            try {
                if (consumer != null) consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
            } catch (RuntimeException e) {
                if (failure == null) failure = failure("Closing consumer", e); else failure.addSuppressed(e);
            } finally {
                if (consumerId != null) ClientIds.release(ClientIds.Kind.CONSUMER, consumerId);
                if (failure == null) stopped.complete(null); else stopped.completeExceptionally(failure);
            }
        }
    }

    @SuppressWarnings("try") // The subscription's lifetime is the side effect of the resource.
    <T> T submit(Execution execution, Work<T> work) throws SQLException {
        execution.check();
        Job<T> job = new Job<>(execution, work);
        Runnable queued = () -> {
            synchronized (this) { active = job; }
            try { job.run(); }
            finally { synchronized (this) { active = null; pending.remove(job); } }
        };
        try (var subscription = execution.onStop(() -> {
            synchronized (this) {
                if (queue.remove(queued)) {
                    pending.remove(job);
                    job.result.completeExceptionally(new SQLException("Query cancelled", "57014"));
                }
                if (active == job && consumer != null) consumer.wakeup();
            }
        })) {
            synchronized (this) {
                if (closing) throw new SQLException("Cache context is closed");
                pending.add(job);
                queue.add(queued);
            }
            // Covers cancellation between listener registration and enqueueing.
            try { execution.check(); }
            catch (SQLException e) {
                synchronized (this) { if (queue.remove(queued)) pending.remove(job); }
                throw e;
            }
            return execution.await(job.result);
        }
    }

    long sync(SyncPlan plan, Execution execution, BatchWriter writer, int batchSize) throws SQLException {
        return submit(execution, () -> {
            syncWrote = false;
            try { return fetch(plan, execution, writer, batchSize); }
            finally { if (syncWrote) checkpoint(); }
        });
    }

    private long fetch(SyncPlan plan, Execution execution, BatchWriter writer, int batchSize) throws SQLException {
        execution.check();
        long generation;
        var observed = TopicRegistry.observe(cache.writer(), plan.topic().name());
        if (plan.generation().isEmpty() || observed.isPresent() && observed.get().deleted()) {
            long before = changes();
            generation = TopicRegistry.register(cache, plan.generation(), plan.topic(), config.topic(plan.topic().name()),
                    AvroJsonConverter.VERSION, execution::check).orElseThrow(() -> new StaleGeneration(plan.topic().name())).id();
            syncWrote |= changes() != before;
        } else {
            generation = plan.generation().getAsLong();
            if (observed.isEmpty() || observed.get().id() != generation) throw new StaleGeneration(plan.topic().name());
        }
        if (observed.isPresent() && !observed.get().deleted() && observed.get().partitions() < plan.topic().partitions()) {
            transaction(plan, generation, execution, () -> {
                TopicRegistry.addPartitions(cache.writer(), generation, plan.topic().partitions());
                return null;
            });
        }
        if (config.cachePruneExpired() && plan.topic().mode() == Mode.APPEND
                && plan.logStarts().values().stream().anyMatch(start -> start > 0)) {
            prune(plan, generation, execution, batchSize);
        }
        for (var partition : plan.windows().entrySet()) {
            var missing = RangeStore.missing(cache.writer(), generation, partition.getKey(), partition.getValue());
            if (partition.getValue().isEmpty()) {
                long end = plan.ends().offsets().get(partition.getKey());
                commit(new SyncBatch(plan, generation, partition.getKey(), end, end, true, List.of()), execution, writer);
            } else if (missing.isEmpty() && plan.topic().mode() == Mode.COMPACTED) {
                caughtUp(plan.topic().name(), generation, partition.getKey(), plan.ends().capturedAtMs());
            }
            for (Range<Long> range : missing.asRanges()) {
                hooks.beforeFetch(plan);
                fetchRange(plan, generation, partition.getKey(), range, execution, writer, batchSize);
            }
        }
        return generation;
    }

    private void fetchRange(SyncPlan plan, long generation, int partition, Range<Long> range,
                            Execution execution, BatchWriter writer, int batchSize) throws SQLException {
        ensureConsumer();
        execution.check();
        TopicPartition tp = new TopicPartition(plan.topic().name(), partition);
        var formats = config.topic(tp.topic());
        Format key = formats.keyFormat().orElse(Format.AUTO), value = formats.valueFormat().orElse(Format.AUTO);
        consumer.assign(List.of(tp));
        consumer.seek(tp, range.lowerEndpoint());
        long start = range.lowerEndpoint();
        long end = range.upperEndpoint();
        long fetched = 0;
        List<SyncBatch.Record> batch = new ArrayList<>(Math.min(batchSize, 5000));
        try {
            while (start < end) {
                execution.check();
                decoder.awaitIdle(execution);
                var records = poll(execution);
                long position = Math.min(end, position(tp, execution));
                var decoded = records.isEmpty() ? List.<SyncBatch.Record>of()
                        : decoder.decode(tp.topic(), records, end, key, value, execution);
                for (var record : decoded) {
                    execution.check();
                    if (batch.size() == batchSize) {
                        long batchEnd = batch.getLast().offset() + 1;
                        commit(new SyncBatch(plan, generation, partition, start, batchEnd, false, batch), execution, writer);
                        start = batchEnd;
                        batch.clear();
                    }
                    batch.add(record);
                    fetched++;
                }
                if (position >= end) {
                    if (start < end) commit(new SyncBatch(plan, generation, partition, start, end,
                            end == plan.ends().offsets().get(partition), batch), execution, writer);
                    start = end;
                }
                log.debug("{} partition {}: {} records fetched, {} offsets remaining", tp.topic(), partition, fetched, end - position);
            }
        } catch (OffsetOutOfRangeException expired) {
            // The read-side validity planner owns §9's append-prune/compacted-rebuild decision.
            throw new StaleGeneration(tp.topic());
        } catch (RuntimeException failure) {
            throw failure("Fetching topic " + tp.topic(), failure);
        }
    }

    /** Lazy creation also lets an online lease join a context first opened by an offline lease. */
    private void ensureConsumer() throws SQLException {
        if (consumer != null) return;
        String id = ClientIds.reserve(ClientIds.Kind.CONSUMER, config.clientId());
        Map<String, Object> properties = new HashMap<>(settings.consumer());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
        try {
            KafkaConsumer<byte[], byte[]> created = new KafkaConsumer<>(properties);
            synchronized (this) { consumer = created; consumerId = id; }
        } catch (RuntimeException e) {
            ClientIds.release(ClientIds.Kind.CONSUMER, id);
            throw failure("Creating cache consumer", e);
        }
    }

    private ConsumerRecords<byte[], byte[]> poll(Execution execution) throws SQLException {
        while (true) {
            execution.check();
            try { return consumer.poll(execution.remaining(POLL)); }
            catch (WakeupException wakeup) { execution.check(); }
        }
    }

    private long position(TopicPartition partition, Execution execution) throws SQLException {
        while (true) {
            execution.check();
            try { return consumer.position(partition, execution.remaining(POLL)); }
            // Preserve the records already returned by poll if a late wakeup reaches position instead.
            catch (WakeupException wakeup) { execution.check(); }
        }
    }

    private void commit(SyncBatch batch, Execution execution, BatchWriter writer) throws SQLException {
        hooks.beforeCommit(batch);
        transaction(batch.plan(), batch.generation(), execution, () -> {
            writer.write(cache.writer(), batch);
            return null;
        });
        hooks.afterCommit(batch);
    }

    private void prune(SyncPlan plan, long generation, Execution execution, int batchSize) throws SQLException {
        boolean needed = transaction(plan, generation, execution, () -> {
            if (!PartitionStore.needsPruning(cache.writer(), generation, plan.logStarts())) return false;
            PartitionStore.raiseFloors(cache.writer(), generation, plan.logStarts());
            return true;
        });
        if (!needed) return;
        for (int partition : plan.logStarts().keySet()) {
            int deleted;
            do {
                deleted = transaction(plan, generation, execution,
                        () -> PartitionStore.deleteChunk(cache.writer(), generation, partition, batchSize));
                hooks.afterPrune(plan, generation, partition, deleted);
            } while (deleted == batchSize);
        }
    }

    private <T> T transaction(SyncPlan plan, long generation, Execution execution, CacheStore.Write<T> work)
            throws SQLException {
        long before = changes();
        T result = CacheStore.writeTransaction(cache.writer(), execution::check, () -> {
            requireGeneration(cache.writer(), plan.topic().name(), generation);
            return work.run();
        });
        syncWrote |= changes() != before;
        return result;
    }

    private long changes() throws SQLException {
        return ((SQLiteConnection) cache.writer()).getDatabase().total_changes();
    }

    /** Best effort after writing syncs, including a cancelled sync's already committed batches. */
    private void checkpoint() {
        try {
            Path wal = Path.of(cache.path() + "-wal");
            if (!Files.exists(wal)) return;
            long size = Files.size(wal);
            if (size <= CacheStore.JOURNAL_SIZE_LIMIT) return;
            hooks.beforeCheckpoint();
            Checkpoint result;
            // The worker's busy_timeout is 100 ms; this pragma runs outside any write transaction.
            try (Statement s = cache.writer().createStatement(); var rows = s.executeQuery("PRAGMA wal_checkpoint(TRUNCATE)")) {
                if (!rows.next()) throw new SQLException("WAL checkpoint returned no result");
                result = new Checkpoint(rows.getInt(1), rows.getInt(2), rows.getInt(3));
            }
            if (result.contended()) {
                if (size > 1024L * 1024 * 1024) {
                    log.warn("WAL checkpoint contention for {}: {} frames, {} checkpointed, busy={}",
                            cache.path(), result.frames(), result.checkpointed(), result.busy());
                } else {
                    log.debug("WAL checkpoint contention for {}: {} frames, {} checkpointed, busy={}",
                            cache.path(), result.frames(), result.checkpointed(), result.busy());
                }
            }
            hooks.afterCheckpoint(result);
        } catch (SQLException | IOException e) {
            log.warn("WAL checkpoint failed: {}", failure("Checkpoint", e).getMessage());
        }
    }

    static void requireGeneration(Connection reader, String topic, long generation) throws SQLException {
        var current = TopicRegistry.observe(reader, topic);
        if (current.isEmpty() || current.get().id() != generation || current.get().deleted()) throw new StaleGeneration(topic);
    }

    void schema(SchemaStore.StoredSchema schema) { background(new SchemaCopy(schema)); }
    void subject(SchemaStore.SubjectSchema subject) { background(new SubjectCopy(subject)); }
    void caughtUp(String topic, long generation, int partition, long capturedAt) {
        background(new CaughtUp(topic, generation, partition, capturedAt));
    }

    private synchronized void background(Background write) {
        if (closing) return;
        Object key = write.key();
        Background pending = background.get(key);
        if (pending != null && pending.version() >= write.version()) return;
        if (pending == null && background.size() >= 1000) return;
        boolean empty = background.isEmpty();
        background.put(key, write);
        if (empty) queue.add(backgroundTurn);
    }

    synchronized int pendingBackground() { return background.size(); }
    synchronized int queuedRequests() { return pending.size() - (active == null ? 0 : 1); }

    private void backgroundTurn() {
        Background write;
        synchronized (this) {
            if (background.isEmpty() || closing) return;
            var iterator = background.values().iterator();
            write = iterator.next();
            iterator.remove();
            if (!background.isEmpty()) queue.add(backgroundTurn);
        }
        // Background copies are disposable; a busy lock or any other failure is logged and dropped.
        try {
            CacheStore.writeTransaction(cache.writer(), () -> { write.write(cache.writer()); return null; });
        } catch (SQLException | RuntimeException e) { log.warn("Dropped background cache write: {}", failure("Background write", e).getMessage()); }
    }

    private SQLException failure(String what, Throwable cause) {
        return new SQLException(Secrets.redact(what + ": " + cause.getMessage(), settings.schemaRegistry()), "08006",
                Secrets.sanitize(cause, settings.schemaRegistry()));
    }

    @Override public void close() throws SQLException {
        Job<?> current;
        synchronized (this) {
            closing = true;
            background.clear();
            current = active;
            for (Job<?> job : pending) job.result.completeExceptionally(new SQLException("Cache context is closed"));
            pending.clear();
            queue.clear();
            queue.add(() -> {});
        }
        if (current != null) current.execution.cancel();
        boolean interrupted = false;
        while (true) {
            try { thread.join(); break; }
            catch (InterruptedException e) { interrupted = true; }
        }
        if (interrupted) Thread.currentThread().interrupt();
        try { stopped.join(); }
        catch (java.util.concurrent.CompletionException e) { throw failure("Closing sync worker", e.getCause()); }
    }
}
