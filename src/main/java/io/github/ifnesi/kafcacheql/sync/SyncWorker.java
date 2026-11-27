package io.github.ifnesi.kafcacheql.sync;

import io.github.ifnesi.kafcacheql.transform.RecordTransforms;
import com.google.common.collect.Range;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.EvictionStore;
import io.github.ifnesi.kafcacheql.cache.PartitionStore;
import io.github.ifnesi.kafcacheql.cache.RangeStore;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.config.Secrets;
import io.github.ifnesi.kafcacheql.kafka.ClientIds;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.schema.SchemaRegistry;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

/** One ordered writer, fed by consumer and decoder owner threads. Callers plan reads independently. */
public final class SyncWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SyncWorker.class);
    private static final Duration POLL = Duration.ofMillis(100);
    private static final long PROGRESS_INTERVAL_NANOS = Duration.ofSeconds(5).toNanos();

    /** Deterministic interleavings for integration tests; hooks run outside write transactions. */
    public interface Hooks {
        default long nanoTime() { return System.nanoTime(); }
        default void beforePrepare(SyncPlan plan) throws SQLException {}
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
    @FunctionalInterface private interface PartitionFetch { void run(FetchConsumer consumer) throws SQLException; }
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

    private record Background(Object key, long version, Work<Void> work) {}

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    private final Map<Object, Background> background = new LinkedHashMap<>();
    private final Set<Job<?>> pending = new HashSet<>();
    private final Runnable backgroundTurn = this::backgroundTurn;
    private final Thread thread;
    private final DriverConfig config;
    private final ClientSettings settings;
    private final RecordTransforms transforms;
    private final CompletableFuture<CacheStore> opened = new CompletableFuture<>();
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private volatile Hooks hooks = new Hooks() {};
    private boolean closing;
    private SQLException terminalFailure; // Guarded by this; retained after the worker stops.
    private Job<?> active;
    private final Set<KafkaConsumer<byte[], byte[]>> consumers = ConcurrentHashMap.newKeySet();
    private final List<FetchConsumer> fetchers = new ArrayList<>(); // Worker-confined; each has its own owner.
    private CacheStore cache;
    private final Object registryLock = new Object();
    private SchemaRegistry registry;
    private CompletableFuture<Void> fetchIdle = CompletableFuture.completedFuture(null);
    // Worker-confined; reset for each foreground sync and set only after a transaction commits.
    private boolean syncWrote;

    SyncWorker(Path path, Optional<String> clusterId, DriverConfig config, ClientSettings settings, RecordTransforms transforms) {
        this.transforms = transforms;
        this.config = config;
        this.settings = settings;
        thread = Thread.ofPlatform().daemon().name("kafcacheql-sync-" + path.getFileName()).unstarted(() -> own(path, clusterId));
        thread.setContextClassLoader(DriverLoader.LOADER);
        thread.start();
    }

    CacheStore opened() throws SQLException {
        try {
            CacheStore store = opened.get();
            checkOpen();
            return store;
        }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException("Interrupted opening cache", e); }
        catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof SQLException sql) throw sql;
            throw failure("Opening sync worker", e.getCause());
        }
    }

    synchronized void checkOpen() throws SQLException {
        if (terminalFailure != null) {
            throw new SQLException(terminalFailure.getMessage(), terminalFailure.getSQLState(), terminalFailure);
        }
        if (closing) throw new SQLException("Cache context is closed");
    }

    void hooks(Hooks hooks) { this.hooks = hooks; }

    private void own(Path path, Optional<String> clusterId) {
        SQLException failure = null;
        try {
            cache = CacheStore.open(path, clusterId, reader -> { if (config.offline()) transforms.validateOffline(reader); });
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
                terminalFailure = failure;
                closing = true;
                for (Job<?> job : pending) job.result.completeExceptionally(failure);
                pending.clear(); queue.clear(); background.clear();
            }
        } finally {
            for (var fetcher : fetchers) {
                try { fetcher.close(); }
                catch (SQLException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            }
            try { if (registry != null) registry.close(); }
            catch (SQLException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            try { if (cache != null) cache.close(); }
            catch (SQLException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            if (failure == null) stopped.complete(null); else stopped.completeExceptionally(failure);
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
                if (active == job) wakeConsumers();
            }
        })) {
            synchronized (this) {
                checkOpen();
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

    long sync(SyncPlan plan, Execution execution, BatchWriter writer, int batchSize, int parallelism) throws SQLException {
        return submit(execution, () -> {
            syncWrote = false;
            try { return fetch(plan, execution, writer, batchSize, parallelism); }
            finally { if (syncWrote) checkpoint(); }
        });
    }

    void delete(String topic, java.util.OptionalLong generation, Execution execution) throws SQLException {
        submit(execution, () -> {
            if (!TopicRegistry.delete(cache, topic, generation, execution::check)) throw new StaleGeneration(topic);
            return null;
        });
    }

    private long fetch(SyncPlan plan, Execution execution, BatchWriter writer, int batchSize, int parallelism) throws SQLException {
        execution.check();
        var observed = TopicRegistry.observe(cache.writer(), plan.topic().name());
        if (plan.generation().isPresent() && (observed.isEmpty() || observed.get().id() != plan.generation().getAsLong())) {
            throw new StaleGeneration(plan.topic().name());
        }
        long generation;
        if (plan.generation().isEmpty() || observed.orElseThrow().deleted()
                || observed.get().partitions() < plan.topic().partitions()
                || TopicRegistry.rebuildReason(cache.writer(), observed.get().id(), plan.topic(), transforms.settings(plan.topic().name(), config.topic(plan.topic().name())),
                        plan.logStarts()).isPresent()) {
            hooks.beforePrepare(plan);
            long before = changes();
            generation = TopicRegistry.prepare(cache, plan.generation(), plan.deleted(), plan.topic(), transforms.settings(plan.topic().name(), config.topic(plan.topic().name())),
                    plan.logStarts(), execution::check)
                    .orElseThrow(() -> new StaleGeneration(plan.topic().name())).id();
            syncWrote |= changes() != before;
        } else generation = plan.generation().getAsLong();
        Map<Integer, Long> baselines = Map.of();
        if (plan.topic().mode() == Mode.COMPACTED) {
            baselines = PartitionStore.safetyBaselines(cache.writer(), generation);
            requireGeneration(cache.writer(), plan.topic().name(), generation);
        }
        if (plan.topic().mode() == Mode.APPEND
                && plan.logStarts().values().stream().anyMatch(start -> start > 0)) {
            prune(plan, generation, execution, batchSize);
        }
        var partitions = new ArrayList<PartitionFetch>();
        var request = new FetchRequest(execution);
        for (var partition : plan.windows().entrySet()) {
            long baseline = plan.topic().mode() == Mode.COMPACTED ? baselines.get(partition.getKey()) : 0;
            var missing = RangeStore.missing(cache.writer(), generation, partition.getKey(), partition.getValue());
            if (partition.getValue().isEmpty()) {
                long end = plan.ends().offsets().get(partition.getKey());
                commit(new SyncBatch(plan, generation, partition.getKey(), end, end, true, List.of()), baseline, execution::check, writer);
            } else if (missing.isEmpty() && plan.checked() && plan.topic().mode() == Mode.COMPACTED) {
                caughtUp(plan.topic().name(), generation, partition.getKey(), plan.ends().capturedAtMs());
            }
            if (missing.isEmpty()) continue;
            partitions.add(fetcher -> {
                // Start the cadence when this partition begins, not while it waits for a consumer.
                var progress = request.write(() -> new PartitionProgress(plan.topic().name(), partition.getKey(), missing.asRanges().stream()
                        .mapToLong(r -> r.upperEndpoint() - r.lowerEndpoint()).sum()));
                for (Range<Long> range : missing.asRanges()) {
                    request.write(() -> { hooks.beforeFetch(plan); return null; });
                    fetchRange(fetcher, plan, generation, partition.getKey(), range, baseline, request, writer, batchSize, progress);
                }
            });
        }
        if (!partitions.isEmpty()) request.fetch(partitions, Math.min(parallelism, partitions.size()));
        return generation;
    }

    /** One outstanding handoff per owner bounds raw polls to the fixed consumer count. */
    private final class FetchRequest {
        private final Execution execution;
        private final BlockingQueue<Runnable> writes = new LinkedBlockingQueue<>();
        private final CompletableFuture<Void> failed = new CompletableFuture<>();

        FetchRequest(Execution execution) { this.execution = execution; }

        void check() throws SQLException {
            execution.check();
            if (failed.isDone()) execution.await(failed);
        }

        void fail(Throwable error) {
            if (failed.completeExceptionally(error)) {
                wakeConsumers();
                writes.add(() -> {}); // Wake the writer even when no poll handoff is waiting.
            }
        }

        <T> T write(Work<T> work) throws SQLException {
            check();
            var job = new Job<T>(execution, () -> { check(); return work.run(); });
            job.result.whenComplete((result, error) -> { if (error != null) fail(error); });
            writes.add(job::run);
            execution.await(CompletableFuture.anyOf(job.result, failed));
            return execution.await(job.result);
        }

        void fetch(List<PartitionFetch> partitions, int count) throws SQLException {
            // A cancelled decode may still be finishing a bounded Registry call. Only new fetches wait.
            execution.await(fetchIdle);
            while (fetchers.size() < count) fetchers.add(new FetchConsumer(fetchers.size() + 1));
            var waiting = new ConcurrentLinkedQueue<>(partitions);
            var tasks = new ArrayList<CompletableFuture<Void>>();
            CompletableFuture<Void> done = null;
            try {
                for (int i = 0; i < count; i++) {
                    var fetcher = fetchers.get(i);
                    tasks.add(CompletableFuture.runAsync(() -> {
                        try {
                            check(); fetcher.ensureConsumer();
                            PartitionFetch partition;
                            while ((partition = waiting.poll()) != null) { check(); partition.run(fetcher); }
                        } catch (SQLException | RuntimeException | Error error) {
                            fail(error);
                            throw new CompletionException(error);
                        }
                    }, fetcher.owner));
                }
                done = CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new));
                done.whenComplete((result, error) -> writes.add(() -> {}));
                while (!done.isDone()) {
                    check();
                    try {
                        var write = writes.poll(execution.remaining(POLL).toNanos(), TimeUnit.NANOSECONDS);
                        if (write != null) { check(); write.run(); }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); execution.cancel(); execution.check();
                    }
                }
                check(); execution.await(done);
            } catch (SQLException | RuntimeException | Error error) {
                fail(error);
                throw error;
            } finally {
                // Free the writer after cancellation; owner executors drain before any later fetch.
                if (done == null) done = CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new));
                fetchIdle = done.handle((result, error) -> null);
            }
        }
    }

    private void fetchRange(FetchConsumer fetcher, SyncPlan plan, long generation, int partition, Range<Long> range,
                            long baseline, FetchRequest request, BatchWriter writer, int batchSize, PartitionProgress progress) throws SQLException {
        request.check();
        TopicPartition tp = new TopicPartition(plan.topic().name(), partition);
        long end = range.upperEndpoint();
        long position = range.lowerEndpoint();
        var output = new RangeWriter(plan, generation, partition, range, baseline, request, writer, batchSize, progress);
        var formats = config.topic(tp.topic());
        Format key = formats.keyFormat().orElse(Format.AUTO), value = formats.valueFormat().orElse(Format.AUTO);
        try {
            fetcher.consumer.assign(List.of(tp));
            fetcher.consumer.seek(tp, range.lowerEndpoint());
            while (position < end) {
                var records = fetcher.poll(request);
                position = Math.min(end, fetcher.position(tp, request));
                var decoded = records.isEmpty() ? List.<SyncBatch.Record>of()
                        : fetcher.decoder.decode(tp.topic(), records, end, key, value);
                long captured = position;
                request.write(() -> { output.accept(decoded, captured); return null; });
            }
        } catch (OffsetOutOfRangeException | org.apache.kafka.common.errors.UnknownTopicOrPartitionException expired) {
            throw new StaleGeneration(tp.topic());
        } catch (RuntimeException error) {
            throw failure("Fetching topic " + tp.topic(), error);
        }
    }

    /** Only the writer assembles and commits the already decoded records. */
    private final class RangeWriter {
        private final SyncPlan plan;
        private final long generation, baseline, end;
        private final int partition, batchSize;
        private final FetchRequest request;
        private final BatchWriter writer;
        private final PartitionProgress progress;
        private final List<SyncBatch.Record> batch;
        private long start, previousPosition;

        RangeWriter(SyncPlan plan, long generation, int partition, Range<Long> range, long baseline,
                    FetchRequest request, BatchWriter writer, int batchSize, PartitionProgress progress) {
            this.plan = plan; this.generation = generation; this.partition = partition; this.baseline = baseline;
            this.request = request; this.writer = writer; this.batchSize = batchSize; this.progress = progress;
            start = range.lowerEndpoint(); previousPosition = start; end = range.upperEndpoint();
            batch = new ArrayList<>(Math.min(batchSize, 5000));
        }

        void accept(List<SyncBatch.Record> decoded, long position) throws SQLException {
            var execution = request.execution;
            progress.fetched += decoded.size();
            progress.remaining -= position - previousPosition;
            previousPosition = position;
            for (var record : decoded) {
                execution.check();
                if (batch.size() == batchSize) {
                    long batchEnd = batch.getLast().offset() + 1;
                    commit(new SyncBatch(plan, generation, partition, start, batchEnd, false, batch), baseline, request::check, writer);
                    start = batchEnd;
                    batch.clear();
                    progress.report();
                }
                batch.add(record);
            }
            if (position >= end) {
                if (start < end) commit(new SyncBatch(plan, generation, partition, start, end,
                        end == plan.ends().offsets().get(partition), batch), baseline, request::check, writer);
                start = end;
            }
            progress.report();
        }
    }

    /** One report cadence across every missing range in this partition; no timer thread or per-record clock read. */
    private final class PartitionProgress {
        private final String topic;
        private final int partition;
        private long lastReport = hooks.nanoTime();
        private long fetched;
        private long remaining;

        PartitionProgress(String topic, int partition, long remaining) {
            this.topic = topic; this.partition = partition; this.remaining = remaining;
        }

        void report() {
            long now = hooks.nanoTime();
            if (now - lastReport < PROGRESS_INTERVAL_NANOS) return;
            lastReport = now;
            log.info("{} partition {}: {} records fetched, {} offsets remaining", topic, partition, fetched, remaining);
        }
    }

    private SchemaRegistry registry() throws SQLException {
        synchronized (registryLock) {
            if (registry == null) registry = SchemaRegistry.open(settings);
            return registry;
        }
    }

    private void wakeConsumers() { consumers.forEach(KafkaConsumer::wakeup); }

    /** A persistent JDK single-thread executor gives each consumer one owner for its whole lifetime. */
    private final class FetchConsumer implements AutoCloseable {
        private final ExecutorService owner;
        private KafkaConsumer<byte[], byte[]> consumer;
        private String id;
        private BatchDecoder decoder;

        FetchConsumer(int number) {
            owner = Executors.newSingleThreadExecutor(work -> {
                var thread = Thread.ofPlatform().daemon().name("kafcacheql-fetch-" + config.clientId() + "-" + number).unstarted(work);
                thread.setContextClassLoader(DriverLoader.LOADER);
                return thread;
            });
        }

        void ensureConsumer() throws SQLException {
            if (decoder == null) decoder = new BatchDecoder(settings, cache.newReader(), SyncWorker.this::registry, transforms);
            if (consumer != null) return;
            id = ClientIds.reserve(ClientIds.Kind.CONSUMER, config.clientId());
            Map<String, Object> properties = new HashMap<>(settings.consumer());
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
            try { consumer = new KafkaConsumer<>(properties); consumers.add(consumer); }
            catch (RuntimeException e) {
                ClientIds.release(ClientIds.Kind.CONSUMER, id); id = null;
                throw failure("Creating cache consumer", e);
            }
        }

        private ConsumerRecords<byte[], byte[]> poll(FetchRequest request) throws SQLException {
            while (true) {
                request.check();
                try { return consumer.poll(request.execution.remaining(POLL)); }
                catch (WakeupException wakeup) { request.check(); }
            }
        }

        private long position(TopicPartition partition, FetchRequest request) throws SQLException {
            while (true) {
                request.check();
                try { return consumer.position(partition, request.execution.remaining(POLL)); }
                // Preserve the records already returned by poll if a late wakeup reaches position instead.
                catch (WakeupException wakeup) { request.check(); }
            }
        }

        @Override @SuppressWarnings("try") // The decoder resource supplies its close action and suppression order.
        public void close() throws SQLException {
            var closed = CompletableFuture.runAsync(() -> {
                try (var decoding = decoder) {
                    if (consumer != null) consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
                } catch (SQLException e) { throw new CompletionException(e); }
                finally {
                    if (consumer != null) consumers.remove(consumer);
                    if (id != null) ClientIds.release(ClientIds.Kind.CONSUMER, id);
                }
            }, owner);
            owner.shutdown();
            try { closed.join(); }
            catch (CompletionException e) { throw failure("Closing consumer", e.getCause()); }
            finally { owner.close(); }
        }
    }

    private void commit(SyncBatch batch, long baseline, CacheStore.Check check, BatchWriter writer) throws SQLException {
        hooks.beforeCommit(batch);
        transaction(batch.plan(), batch.generation(), check, () -> {
            writer.write(cache.writer(), batch);
            // Roll back every compacted batch if fetching outlived its original safety baseline.
            // The unchanged baseline makes the existing validity check rebuild on the bounded replan.
            if (batch.plan().topic().mode() == Mode.COMPACTED
                    && System.currentTimeMillis() - baseline > batch.plan().topic().deleteRetentionMs()) {
                throw new StaleGeneration(batch.plan().topic().name());
            }
            return null;
        });
        hooks.afterCommit(batch);
    }

    private void prune(SyncPlan plan, long generation, Execution execution, int batchSize) throws SQLException {
        boolean needed = transaction(plan, generation, execution::check, () -> {
            if (!PartitionStore.needsPruning(cache.writer(), generation, plan.logStarts())) return false;
            PartitionStore.raiseFloors(cache.writer(), generation, plan.logStarts());
            return true;
        });
        if (!needed) return;
        for (int partition : plan.logStarts().keySet()) {
            int deleted;
            do {
                deleted = transaction(plan, generation, execution::check,
                        () -> PartitionStore.deleteChunk(cache.writer(), generation, partition, batchSize));
                hooks.afterPrune(plan, generation, partition, deleted);
            } while (deleted == batchSize);
        }
    }

    private <T> T transaction(SyncPlan plan, long generation, CacheStore.Check check, CacheStore.Write<T> work)
            throws SQLException {
        long before = changes();
        T result = CacheStore.writeTransaction(cache.writer(), check, () -> {
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

    public static void requireGeneration(Connection reader, String topic, long generation) throws SQLException {
        var current = TopicRegistry.observe(reader, topic);
        if (current.isEmpty() || current.get().id() != generation || current.get().deleted()) throw new StaleGeneration(topic);
    }

    void caughtUp(String topic, long generation, int partition, long capturedAt) {
        background(new Background(List.of("caught-up", generation, partition), capturedAt, () ->
                CacheStore.writeTransaction(cache.writer(), () -> {
                    var current = TopicRegistry.observe(cache.writer(), topic);
                    if (current.isPresent() && current.get().id() == generation && !current.get().deleted()) {
                        PartitionStore.caughtUp(cache.writer(), generation, partition, capturedAt);
                    }
                    return null;
                })));
    }

    void drop(String topic, long generation) {
        background(new Background(List.of("drop", generation), 0, () -> {
            TopicRegistry.delete(cache, topic, java.util.OptionalLong.of(generation), null);
            return null;
        }));
    }

    void queried(String topic, long generation, long at) {
        background(new Background(List.of("last-query", generation), at, () ->
                CacheStore.writeTransaction(cache.writer(), () -> {
                    EvictionStore.queried(cache.writer(), topic, generation, at);
                    return null;
                })));
    }

    /** The caller already holds its checked read snapshot. A sync alone must never evict its window. */
    void readerReady() {
        if (config.cacheMaxBytes().isEmpty() && config.cacheRetentionHours().isEmpty()) return;
        background(new Background("eviction", 0, () -> {
            evict();
            checkpoint();
            return null;
        }));
    }

    private void evict() throws SQLException {
        if (config.cacheRetentionHours().isEmpty() && !overLimit()) return;
        long cutoff = config.cacheRetentionHours().isPresent()
                ? System.currentTimeMillis() - Duration.ofHours(config.cacheRetentionHours().getAsLong()).toMillis()
                : Long.MIN_VALUE;
        while (true) {
            synchronized (this) { if (closing) return; }
            boolean removed = CacheStore.writeTransaction(cache.writer(), () -> {
                boolean oversized = overLimit();
                var oldest = EvictionStore.oldestAppend(cache.writer());
                if (oldest.isEmpty() || (!oversized && oldest.get().timestamp() >= cutoff)) return false;
                var topic = oldest.get().topic();
                long ceiling = oversized ? oldest.get().ceiling() : Math.min(oldest.get().ceiling(), cutoff - 1);
                requireGeneration(cache.writer(), topic.name(), topic.generation());
                return PartitionStore.evictChunk(cache.writer(), topic.generation(), ceiling, config.syncBatchRecords()) > 0;
            });
            if (removed) continue;
            if (!overLimit()) return;
            var oldest = EvictionStore.oldestCompacted(cache.writer());
            if (oldest.isEmpty()) {
                log.warn("Cache uses {} bytes, above cache.max.bytes={}; nothing remains to evict", CacheStore.usedBytes(cache.writer()), config.cacheMaxBytes().orElseThrow());
                return;
            }
            var topic = oldest.get();
            TopicRegistry.delete(cache, topic.name(), java.util.OptionalLong.of(topic.generation()), null, () ->
                    overLimit() && EvictionStore.oldestAppend(cache.writer()).isEmpty()
                    && EvictionStore.oldestCompacted(cache.writer()).map(t -> t.generation() == topic.generation()).orElse(false));
        }
    }

    private boolean overLimit() throws SQLException {
        return config.cacheMaxBytes().isPresent() && CacheStore.usedBytes(cache.writer()) > config.cacheMaxBytes().getAsLong();
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
            write.work().run();
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
