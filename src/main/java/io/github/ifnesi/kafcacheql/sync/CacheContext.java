package io.github.ifnesi.kafcacheql.sync;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.PartitionStore;
import io.github.ifnesi.kafcacheql.cache.RangeStore;
import io.github.ifnesi.kafcacheql.cache.RecordStore;
import io.github.ifnesi.kafcacheql.cache.SchemaStore;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared lifetime of one real file's worker, writer, consumer, and readers, independent of query settings. */
public final class CacheContext {
    private static final ConcurrentHashMap<Path, CacheContext> OPEN = new ConcurrentHashMap<>();
    private final DriverConfig config;
    private final ClientSettings settings;
    private final SyncWorker worker;
    private volatile CacheStore cache;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final ArrayDeque<Connection> readers = new ArrayDeque<>();
    private final Path path;
    private int leases;
    private volatile boolean closing;

    private CacheContext(Path path, DriverConfig config, ClientSettings settings, Optional<String> cluster) throws SQLException {
        this.path = path;
        this.config = config;
        this.settings = settings;
        worker = new SyncWorker(path, cluster, config, settings);

    }

    @FunctionalInterface public interface SqlWork<T> { T run(Connection connection) throws SQLException; }

    /** A connection's reference. Client leases are acquired and released independently of refused contexts. */
    public static final class Lease implements AutoCloseable {
        private final CacheContext context;
        private final DriverConfig config;
        private final KafkaClients.Lease clients;
        private boolean closed;

        private Lease(CacheContext context, DriverConfig config, KafkaClients.Lease clients) {
            this.context = context; this.config = config; this.clients = clients;
        }

        public Path path() { return context.path; }
        public synchronized <T> T read(SqlWork<T> work) throws SQLException {
            ensureOpen();
            return context.read(work);
        }

        /** Full-topic window after the caller's §9 validation. Rebuild policy belongs to 0007a;
         * SQL's window resolver can supply bounded windows through the overload.
         */
        public SyncPlan plan(String topic, Execution execution) throws SQLException { return plan(topic, null, execution); }

        public SyncPlan plan(String topic, Map<Integer, ? extends RangeSet<Long>> wanted, Execution execution) throws SQLException {
            execution.check();
            OptionalLong generation = read(c -> TopicRegistry.generation(c, topic));
            if (config.offline()) throw new SQLException("Offline planning needs cached windows for topic " + topic);
            var kafka = clients.clients();
            var info = kafka.describeTopic(topic, config, execution)
                    .orElseThrow(() -> new SQLException("Unknown topic " + topic, "42S02"));
            var starts = kafka.logStartOffsets(topic, info.partitions(), execution);
            var ends = kafka.endOffsets(topic, info.partitions(), execution);
            Map<Integer, ImmutableRangeSet<Long>> windows = new TreeMap<>();
            var warnings = new ArrayList<SQLWarning>();
            for (int partition = 0; partition < info.partitions(); partition++) {
                Range<Long> available = Range.closedOpen(starts.get(partition), ends.offsets().get(partition));
                RangeSet<Long> ranges = wanted == null || info.mode() == Mode.COMPACTED
                        ? ImmutableRangeSet.of(available) : wanted.get(partition);
                if (ranges != null) {
                    if (info.mode() == Mode.APPEND && !ranges.subRangeSet(Range.lessThan(starts.get(partition))).isEmpty()) {
                        warnings.add(new SQLWarning("Topic " + topic + " partition " + partition
                                + ": requested data below offset " + starts.get(partition) + " has expired", "01000"));
                    }
                    windows.put(partition, ImmutableRangeSet.copyOf(ranges.subRangeSet(available)));
                }
            }
            return new SyncPlan(info, generation, windows, ends, starts, warnings);
        }

        public long sync(SyncPlan plan, Execution execution) throws SQLException {
            return sync(plan, execution, RecordStore::write);
        }

        /** Cached windows take only a reader, even while the worker is held in another topic's first sync. */
        public long sync(SyncPlan plan, Execution execution, BatchWriter writer) throws SQLException {
            ensureOpen();
            execution.check();
            boolean cached = plan.generation().isPresent() && read(c -> {
                var current = TopicRegistry.observe(c, plan.topic().name());
                if (current.isEmpty() || current.get().id() != plan.generation().getAsLong()) throw new StaleGeneration(plan.topic().name());
                if (current.get().deleted()) return false;
                if (current.get().partitions() < plan.topic().partitions()) return false;
                if (config.cachePruneExpired() && plan.topic().mode() == Mode.APPEND
                        && PartitionStore.needsPruning(c, current.get().id(), plan.logStarts())) return false;
                for (var entry : plan.windows().entrySet()) {
                    if (!RangeStore.missing(c, plan.generation().getAsLong(), entry.getKey(), entry.getValue()).isEmpty()) return false;
                }
                return true;
            });
            if (cached) {
                long id = plan.generation().getAsLong();
                if (!config.offline() && plan.topic().mode() == Mode.COMPACTED) {
                    for (var entry : plan.ends().offsets().entrySet()) {
                        context.worker.caughtUp(plan.topic().name(), id, entry.getKey(), plan.ends().capturedAtMs());
                    }
                }
                return id;
            }
            if (config.offline()) throw new SQLException("Offline cache is missing a window for topic " + plan.topic().name());
            return context.worker.sync(plan, execution, writer, config.syncBatchRecords());
        }

        public void storeSchema(SchemaStore.StoredSchema schema) throws SQLException { ensureOpen(); context.worker.schema(schema); }
        public void storeSubject(SchemaStore.SubjectSchema subject) throws SQLException { ensureOpen(); context.worker.subject(subject); }
        /** Registration's specialised path is in sync; rebuild/prune/view work can use this guarded handoff. */
        public <T> T write(String topic, long generation, Execution execution, SqlWork<T> work) throws SQLException {
            ensureOpen();
            return context.worker.submit(execution, () -> CacheStore.writeTransaction(context.cache.writer(), execution::check, () -> {
                SyncWorker.requireGeneration(context.cache.writer(), topic, generation);
                return work.run(context.cache.writer());
            }));
        }
        public void hooks(SyncWorker.Hooks hooks) throws SQLException { ensureOpen(); context.worker.hooks(hooks); }
        int pendingBackground() { return context.worker.pendingBackground(); }
        int queuedRequests() { return context.worker.queuedRequests(); }

        private synchronized void ensureOpen() throws SQLException {
            if (closed) throw new SQLException("Cache lease is closed");
        }

        @Override public void close() throws SQLException {
            synchronized (this) { if (closed) return; closed = true; }
            try { context.release(); }
            finally { if (clients != null) clients.close(); }
        }
    }

    /** Only callers opening the same real file wait on each other. No network operation holds OPEN. */
    public static Lease acquire(DriverConfig config) throws SQLException {
        return DriverLoader.call(() -> {
            ClientSettings settings = ClientSettings.of(config);
            KafkaClients.Lease clients = config.offline() ? null : KafkaClients.acquire(config);
            try {
                Optional<String> cluster = clients == null ? Optional.empty() : Optional.of(clients.clients().clusterId());
                Path path = CacheStore.canonicalPath(CacheStore.resolve(config, cluster), !config.offline());
                CacheContext context = acquireContext(path, config, settings, cluster);
                return new Lease(context, config, clients);
            } catch (SQLException | RuntimeException | Error e) {
                if (clients != null) clients.close();
                throw e;
            }
        });
    }

    private static final class OpenFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
        OpenFailure(SQLException cause) { super(cause); }
    }

    private static CacheContext acquireContext(Path path, DriverConfig config, ClientSettings settings, Optional<String> cluster)
            throws SQLException {
        while (true) {
            CacheContext context;
            try {
                context = OPEN.compute(path, (p, existing) -> {
                    try {
                        if (existing == null) existing = new CacheContext(path, config, settings, cluster);
                        if (!existing.closing) {
                            existing.compatible(config, settings);
                            existing.leases++;
                        }
                        return existing;
                    } catch (SQLException e) { throw new OpenFailure(e); }
                });
            } catch (OpenFailure e) { throw (SQLException) e.getCause(); }
            if (context.closing) {
                try { context.closed.get(); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while cache closed", e);
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new SQLException("Closing cache failed", e.getCause());
                }
                continue;
            }
            try {
                context.cache = context.worker.opened();
                if (cluster.isPresent() && !context.cache.clusterId().equals(cluster.get())) {
                    throw ConfigErrors.problem("The cache " + path + " belongs to another Kafka cluster");
                }
                return context;
            } catch (SQLException e) {
                try { context.release(); } catch (SQLException close) { e.addSuppressed(close); }
                throw e;
            }
        }
    }

    private void compatible(DriverConfig other, ClientSettings clients) throws SQLException {
        if (!config.clientId().equals(other.clientId()) || !settings.consumer().equals(clients.consumer())
                || !settings.admin().equals(clients.admin()) || !settings.schemaRegistry().equals(clients.schemaRegistry())) {
            throw ConfigErrors.problem("The open cache " + path + " has different client settings");
        }
        if (config.cachePruneExpired() != other.cachePruneExpired()) different("cache.prune.expired");
        Set<String> topics = new HashSet<>(config.topics().keySet());
        topics.addAll(other.topics().keySet());
        for (String topic : topics) {
            var a = config.topic(topic); var b = other.topic(topic);
            if (!a.mode().equals(b.mode())) different("topic." + topic + ".mode");
            if (a.keyFormat().orElse(Format.AUTO) != b.keyFormat().orElse(Format.AUTO)) different("topic." + topic + ".key.format");
            if (a.valueFormat().orElse(Format.AUTO) != b.valueFormat().orElse(Format.AUTO)) different("topic." + topic + ".value.format");
        }
    }

    private void different(String setting) throws SQLException {
        throw ConfigErrors.problem("The open cache " + path + " has a different " + setting + " setting");
    }

    private <T> T read(SqlWork<T> work) throws SQLException {
        Connection reader;
        synchronized (readers) { reader = readers.poll(); }
        if (reader == null) reader = cache.newReader();
        boolean reusable = false;
        try (Statement s = reader.createStatement()) {
            // The generation, storage schema, pruning floors and ranges must describe one snapshot.
            s.execute("BEGIN");
            T result = work.run(reader);
            s.execute("COMMIT");
            reusable = true;
            return result;
        }
        finally {
            synchronized (readers) {
                if (reusable && !closing) readers.add(reader); else reader.close();
            }
        }
    }

    private void release() throws SQLException {
        AtomicBoolean last = new AtomicBoolean();
        OPEN.computeIfPresent(path, (p, context) -> {
            if (--leases == 0) { closing = true; last.set(true); }
            return this;
        });
        if (!last.get()) return;
        SQLException failure = null;
        synchronized (readers) {
            while (!readers.isEmpty()) {
                try { readers.remove().close(); }
                catch (SQLException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            }
        }
        try { worker.close(); }
        catch (SQLException e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
        finally { OPEN.remove(path, this); closed.complete(null); }
        if (failure != null) throw failure;
    }
}
