package io.github.ifnesi.kafcacheql.sync;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import io.github.ifnesi.kafcacheql.transform.RecordTransforms;
import com.google.common.base.Ascii;
import com.google.common.collect.RangeSet;
import io.github.ifnesi.kafcacheql.cache.CacheStore;
import io.github.ifnesi.kafcacheql.cache.PartitionStore;
import io.github.ifnesi.kafcacheql.cache.RangeStore;
import io.github.ifnesi.kafcacheql.cache.RecordStore;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.kafka.ClientSettings;
import io.github.ifnesi.kafcacheql.kafka.EndOffsets;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import io.github.ifnesi.kafcacheql.schema.AvroJsonConverter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Instant;
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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

/** Shared lifetime of one real file's worker, writer, consumer, and readers, independent of query settings. */
public final class CacheContext {
    private static final ConcurrentHashMap<Path, CacheContext> OPEN = new ConcurrentHashMap<>();
    private final DriverConfig config;
    private final ClientSettings settings;
    private final SyncWorker worker;
    private final RecordTransforms transforms;
    private volatile CacheStore cache;
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final ArrayDeque<Connection> readers = new ArrayDeque<>();
    private final Path path;
    private final AtomicBoolean syncing = new AtomicBoolean();
    private record Checked(long generation, TopicInfo topic, EndOffsets ends, Map<Integer, Long> starts) {}
    private final ConcurrentHashMap<String, Checked> checked = new ConcurrentHashMap<>();
    private int leases;
    private volatile boolean closing;

    private CacheContext(Path path, DriverConfig config, ClientSettings settings, Optional<String> cluster) throws SQLException {
        this.path = path;
        this.config = config;
        this.settings = settings;
        transforms = new RecordTransforms(config);
        worker = new SyncWorker(path, cluster, config, settings, transforms);

    }

    @FunctionalInterface public interface SqlWork<T> { T run(Connection connection) throws SQLException; }

    /** A connection's reference. Client leases are acquired and released independently of refused contexts. */
    public static final class Lease implements AutoCloseable {
        private final CacheContext context;
        private final DriverConfig config;
        private final KafkaClients.Lease clients;
        private final KafkaClients.Topics topics;
        private boolean closed;

        private Lease(CacheContext context, DriverConfig config, KafkaClients.Lease clients) {
            this.context = context; this.config = config; this.clients = clients;
            topics = clients == null ? null : clients.clients().topics(config);
        }

        /** Take the process-local guard before opening the OS lock file: closing any channel can release its lock. */
        public void beginSync() throws SQLException {
            ensureOpen();
            if (!context.syncing.compareAndSet(false, true))
                throw new SQLException(sentence("another SYNC is running on cache " + context.path), "55006");
        }
        public void endSync() { context.syncing.set(false); }

        public Path path() { return context.path; }
        public DriverConfig config() { return config; }
        public synchronized <T> T read(SqlWork<T> work) throws SQLException {
            ensureOpen();
            return context.read(work);
        }
        public synchronized Reader borrowReader(Execution execution) throws SQLException {
            ensureOpen();
            return context.borrowReader(execution);
        }
        public <T> T read(Execution execution, SqlWork<T> work) throws SQLException {
            try (var reader = borrowReader(execution)) { return work.run(reader.connection()); }
        }
        public boolean isValid(Execution execution) throws SQLException {
            ensureOpen();
            return clients == null || clients.clients().isValid(execution);
        }

        /** Refresh captures generations before Kafka; missing topics are dropped by the worker, without waiting. */
        public Set<String> listTopics() throws SQLException { return listTopics(null); }
        public Set<String> listTopics(Execution execution) throws SQLException {
            ensureOpen();
            var cached = read(execution, reader -> {
                var generations = new TreeMap<String, Long>();
                try (var s = reader.createStatement(); var r = s.executeQuery("SELECT topic,id FROM \"$topics\" WHERE deleted_ms IS NULL")) {
                    while (r.next()) generations.put(r.getString(1), r.getLong(2));
                }
                return generations;
            });
            if (clients == null) return Set.copyOf(cached.keySet());
            var listed = clients.clients().listTopics(execution);
            cached.forEach((topic, generation) -> { if (!listed.contains(topic)) context.worker.drop(topic, generation); });
            return listed;
        }

        public Optional<TopicInfo> describeTopic(String topic) throws SQLException {
            ensureOpen();
            return clients == null ? Optional.empty() : topics.describe(topic, null);
        }

        public Map<String, Mode> topicModes(Set<String> names) throws SQLException {
            ensureOpen();
            return clients == null ? Map.of() : clients.clients().topicModes(names, config);
        }

        /** Resolve SQL names without fetching records. Include/exclude patterns affect metadata only. */
        public TopicRegistry.ResolvedTable resolveTable(String name, Execution execution) throws SQLException {
            execution.check();
            var cached = read(c -> TopicRegistry.resolve(c, name));
            if (cached.isPresent()) return cached.get();
            if (config.offline() || name.startsWith("$")) throw unknownTable(name);
            var knownTopics = clients.clients().listTopics(execution);
            var names = read(c -> TopicRegistry.viewNames(c, knownTopics));
            String topic = names.entrySet().stream().filter(e -> Ascii.equalsIgnoreCase(e.getValue(), name))
                    .map(Map.Entry::getKey).findFirst().orElseThrow(() -> unknownTable(name));
            var observed = read(c -> TopicRegistry.generation(c, topic));
            var info = topics.describe(topic, execution).orElseThrow(() -> unknownTable(name));
            var registered = context.worker.submit(execution, () -> TopicRegistry.register(context.cache, observed, info,
                    context.transforms.settings(topic, config.topic(topic)), AvroJsonConverter.VERSION, execution::check, knownTopics)
                    .orElseThrow(() -> new StaleGeneration(topic)));
            return new TopicRegistry.ResolvedTable(registered.viewName(), Optional.of(topic), registered.warning());
        }

        private static SQLException unknownTable(String name) {
            return new SQLException(sentence("Unknown table " + name), "42S02");
        }

        /** Full-topic window with §9 validity observations; SQL's resolver can supply bounded windows. */
        public SyncPlan plan(String topic, Execution execution) throws SQLException { return plan(topic, null, execution); }

        public SyncPlan plan(String topic, Map<Integer, ? extends RangeSet<Long>> wanted, Execution execution) throws SQLException {
            return plan(topic, wanted, execution, false);
        }
        public SyncPlan freshPlan(String topic, Execution execution) throws SQLException { return plan(topic, null, execution, true); }
        private SyncPlan plan(String topic, Map<Integer, ? extends RangeSet<Long>> wanted, Execution execution, boolean fresh) throws SQLException {
            execution.check();
            var observed = read(c -> TopicRegistry.observe(c, topic));
            OptionalLong generation = observed.isPresent() ? OptionalLong.of(observed.get().id()) : OptionalLong.empty();
            boolean deleted = observed.isPresent() && observed.get().deleted();
            if (config.offline()) return read(execution, c -> cachedPlan(c, topic, wanted));
            var recent = context.checked.get(topic);
            if (!fresh && config.maxStalenessMs() > 0 && recent != null && generation.isPresent() && !deleted
                    && recent.generation() == generation.getAsLong()
                    && System.currentTimeMillis() - recent.ends().capturedAtMs() < config.maxStalenessMs()) {
                return SyncPlan.create(recent.topic(), generation, false, wanted, recent.ends(), recent.starts(), SyncPlan.Source.RECENT);
            }
            var kafka = clients.clients();
            var found = topics.describe(topic, execution);
            if (found.isEmpty()) {
                context.worker.delete(topic, generation, execution);
                throw new SQLException(sentence("Unknown topic " + topic), "42S02");
            }
            var info = found.get();
            var starts = kafka.logStartOffsets(topic, info.partitions(), execution);
            var ends = kafka.endOffsets(topic, info.partitions(), execution);
            return SyncPlan.create(info, generation, deleted, wanted, ends, starts);
        }

        /** Cached topic metadata and per-partition ends, read on one snapshot. No registry or broker exists offline. */
        private SyncPlan cachedPlan(Connection reader, String topic, Map<Integer, ? extends RangeSet<Long>> wanted) throws SQLException {
            try (var p = reader.prepareStatement("SELECT id,topic_uuid,partition_count,mode,delete_retention_ms,timestamp_type FROM \"$topics\" WHERE topic=? AND deleted_ms IS NULL")) {
                p.setString(1, topic);
                try (var row = p.executeQuery()) {
                    if (!row.next()) throw unknownTable(topic);
                    long generation = row.getLong(1);
                    var info = new TopicInfo(topic, Uuid.fromString(row.getString(2)), row.getInt(3),
                            "append".equals(row.getString(4)) ? Mode.APPEND : Mode.COMPACTED, row.getLong(5),
                            "LogAppendTime".equals(row.getString(6)) ? TimestampType.LOG_APPEND_TIME : TimestampType.CREATE_TIME, false);
                    var bounds = PartitionStore.cachedBounds(reader, generation);
                    return SyncPlan.create(info, OptionalLong.of(generation), false, wanted, new EndOffsets(bounds.ends(), 0), bounds.starts(), SyncPlan.Source.OFFLINE);
                }
            }
        }

        public Map<Integer, OptionalLong> offsetsForTimes(SyncPlan plan, long timestamp, Execution execution) throws SQLException {
            ensureOpen();
            if (!config.offline()) return clients.clients().offsetsForTimes(plan.topic().name(), plan.topic().partitions(), timestamp, execution);
            return read(execution, reader -> {
                // A gap could contain an earlier matching offset, even when no cached row matches.
                validate(reader, plan, plan.generation().orElseThrow());
                var offsets = new TreeMap<Integer, OptionalLong>();
                for (int partition : plan.ends().offsets().keySet()) offsets.put(partition, OptionalLong.empty());
                try (var p = reader.prepareStatement("SELECT _partition,min(_offset) FROM "
                        + StorageDdl.quote(StorageDdl.tableName(plan.generation().orElseThrow()))
                        + " WHERE _timestamp>=? GROUP BY _partition")) {
                    p.setLong(1, timestamp);
                    try (var rows = p.executeQuery()) { while (rows.next()) offsets.put(rows.getInt(1), OptionalLong.of(rows.getLong(2))); }
                }
                return offsets;
            });
        }

        public Map<Integer, Long> newestTimestamps(SyncPlan plan, Execution execution) throws SQLException {
            return read(execution, reader -> {
                validate(reader, plan, plan.generation().orElseThrow());
                var times = new TreeMap<Integer, Long>();
                try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT _partition,max(_timestamp) FROM "
                        + StorageDdl.quote(StorageDdl.tableName(plan.generation().orElseThrow())) + " GROUP BY _partition")) {
                    while (rows.next()) times.put(rows.getInt(1), rows.getLong(2));
                }
                return times;
            });
        }

        /** The sync fast path and the pipeline's final reader use the same generation/range check. */
        public void validate(Connection reader, SyncPlan plan, long generation) throws SQLException {
            try {
                SyncWorker.requireGeneration(reader, plan.topic().name(), generation);
                if (config.offline()) context.transforms.validateOffline(reader, plan.topic().name());
                var missing = new TreeMap<Integer, RangeSet<Long>>();
                for (var partition : plan.windows().entrySet()) {
                    var ranges = RangeStore.missing(reader, generation, partition.getKey(), partition.getValue());
                    if (!ranges.isEmpty()) missing.put(partition.getKey(), ranges);
                }
                if (!missing.isEmpty()) {
                    if (config.offline()) throw missing(plan, missing);
                    throw new StaleGeneration(plan.topic().name());
                }
            } catch (StaleGeneration stale) {
                context.checked.remove(plan.topic().name());
                if (config.offline()) throw missing(plan, plan.windows());
                throw stale;
            }
        }
        private static SQLException missing(SyncPlan plan, Object ranges) {
            return new SQLException(sentence("Offline cache is missing ranges for topic " + plan.topic().name() + " (partition: ranges): " + ranges), "40001");
        }
        public SQLWarning offlineWarning(Connection reader, SyncPlan plan, long generation) throws SQLException {
            var times = new ArrayList<String>();
            try (var p = reader.prepareStatement("SELECT partition_no,caught_up_at_ms FROM \"$partitions\" WHERE topic_ref=? ORDER BY partition_no")) {
                p.setLong(1, generation);
                try (var rows = p.executeQuery()) {
                    while (rows.next()) times.add("partition " + rows.getInt(1) + " as of "
                            + (rows.getObject(2) == null ? "unknown (no complete sync)" : Instant.ofEpochMilli(rows.getLong(2))));
                }
            }
            return new SQLWarning(sentence("Topic " + plan.topic().name() + ": offline cache, " + String.join(", ", times)), "01000");
        }

        public long sync(SyncPlan plan, Execution execution) throws SQLException {
            return sync(plan, execution, RecordStore::write);
        }

        /** Cached windows take only a reader, even while the worker is held in another topic's first sync. */
        public long sync(SyncPlan plan, Execution execution, BatchWriter writer) throws SQLException {
            return sync(plan, execution, writer, count -> {});
        }
        public long sync(SyncPlan plan, Execution execution, BatchWriter writer, java.util.function.LongConsumer fetched) throws SQLException {
            ensureOpen();
            execution.check();
            if (config.offline()) return read(execution, c -> {
                long generation = plan.generation().orElseThrow(); validate(c, plan, generation); return generation;
            });
            try {
                long generation = syncOnline(plan, execution, writer, fetched);
                if (plan.checked()) {
                    var check = new Checked(generation, plan.topic(), plan.ends(), plan.logStarts());
                    context.checked.compute(plan.topic().name(), (topic, old) -> old == null
                            || old.ends().capturedAtMs() <= plan.ends().capturedAtMs() ? check : old);
                }
                return generation;
            } catch (StaleGeneration stale) { context.checked.remove(plan.topic().name()); throw stale; }
        }

        private long syncOnline(SyncPlan plan, Execution execution, BatchWriter writer, java.util.function.LongConsumer fetched) throws SQLException {
            boolean cached = plan.generation().isPresent() && read(execution, c -> {
                var current = TopicRegistry.observe(c, plan.topic().name());
                if (current.isEmpty() || current.get().id() != plan.generation().getAsLong()) throw new StaleGeneration(plan.topic().name());
                if (current.get().deleted()) {
                    if (!plan.deleted()) throw new StaleGeneration(plan.topic().name());
                    return false;
                }
                if (TopicRegistry.rebuildReason(c, current.get().id(), plan.topic(), context.transforms.settings(plan.topic().name(), config.topic(plan.topic().name())),
                        plan.logStarts()).isPresent()) {
                    if (!plan.checked()) throw new StaleGeneration(plan.topic().name());
                    return false;
                }
                if (current.get().partitions() < plan.topic().partitions()) return false;
                if (plan.topic().mode() == Mode.APPEND
                        && PartitionStore.needsPruning(c, current.get().id(), plan.logStarts())) return false;
                for (var entry : plan.windows().entrySet()) {
                    if (!RangeStore.missing(c, plan.generation().getAsLong(), entry.getKey(), entry.getValue()).isEmpty()) return false;
                }
                return true;
            });
            if (cached) {
                long id = plan.generation().getAsLong();
                if (plan.checked() && plan.topic().mode() == Mode.COMPACTED) {
                    for (var entry : plan.ends().offsets().entrySet()) {
                        caughtUp(plan.topic().name(), id, entry.getKey(), plan.ends().capturedAtMs());
                    }
                }
                return id;
            }
            return context.worker.sync(plan, execution, writer, config.syncBatchRecords(), config.syncFetchParallelism(), fetched);
        }

        void caughtUp(String topic, long generation, int partition, long capturedAt) throws SQLException {
            ensureOpen();
            context.worker.caughtUp(topic, generation, partition, capturedAt);
        }

        /** SYNC starts at the last cached range, unless the existing validity rules require rebuilding. */
        public Map<Integer, Long> newestOffsets(SyncPlan plan, Execution execution) throws SQLException {
            if (plan.generation().isEmpty() || plan.deleted()) return Map.of();
            return read(execution, reader -> {
                long id = plan.generation().getAsLong();
                SyncWorker.requireGeneration(reader, plan.topic().name(), id);
                if (TopicRegistry.rebuildReason(reader, id, plan.topic(), context.transforms.settings(plan.topic().name(), config.topic(plan.topic().name())),
                        plan.logStarts()).isPresent()) return Map.of();
                return PartitionStore.cachedBounds(reader, id).ends();
            });
        }

        public boolean dropTable(String name, Execution execution) throws SQLException {
            var target = read(execution, reader -> {
                var resolved = TopicRegistry.resolve(reader, name);
                if (resolved.isEmpty() || resolved.get().topic().isEmpty()) return null;
                String topic = resolved.get().topic().get();
                return Map.entry(topic, TopicRegistry.generation(reader, topic));
            });
            if (target == null) return false;
            context.worker.delete(target.getKey(), target.getValue(), execution);
            context.checked.remove(target.getKey());
            return true;
        }

        /** Disposable metadata only; the query's own checked snapshot is already open. */
        public void queried(Map<String, Long> generations, long at) throws SQLException {
            ensureOpen();
            generations.forEach((topic, generation) -> context.worker.queried(topic, generation, at));
            if (!generations.isEmpty()) context.worker.readerReady();
        }
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
            if (closed) throw new SQLException(sentence("Cache lease is closed"));
            context.worker.checkOpen();
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
                    throw new SQLException(sentence("Interrupted while cache closed"), e);
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new SQLException(sentence("Closing cache failed"), e.getCause());
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
        if (!config.cacheMaxBytes().equals(other.cacheMaxBytes())) different("cache.max.bytes");
        if (!config.cacheRetentionHours().equals(other.cacheRetentionHours())) different("cache.retention.hours");
        if (!config.clientId().equals(other.clientId()) || !settings.consumer().equals(clients.consumer())
                || !settings.admin().equals(clients.admin()) || !settings.schemaRegistry().equals(clients.schemaRegistry())) {
            throw ConfigErrors.problem("The open cache " + path + " has different client settings");
        }
        Set<String> topics = new HashSet<>(config.topics().keySet());
        topics.addAll(other.topics().keySet());
        for (String topic : topics) {
            var a = config.topic(topic); var b = other.topic(topic);
            if (!a.transforms().equals(b.transforms()) || (a.transforms().enabled() && !config.maskSecret().equals(other.maskSecret())))
                different("record transform for " + topic);
            if (!a.mode().equals(b.mode())) different("topic." + topic + ".mode");
            if (a.keyFormat().orElse(Format.AUTO) != b.keyFormat().orElse(Format.AUTO)) different("topic." + topic + ".key.format");
            if (a.valueFormat().orElse(Format.AUTO) != b.valueFormat().orElse(Format.AUTO)) different("topic." + topic + ".value.format");
        }
    }

    private void different(String setting) throws SQLException {
        throw ConfigErrors.problem("The open cache " + path + " has a different " + setting + " setting");
    }

    private <T> T read(SqlWork<T> work) throws SQLException {
        try (var reader = borrowReader(null)) {
            return work.run(reader.connection());
        }
    }

    private Reader borrowReader(Execution execution) throws SQLException {
        Connection connection;
        synchronized (readers) { connection = readers.poll(); }
        if (connection == null) connection = cache.newReader();
        try { return new Reader(connection, execution); }
        catch (SQLException | RuntimeException | Error e) {
            try { connection.close(); } catch (SQLException close) { e.addSuppressed(close); }
            throw e;
        }
    }

    /** One execution's snapshot and interrupt target. An old lease never interrupts a reused connection. */
    public final class Reader implements AutoCloseable {
        private final Connection connection;
        private final Statement control;
        private Execution.Subscription stop;
        private PreparedStatement statement;
        private ResultSet rows;
        private boolean released;
        private boolean initialized;
        private SQLException interruptFailure;

        private Reader(Connection connection, Execution execution) throws SQLException {
            this.connection = connection;
            control = connection.createStatement();
            try {
                if (execution != null) stop = execution.onStop(this::interrupt);
                control.execute("BEGIN");
                if (execution != null) execution.check();
                initialized = true;
            } catch (SQLException | RuntimeException | Error e) {
                try { close(); } catch (SQLException close) { e.addSuppressed(close); }
                throw e;
            }
        }

        public Connection connection() { return connection; }
        public PreparedStatement prepare(String sql) throws SQLException {
            statement = connection.prepareStatement(sql);
            return statement;
        }
        public ResultSet executeQuery() throws SQLException { rows = statement.executeQuery(); return rows; }

        private synchronized void interrupt() {
            if (released) return;
            try { control.cancel(); }
            catch (SQLException e) { interruptFailure = e; }
        }

        @Override public void close() throws SQLException {
            // Never take Execution's lock while holding the reader lock: stop callbacks hold it first.
            if (stop != null) stop.close();
            synchronized (this) {
                if (released) return;
                released = true;
                SQLException failure = interruptFailure;
                try { if (rows != null) rows.close(); } catch (SQLException e) { failure = append(failure, e); }
                try { if (statement != null) statement.close(); } catch (SQLException e) { failure = append(failure, e); }
                try { control.execute("COMMIT"); } catch (SQLException e) { failure = append(failure, e); }
                try { control.close(); } catch (SQLException e) { failure = append(failure, e); }
                synchronized (readers) {
                    if (initialized && failure == null && !closing) readers.add(connection);
                    else try { connection.close(); } catch (SQLException e) { failure = append(failure, e); }
                }
                if (failure != null) throw failure;
            }
        }
    }

    private static SQLException append(SQLException failure, SQLException next) {
        if (failure == null) return next;
        failure.addSuppressed(next);
        return failure;
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
