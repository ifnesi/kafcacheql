package io.github.ifnesi.kafcacheql.driver;

import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import io.github.ifnesi.kafcacheql.sync.SyncPlan;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.junit.jupiter.api.Test;

/** PLAN §16/§18 and 0013c: measure JDBC queries, fetch metrics and heap, without performance thresholds.
 * Deliberately matches neither *Test nor *IT: see docs/performance.md for the opt-in command.
 */
class PerformanceMeasurement {
    private static final String URL = "jdbc:kafcacheql://" + TestStack.BOOTSTRAP;
    private static final int PARTITIONS = 3, DELTA = 1_000;
    private static final String CSV_HEADER = "topic,query,seconds,rows,committed_records,batches,planning_s,fetch_decode_wait_s,commit_s,after_sync_s,checkpoint_s,db_bytes,wal_bytes,kafka_bytes,kafka_records,heap_before_bytes,heap_after_bytes,admin_calls";
    private static final Schema VALUE = new Schema.Parser().parse("""
            {"type":"record","name":"PerformanceValue","fields":[
              {"name":"id","type":"long"}, {"name":"status","type":"string"},
              {"name":"amount","type":"double"}, {"name":"active","type":"boolean"}]}
            """);

    @Test void measure() throws Exception {
        TestStack.requireUp();
        Class.forName(KafCacheQLDriver.class.getName());
        Path output = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "performance-");
        System.out.printf("PERF output=%s processors=%d maxHeapBytes=%d java=%s%n", output,
                Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory(), Runtime.version());
        List<String> created = new ArrayList<>();
        try (var report = new PrintWriter(Files.newBufferedWriter(output.resolve("queries.csv")))) {
            report.println(CSV_HEADER);
            for (boolean compacted : List.of(true, false)) {
                String mode = compacted ? "compact" : "append";
                String topic = TestStack.uniqueTopic("perf-" + mode);
                TestStack.createTopic(topic, PARTITIONS, Map.of("cleanup.policy", compacted ? "compact" : "delete"));
                created.add(topic);
                int records = compacted ? 1_000_000 : 3_000_000;
                try (var producer = producer()) {
                    long timestampBase = System.currentTimeMillis() - records * 2L - 60_000;
                    produce(producer, topic, 0, records, false, timestampBase);
                    if (!compacted) defaultWindow(topic, records, timestampBase, output, report);
                    Path cache = output.resolve(mode + ".db");
                    Properties p = settings(cache);
                    var config = DriverConfig.load(URL, p);
                    String sql = aggregate(topic) + " WHERE _offset>=0";
                    long opening = System.nanoTime();
                    try (var connection = DriverManager.getConnection(URL, p);
                         var control = CacheContext.acquire(config);
                         var clients = KafkaClients.acquire(config);
                         var calls = new Calls(clients.clients())) {
                        System.out.printf("PERF %s connection_open_s=%.3f%n", mode, seconds(System.nanoTime() - opening));
                        query(connection, control, calls, topic, "first", sql, 0, records, 0, records, cache, output, report, true);
                        query(connection, control, calls, topic, "unchanged", sql, 0, records, 0, 0, cache, output, report, false);
                        produce(producer, topic, compacted ? 0 : records, DELTA, true,
                                compacted ? timestampBase + records * 2L : timestampBase);
                        query(connection, control, calls, topic, "delta", sql, 0,
                                compacted ? records : records + DELTA, DELTA, DELTA, cache, output, report, false);
                    }
                    System.out.printf("PERF %s closed_cache_bytes=%d wal_bytes=%d%n", mode, size(cache), size(Path.of(cache + "-wal")));
                    p.setProperty("offline", "true");
                    try (var connection = DriverManager.getConnection("jdbc:kafcacheql://", p);
                         var control = CacheContext.acquire(DriverConfig.load("jdbc:kafcacheql://", p))) {
                        assertTrue(FetchMetrics.ACTIVE.isEmpty(), "offline opens no Kafka clients");
                        query(connection, control, null, topic, "offline", sql, 0,
                                compacted ? records : records + DELTA, DELTA, 0, cache, output, report, false);
                    }
                }
            }
        } finally {
            // Only these exact names, created by this invocation; never discover/delete other topics.
            Exception failure = null;
            for (String topic : created) {
                try { TestStack.deleteTopic(topic); System.out.println("PERF deleted " + topic); }
                catch (Exception e) { if (failure == null) failure = e; else failure.addSuppressed(e); }
            }
            if (failure != null) throw failure;
        }
    }

    /** 0016c / PLAN §10: adopt parallel fetching only when it beats one consumer on the same data. */
    @Test void parallelFetch() throws Exception {
        TestStack.requireUp(); Class.forName(KafCacheQLDriver.class.getName());
        Path output = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "performance-parallel-");
        System.out.printf("PERF output=%s processors=%d maxHeapBytes=%d java=%s%n", output,
                Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory(), Runtime.version());
        String topic = TestStack.uniqueTopic("perf-parallel");
        TestStack.createTopic(topic, 6, Map.of("cleanup.policy", "delete"));
        try (var report = new PrintWriter(Files.newBufferedWriter(output.resolve("queries.csv")));
             var producer = producer()) {
            report.println(CSV_HEADER);
            produce(producer, topic, 0, 3_000_000, false, System.currentTimeMillis() - 6_060_000, 6);
            var seconds = new TreeMap<Integer, Double>();
            // Warm both configurations before timing full first syncs, each into a fresh cache.
            for (boolean warmup : List.of(true, false)) for (int parallelism : List.of(1, 4)) {
                String label = (warmup ? "warmup-" : "first-") + parallelism;
                Path cache = output.resolve(label + ".db");
                var p = settings(cache); p.setProperty("sync.fetch.parallelism", Integer.toString(parallelism));
                var config = DriverConfig.load(URL, p);
                try (var connection = DriverManager.getConnection(URL, p); var control = CacheContext.acquire(config);
                     var clients = KafkaClients.acquire(config); var calls = new Calls(clients.clients())) {
                    long records = warmup ? 60_000 : 3_000_000;
                    String sql = aggregate(topic) + " WHERE _offset>=0" + (warmup ? " AND _offset<10000" : "");
                    double elapsed = query(connection, control, calls, topic, label, sql, 0, records, 0, records,
                            cache, output, report, !warmup);
                    if (!warmup) seconds.put(parallelism, elapsed);
                }
            }
            System.out.println("PERF parallel_first_sync_seconds=" + seconds);
            assertTrue(seconds.get(4) < seconds.get(1), "Four consumers did not beat one; stop 0016c and report the measurements");
        } finally { TestStack.deleteTopic(topic); System.out.println("PERF deleted " + topic); }
    }

    private static Properties settings(Path cache) {
        var p = new Properties();
        p.setProperty("cache.path", cache.toString());
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        p.setProperty("metric.reporters", FetchMetrics.class.getName());
        return p;
    }

    private static KafkaProducer<String, GenericData.Record> producer() {
        var p = new Properties();
        p.setProperty("bootstrap.servers", TestStack.BOOTSTRAP);
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        p.setProperty("key.serializer", StringSerializer.class.getName());
        p.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        p.setProperty("acks", "all");
        p.setProperty("linger.ms", "5");
        return new KafkaProducer<>(p);
    }

    private static void produce(KafkaProducer<String, GenericData.Record> producer, String topic,
                                int start, int count, boolean updated, long timestampBase) {
        produce(producer, topic, start, count, updated, timestampBase, PARTITIONS);
    }

    private static void produce(KafkaProducer<String, GenericData.Record> producer, String topic,
                                int start, int count, boolean updated, long timestampBase, int partitions) {
        long begin = System.nanoTime();
        var failure = new AtomicReference<Exception>();
        for (int i = 0; i < count; i++) {
            long id = (long) start + i;
            var value = new GenericData.Record(VALUE);
            value.put("id", id); value.put("status", updated ? "updated" : "new");
            value.put("amount", id / 100.0); value.put("active", id % 2 == 0);
            // Explicit assignment makes every partition's load and default window reproducible.
            producer.send(new ProducerRecord<>(topic, (int) (id % partitions), timestampBase + id * 2, "key-" + id, value),
                    (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
            if ((i + 1) % 100_000 == 0 || i + 1 == count) {
                producer.flush();
                if (failure.get() != null) throw new IllegalStateException("Producing " + topic, failure.get());
                System.out.printf("PERF seeded %s %,d/%,d elapsed_s=%.3f%n", topic, i + 1, count, seconds(System.nanoTime() - begin));
            }
        }
    }

    private static double query(Connection connection, CacheContext.Lease control, Calls calls, String topic,
                              String label, String sql, long firstId, long expectedRows, long expectedUpdated, long expectedFetched,
                              Path cache, Path output, PrintWriter report, boolean profile) throws Exception {
        var timings = new Timings(label);
        control.hooks(timings); if (calls != null) calls.reset();
        try (var recording = profile ? new Recording(Configuration.getConfiguration("profile")) : null) {
            if (recording != null) recording.start();
            var before = FetchMetrics.snapshot(); long heapBefore = heapUsed();
            long begin = System.nanoTime();
            long rows = aggregate(connection, sql, firstId, expectedRows, expectedUpdated);
            long end = System.nanoTime();
            assertEquals(expectedFetched, timings.fetched);
            write(report, topic, label, begin, end, rows, timings, cache, calls, before, heapBefore);
            if (recording != null) { recording.stop(); recording.dump(output.resolve(topic + "-" + label + ".jfr")); }
            return seconds(end - begin);
        }
    }

    private static String aggregate(String topic) {
        return "SELECT count(*),sum(id),sum(CASE WHEN status='updated' THEN 1 ELSE 0 END) FROM " + StorageDdl.quote(topic);
    }

    private static long aggregate(Connection connection, String sql, long firstId, long expectedRows, long expectedUpdated) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertTrue(result.next()); long rows = result.getLong(1);
            assertEquals(expectedRows, rows);
            assertEquals(expectedRows * (2 * firstId + expectedRows - 1) / 2, result.getLong(2));
            assertEquals(expectedUpdated, result.getLong(3)); assertFalse(result.next()); return rows;
        }
    }

    /** Cold, ordinary SELECT (no bounds): the §18 large-append/default-window criterion, without a GUI. */
    private static void defaultWindow(String topic, int records, long timestampBase, Path output, PrintWriter report) throws Exception {
        Path cache = output.resolve("append-default.db");
        var p = settings(cache); p.setProperty("append.default.window", "1h");
        var config = DriverConfig.load(URL, p);
        try (var connection = DriverManager.getConnection(URL, p); var control = CacheContext.acquire(config);
             var clients = KafkaClients.acquire(config); var calls = new Calls(clients.clients());
             var recording = new Recording(Configuration.getConfiguration("profile"))) {
            var timings = new Timings("default"); control.hooks(timings); calls.reset();
            recording.start();
            var before = FetchMetrics.snapshot(); long heapBefore = heapUsed();
            long begin = System.nanoTime(), rows = 0;
            try (var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT * FROM " + StorageDdl.quote(topic))) {
                while (result.next()) { result.getLong("id"); result.getString("status"); rows++; }
                assertNotNull(statement.getWarnings());
            }
            long end = System.nanoTime();
            assertEquals(30_000, rows); assertEquals(rows, timings.fetched);
            write(report, topic, "default", begin, end, rows, timings, cache, calls, before, heapBefore);
            recording.stop(); recording.dump(output.resolve(topic + "-default.jfr"));
            String recent = aggregate(topic) + " WHERE _timestamp>=" + (timestampBase + (records - 90_000L) * 2);
            query(connection, control, calls, topic, "timestamp", recent, records - 90_000L, 90_000, 0, 60_000,
                    cache, output, report, false);
            query(connection, control, calls, topic, "timestamp-repeat", recent, records - 90_000L, 90_000, 0, 0,
                    cache, output, report, false);
            query(connection, control, calls, topic, "historical", aggregate(topic) + " WHERE _offset>=0 AND _offset<10000",
                    0, 30_000, 0, 30_000, cache, output, report, false);
            try (var other = DriverManager.getConnection(URL, p)) {
                concurrent(connection, other, control, calls, topic, cache, report);
            }
        }
    }

    /** Two callers start together on the same uncached window; shared fetch metrics are reported once. */
    private static void concurrent(Connection first, Connection second, CacheContext.Lease control, Calls calls,
                                   String topic, Path cache, PrintWriter report) throws Exception {
        var timings = new Timings("concurrent-pair"); control.hooks(timings); calls.reset();
        var before = FetchMetrics.snapshot(); long heapBefore = heapUsed();
        var start = new CountDownLatch(1);
        try (var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var queries = new ArrayList<java.util.concurrent.Future<Long>>();
            for (var connection : List.of(first, second)) queries.add(tasks.submit(() -> {
                start.await(); long begin = System.nanoTime();
                aggregate(connection, aggregate(topic) + " WHERE _offset>=10000 AND _offset<20000", 30_000, 30_000, 0);
                return System.nanoTime() - begin;
            }));
            long begin = System.nanoTime(); start.countDown();
            var durations = new ArrayList<Double>();
            for (var query : queries) durations.add(seconds(query.get()));
            long end = System.nanoTime();
            assertEquals(30_000, timings.fetched, "the shared worker fetches the missing window once");
            write(report, topic, "concurrent-pair", begin, end, 60_000, timings, cache, calls, before, heapBefore);
            System.out.println("PERF concurrent_query_seconds=" + durations);
        }
    }

    private static void write(PrintWriter report, String topic, String label, long begin, long end, long rows,
                              Timings t, Path cache, Calls calls, Fetched before, long heapBefore) throws Exception {
        var fetched = FetchMetrics.snapshot().minus(before);
        long heapAfter = heapUsed();
        if (t.fetched == 0) {
            assertEquals(0, fetched.bytes()); assertEquals(0, fetched.records());
        } else {
            assertTrue(fetched.bytes() > 0, "consumer byte metric advances on a fetch");
            assertTrue(fetched.records() > 0, "consumer record metric advances on a fetch");
        }
        long planning = t.firstFetch == 0 ? 0 : t.firstFetch - begin;
        long fetching = t.firstFetch == 0 ? 0 : t.lastCommit - t.firstFetch - t.committing;
        long after = t.lastCommit == 0 ? end - begin : end - t.lastCommit;
        String line = String.format(Locale.ROOT, "%s,%s,%.6f,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%d,%d,%d,%d,%d,%d,\"%s\"",
                topic, label, seconds(end - begin), rows, t.fetched, t.batches, seconds(planning), seconds(fetching),
                seconds(t.committing), seconds(after), seconds(t.checkpointing), size(cache), size(Path.of(cache + "-wal")),
                fetched.bytes(), fetched.records(), heapBefore, heapAfter, calls == null ? Map.of() : calls.counts());
        report.println(line); report.flush();
        System.out.println("PERF " + line);
    }

    private static long heapUsed() { return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed(); }

    private record Fetched(long bytes, long records) {
        Fetched minus(Fetched before) { return new Fetched(bytes - before.bytes, records - before.records); }
    }

    /** Kafka's own cumulative consumer totals; exclude per-topic metrics to avoid double counting.
     * Admin reporters have neither metric and contribute zero. Never touch the consumer from the caller.
     */
    public static final class FetchMetrics implements MetricsReporter {
        private static final Set<FetchMetrics> ACTIVE = ConcurrentHashMap.newKeySet();
        private volatile KafkaMetric bytes, records;
        public FetchMetrics() {}
        @Override public void configure(Map<String, ?> config) {}
        @Override public void init(List<KafkaMetric> metrics) { metrics.forEach(this::metricChange); ACTIVE.add(this); }
        @Override public void metricChange(KafkaMetric metric) {
            var name = metric.metricName();
            if (!name.group().equals("consumer-fetch-manager-metrics") || name.tags().containsKey("topic")) return;
            switch (name.name()) {
                case "bytes-consumed-total" -> bytes = metric;
                case "records-consumed-total" -> records = metric;
                default -> { }
            }
        }
        @Override public void metricRemoval(KafkaMetric metric) {
            if (bytes == metric) bytes = null;
            if (records == metric) records = null;
        }
        @Override public void close() { ACTIVE.remove(this); }
        static Fetched snapshot() {
            long bytes = 0, records = 0;
            for (var reporter : ACTIVE) {
                var b = reporter.bytes; var r = reporter.records;
                if (b != null) bytes += ((Number) b.metricValue()).longValue();
                if (r != null) records += ((Number) r.metricValue()).longValue();
            }
            return new Fetched(bytes, records);
        }
    }

    private static double seconds(long nanos) { return nanos / 1_000_000_000.0; }
    private static long size(Path file) throws Exception { return Files.exists(file) ? Files.size(file) : 0; }

    /** Existing worker hooks: commit includes the whole SQLite transaction; checkpoint is also reported
     * separately and may overlap the fetch/decode/wait or after-sync intervals. */
    private static final class Timings implements SyncWorker.Hooks {
        final String label;
        long firstFetch, lastCommit, commitStart, checkpointStart, committing, checkpointing, fetched, batches;
        long lastProgress = System.nanoTime();
        Timings(String label) { this.label = label; }
        @Override public void beforeFetch(SyncPlan plan) { if (firstFetch == 0) firstFetch = System.nanoTime(); }
        @Override public void beforeCommit(SyncBatch batch) { commitStart = System.nanoTime(); }
        @Override public void afterCommit(SyncBatch batch) {
            lastCommit = System.nanoTime(); committing += lastCommit - commitStart;
            fetched += batch.records().size(); batches++;
            if (lastCommit - lastProgress >= 10_000_000_000L) {
                System.out.printf("PERF sync %s fetched=%,d batches=%d commit_s=%.3f%n", label, fetched, batches, seconds(committing));
                lastProgress = lastCommit;
            }
        }
        @Override public void beforeCheckpoint() { checkpointStart = System.nanoTime(); }
        @Override public void afterCheckpoint(SyncWorker.Checkpoint result) { checkpointing += System.nanoTime() - checkpointStart; }
    }

    /** As in JdbcIT, count calls on the driver's own Admin, not the producer or stack helper. */
    private static final class Calls implements AutoCloseable {
        private final KafkaClients clients;
        private final Field field;
        private final Admin original;
        private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
        Calls(KafkaClients clients) throws Exception {
            this.clients = clients;
            field = KafkaClients.class.getDeclaredField("admin"); field.setAccessible(true);
            original = (Admin) field.get(clients);
            field.set(clients, Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (self, method, args) -> {
                String name = method.getName();
                if (name.equals("listOffsets")) {
                    var spec = ((Map<?, ?>) args[0]).values().iterator().next();
                    name += spec instanceof OffsetSpec.EarliestSpec ? ":start" : spec instanceof OffsetSpec.LatestSpec ? ":end" : ":time";
                }
                counts.computeIfAbsent(name, ignored -> new AtomicLong()).incrementAndGet();
                try { return method.invoke(original, args); }
                catch (InvocationTargetException e) { throw e.getCause(); }
            }));
        }
        void reset() { counts.clear(); }
        Map<String, Long> counts() {
            var result = new TreeMap<String, Long>(); counts.forEach((name, count) -> result.put(name, count.get())); return result;
        }
        @Override public void close() throws IllegalAccessException { field.set(clients, original); }
    }
}
