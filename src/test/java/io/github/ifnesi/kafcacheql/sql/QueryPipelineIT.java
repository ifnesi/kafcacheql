package io.github.ifnesi.kafcacheql.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncBatch;
import io.github.ifnesi.kafcacheql.sync.SyncPlan;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryPipelineIT {
    private static final Schema VALUE = new Schema.Parser().parse("""
            {"type":"record","name":"PipelineValue","fields":[{"name":"n","type":"int"}]}
            """);
    private static final List<List<Object>> INITIAL = List.of(List.of(0, 6, 15), List.of(1, 6, 15));
    @TempDir Path dir;
    @BeforeAll static void stack() { TestStack.requireUp(); }

    /** PLAN §8.2: fetch "only the missing ranges", then execute on a checked reader;
     * §9/§11 (maintainer's 0011a decision): live topic description and start/end offsets per query,
     * topic config once per connection/topic, successful results only; §9: a recreated topic is rebuilt
     * with its current config; §10/0011a: progress per partition at a fixed interval.
     * The fake monotonic clock advances one second per real Kafka batch, avoiding elapsed-time sleeps.
     */
    @Test void appendAndCompactedQueriesCountCallsFetchOnlyNewRecordsAndLogPeriodicProgress() throws Exception {
        for (String policy : List.of("delete", "compact")) {
            String topic = topic(policy);
            populate(topic);
            var config = PipelineProcess.config(dir.resolve(policy + ".db"));
            try (var clients = KafkaClients.acquire(config); var calls = new Calls(clients.clients());
                 var cache = CacheContext.acquire(config); var logs = LogCapture.capture()) {
                AtomicInteger fetched = new AtomicInteger(), ranges = new AtomicInteger();
                AtomicLong time = new AtomicLong();
                AtomicBoolean firstQuery = new AtomicBoolean(true);
                Map<Integer, AtomicInteger> commits = new ConcurrentHashMap<>();
                cache.hooks(new SyncWorker.Hooks() {
                    // Used by the worker's progress clock; does not affect the execution deadline.
                    @Override public long nanoTime() { return time.get(); }
                    @Override public void beforeFetch(SyncPlan plan) { ranges.incrementAndGet(); }
                    @Override public void afterCommit(SyncBatch batch) {
                        int count = commits.computeIfAbsent(batch.partition(), ignored -> new AtomicInteger()).incrementAndGet();
                        if (firstQuery.get() && count == 6) {
                            assertEquals(1, logs.lines().stream().filter(l -> l.startsWith("INFO ")
                                    && l.contains(topic + " partition " + batch.partition() + ":")).count(),
                                    "the five-second report appears before the final commit, not just at completion");
                        }
                        fetched.addAndGet(batch.records().size());
                        time.addAndGet(TimeUnit.SECONDS.toNanos(1));
                    }
                });
                calls.clear();
                assertEquals(INITIAL, query(cache, topic).rows());
                firstQuery.set(false);
                assertEquals(12, fetched.get()); assertEquals(2, ranges.get());
                assertEquals(1, calls.count("describeConfigs"), "resolution and planning share this connection's config");
                assertEquals(2, calls.count("listOffsets"));
                List<String> progress = logs.lines().stream().filter(l -> l.startsWith("INFO ")
                        && l.contains(topic + " partition ") && l.contains("records fetched")).toList();
                assertEquals(2, progress.size(), progress.toString());
                for (int p = 0; p < 2; p++) {
                    String expected = topic + " partition " + p + ": 6 records fetched, 0 offsets remaining";
                    assertTrue(progress.stream().anyMatch(l -> l.endsWith(expected)), progress.toString());
                }
                for (int p = 0; p < 2; p++) {
                    send(topic, p, "k0", 100);
                    send(topic, p, "k6", 106);
                    send(topic, p, "k1", null);
                }
                fetched.set(0); ranges.set(0); calls.clear();
                assertEquals(policy.equals("delete") ? List.of(List.of(0, 9, 221), List.of(1, 9, 221))
                        : List.of(List.of(0, 6, 220), List.of(1, 6, 220)), query(cache, topic).rows());
                assertEquals(6, fetched.get()); assertEquals(2, ranges.get());
                calls.assertWarm();
                fetched.set(0); ranges.set(0); calls.clear();
                var unchanged = query(cache, topic);
                assertEquals(policy.equals("delete") ? 9 : 6, unchanged.rows().getFirst().get(1));
                assertEquals(0, fetched.get()); assertEquals(0, ranges.get());
                calls.assertWarm();
                // Same cache and admin client, a new connection: config must be read once again.
                calls.clear();
                try (var second = CacheContext.acquire(config)) {
                    calls.failNextConfig.set(true);
                    SQLException failure = assertThrows(SQLException.class, () -> query(second, topic));
                    assertTrue(failure.getMessage().contains("config lookup timeout"), failure.toString());
                    assertEquals(1, calls.count("describeConfigs"));
                    calls.clear();
                    assertEquals(unchanged.rows(), query(second, topic).rows());
                    assertEquals(1, calls.count("describeTopics"));
                    assertEquals(1, calls.count("describeConfigs"), "a failed lookup must not poison the connection");
                    assertEquals(2, calls.count("listOffsets"));
                    calls.clear();
                    assertEquals(unchanged.rows(), query(second, topic).rows());
                    calls.assertWarm();
                }
                long generation = cache.read(c -> TopicRegistry.generation(c, topic).orElseThrow());
                String replacementPolicy = policy.equals("delete") ? "compact" : "delete";
                TestStack.recreateTopic(topic, 2, Map.of("cleanup.policy", replacementPolicy, "delete.retention.ms", "600000"));
                for (int p = 0; p < 2; p++) {
                    send(topic, p, "same", 7);
                    send(topic, p, "same", 9);
                }
                fetched.set(0); ranges.set(0); calls.clear();
                var replacement = query(cache, topic);
                assertEquals(replacementPolicy.equals("compact") ? List.of(List.of(0, 1, 9), List.of(1, 1, 9))
                        : List.of(List.of(0, 2, 16), List.of(1, 2, 16)), replacement.rows());
                assertEquals(4, fetched.get()); assertEquals(2, ranges.get());
                assertTrue(cache.read(c -> TopicRegistry.generation(c, topic).orElseThrow()) > generation);
                assertEquals(600000L, cache.read(c -> {
                    try (var s = c.prepareStatement("SELECT delete_retention_ms FROM \"$topics\" WHERE topic=?")) {
                        s.setString(1, topic);
                        try (var r = s.executeQuery()) { assertTrue(r.next()); return r.getLong(1); }
                    }
                }).longValue());
                assertEquals(1, calls.count("describeConfigs"), "a new topic ID must read new configs");
                assertEquals(1, calls.count("describeTopics")); assertEquals(2, calls.count("listOffsets"));
                fetched.set(0); ranges.set(0); calls.clear();
                assertEquals(replacement.rows(), query(cache, topic).rows());
                assertEquals(0, fetched.get()); assertEquals(0, ranges.get());
                calls.assertWarm();
            }
        }
    }

    /** PLAN §10.1: "Two processes syncing one topic at once both fetch it, and each offset is stored once";
     * 0011a acceptance: "two JVMs querying one cache file at the same time".
     * Both whole queries have fetched a batch before either can write it.
     */
    @Test void twoJvmsRunOverlappingWholeQueriesThroughTheSameTypedViews() throws Exception {
        String topic = topic("concurrent"); populate(topic);
        var config = PipelineProcess.config(dir.resolve("concurrent.db"));
        CountDownLatch buffered = new CountDownLatch(1), release = new CountDownLatch(1);
        try (var cache = CacheContext.acquire(config)) {
            cache.hooks(new SyncWorker.Hooks() {
                boolean first = true;
                @Override public void beforeCommit(SyncBatch batch) throws SQLException {
                    if (!first) return;
                    first = false; buffered.countDown();
                    try { if (!release.await(20, TimeUnit.SECONDS)) throw new SQLException("Query release timed out"); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException(e); }
                }
            });
            var local = CompletableFuture.supplyAsync(() -> {
                try { return query(cache, topic); } catch (SQLException e) { throw new CompletionException(e); }
            });
            assertTrue(buffered.await(10, TimeUnit.SECONDS));
            Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                    System.getProperty("java.class.path"), PipelineProcess.class.getName(), cache.path().toString(), topic)
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try (var output = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
                assertEquals("BUFFERED", line(output));
                child.getOutputStream().write('\n'); child.getOutputStream().flush(); release.countDown();
                assertEquals(INITIAL, local.get(20, TimeUnit.SECONDS).rows());
                assertEquals("RESULT " + INITIAL, line(output));
                assertTrue(child.waitFor(20, TimeUnit.SECONDS)); assertEquals(0, child.exitValue());
                assertEquals(INITIAL, query(cache, topic).rows());
            } finally { release.countDown(); child.destroyForcibly(); }
        } finally { release.countDown(); }
    }

    private static String line(BufferedReader output) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try { return output.readLine(); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
        }).get(20, TimeUnit.SECONDS);
    }
    private static QueryPipeline.Result query(CacheContext.Lease cache, String topic) throws SQLException {
        try (var execution = new Execution(Duration.ofSeconds(30))) {
            return QueryPipeline.execute(cache, PipelineProcess.sql(topic), Map.of(), execution);
        }
    }
    private static String topic(String policy) {
        String topic = TestStack.uniqueTopic("pipeline-" + policy);
        TestStack.createTopic(topic, 2, Map.of("cleanup.policy", policy.equals("compact") ? "compact" : "delete"));
        return topic;
    }
    private static void populate(String topic) {
        for (int p = 0; p < 2; p++) for (int n = 0; n < 6; n++) send(topic, p, "k" + n, n);
    }
    private static void send(String topic, int partition, String key, Integer n) {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        GenericData.Record value = null;
        if (n != null) { value = new GenericData.Record(VALUE); value.put("n", n); }
        byte[] k = TestStack.serializeAvro(topic, headers, key, TestStack.SchemaIdIn.PREFIX, true);
        byte[] v = TestStack.serializeAvro(topic, headers, value, TestStack.SchemaIdIn.PREFIX, false);
        TestStack.send(topic, partition, null, k, v, headers);
    }

    /** Counts real Admin calls, with one injectable config timeout to exercise failure recovery. */
    private static final class Calls implements AutoCloseable {
        private final KafkaClients clients;
        private final Field field;
        private final Admin original;
        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        private final AtomicBoolean failNextConfig = new AtomicBoolean();
        Calls(KafkaClients clients) throws Exception {
            this.clients = clients;
            field = KafkaClients.class.getDeclaredField("admin"); field.setAccessible(true);
            original = (Admin) field.get(clients);
            field.set(clients, Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class}, (proxy, method, args) -> {
                calls.computeIfAbsent(method.getName(), ignored -> new AtomicInteger()).incrementAndGet();
                if (method.getName().equals("describeConfigs") && failNextConfig.getAndSet(false)) {
                    var resource = ((java.util.Collection<?>) args[0]).stream().map(ConfigResource.class::cast).findFirst().orElseThrow();
                    var failed = new KafkaFutureImpl<Config>();
                    failed.completeExceptionally(new TimeoutException("config lookup timeout"));
                    return new DescribeConfigsResult(Map.of(resource, failed)) {};
                }
                try { return method.invoke(original, args); }
                catch (InvocationTargetException e) { throw e.getCause(); }
            }));
        }
        void clear() { calls.clear(); }
        int count(String method) { var count = calls.get(method); return count == null ? 0 : count.get(); }
        void assertWarm() {
            assertEquals(Map.of("describeTopics", 1, "listOffsets", 2), calls.entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get())));
        }
        @Override public void close() throws IllegalAccessException { field.set(clients, original); }
    }
}
