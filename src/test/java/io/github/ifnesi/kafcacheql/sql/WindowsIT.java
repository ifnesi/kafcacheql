package io.github.ifnesi.kafcacheql.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import io.github.ifnesi.kafcacheql.testkit.LogCapture;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WindowsIT {
    @TempDir Path dir;
    @BeforeAll static void stack() { TestStack.requireUp(); }

    private DriverConfig config(String... settings) throws SQLException {
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("cache.db").toString());
        p.setProperty("append.default.window", "all");
        for (int i = 0; i < settings.length; i += 2) p.setProperty(settings[i], settings[i + 1]);
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }

    private static String q(String name) { return StorageDdl.quote(name); }
    private static void send(String topic, int partition, String key, String value, Long timestamp) {
        TestStack.send(topic, partition, timestamp, TestStack.utf8(key), TestStack.utf8(value), null);
    }
    private static QueryPipeline.Result query(CacheContext.Lease cache, String sql) throws SQLException {
        return query(cache, sql, new QueryPipeline.Hooks() {});
    }
    private static QueryPipeline.Result query(CacheContext.Lease cache, String sql, QueryPipeline.Hooks hooks) throws SQLException {
        try (var execution = new Execution(Duration.ofSeconds(30))) {
            return QueryPipeline.execute(cache, sql, Map.of(), execution, hooks);
        }
    }
    private static List<String> rows(QueryPipeline.Result result) {
        return result.rows().stream().map(row -> row.stream().map(String::valueOf).collect(Collectors.joining("|"))).toList();
    }
    private static void sync(CacheContext.Lease cache, String topic) throws SQLException {
        try (var execution = new Execution(Duration.ofSeconds(30))) {
            execution.run(false, () -> cache.sync(cache.plan(topic, execution), execution));
        }
    }

    /** PLAN §8.3: CreateTime has no timestamp upper bound; LogAppendTime bounds are exact;
     * offset windows are limited to log start and the captured end;
     * §8.4: "start = max(offsetsForTimes(now − window), end − max.records)"; a partition-only bound keeps it.
     */
    @Test void windowsFollowTimestampsOffsetsAndDefaultLimits() throws Exception {
        String create = TestStack.uniqueTopic("window-create"), append = TestStack.uniqueTopic("window-append");
        TestStack.createTopic(create, 1, Map.of());
        TestStack.createTopic(append, 1, Map.of("message.timestamp.type", "LogAppendTime"));
        long base = System.currentTimeMillis() - 120_000;
        long[] times = {base, base + 6000, base + 4000, base + 9000};
        for (int i = 0; i < times.length; i++) send(create, 0, "k" + i, "v" + i, times[i]);
        for (int i = 0; i < 4; i++) send(append, 0, "k" + i, "v" + i, base);
        String ct = "SELECT _offset FROM " + q(create), lat = "SELECT _offset FROM " + q(append);
        try (var cache = CacheContext.acquire(config())) {
            assertEquals(List.of("0", "2"), rows(query(cache, ct + " WHERE _timestamp<" + (base + 5000) + " ORDER BY _offset")));
            send(create, 0, "late", "late", base + 1000);
            assertEquals(List.of("0", "2", "4"), rows(query(cache, ct + " WHERE _timestamp<" + (base + 5000) + " ORDER BY _offset")));
            assertEquals(List.of("0", "2", "4"), rows(query(cache, ct + " WHERE _timestamp<=" + (base + 4000) + " ORDER BY _offset")));
            assertEquals(List.of("1", "2", "3"), rows(query(cache, ct + " WHERE _timestamp>=" + (base + 4000) + " ORDER BY _offset")));
            assertEquals(List.of("1", "2", "3"), rows(query(cache, ct + " WHERE _offset BETWEEN 1 AND 3 ORDER BY _offset")));
            assertTrue(query(cache, ct + " WHERE _offset>=999").rows().isEmpty());
            assertTrue(query(cache, ct + " WHERE _offset<0").rows().isEmpty());
            assertTrue(query(cache, ct + " WHERE _timestamp>=" + (System.currentTimeMillis() + 3_600_000)).rows().isEmpty());
            var kafka = TestStack.readAll(append, true);
            long cut = kafka.get(1).timestamp();
            assertEquals(kafka.stream().filter(r -> r.timestamp() <= cut).map(r -> Long.toString(r.offset())).toList(),
                    rows(query(cache, lat + " WHERE _timestamp<=" + cut + " ORDER BY _offset")));
            assertEquals(kafka.stream().filter(r -> r.timestamp() > cut).map(r -> Long.toString(r.offset())).toList(),
                    rows(query(cache, lat + " WHERE _timestamp>" + cut + " ORDER BY _offset")));
        }

        String defaults = TestStack.uniqueTopic("window-default");
        TestStack.createTopic(defaults, 2, Map.of());
        long now = System.currentTimeMillis();
        for (int partition = 0; partition < 2; partition++) {
            send(defaults, partition, "old", "old", now - 120_000);
            send(defaults, partition, "a", "recent-a", now - 10_000);
            send(defaults, partition, "b", "recent-b", now - 5000);
        }
        String sql = "SELECT _partition,_offset FROM " + q(defaults) + " ORDER BY _partition,_offset";
        try (var time = CacheContext.acquire(config("append.default.window", "30s", "append.default.window.max.records", "100"));
             var records = CacheContext.acquire(config("append.default.window", "1h", "append.default.window.max.records", "100",
                     "topic." + defaults + ".append.default.window.max.records", "1"));
             var all = CacheContext.acquire(config("append.default.window", "30s", "append.default.window.max.records", "1",
                     "topic." + defaults + ".append.default.window", "all"))) {
            var timeResult = query(time, sql);
            assertEquals(List.of("0|1", "0|2", "1|1", "1|2"), rows(timeResult));
            assertEquals(defaults + ": only records from the last 30s are read, up to 100 per partition; add a _timestamp or _offset condition to read more.",
                    timeResult.warnings().getFirst().getMessage());
            assertEquals(List.of("0|2", "1|2"), rows(query(records, sql)));
            var partitionOnly = query(records, "SELECT _partition,_offset FROM " + q(defaults) + " WHERE _partition=1 ORDER BY _offset");
            assertEquals(List.of("1|2"), rows(partitionOnly));
            assertTrue(partitionOnly.warnings().getFirst().getMessage().contains("last 1h are read, up to 1 per partition"));
            var allResult = query(all, sql);
            assertEquals(List.of("0|0", "0|1", "0|2", "1|0", "1|1", "1|2"), rows(allResult));
            assertTrue(allResult.warnings().isEmpty());
            assertTrue(query(records, "SELECT * FROM " + q(defaults) + " WHERE _offset>=0").warnings().isEmpty());
        }
    }

    /** PLAN §8.5: "Every append-topic reference is replaced by a subquery limited to ... the end offset
     * captured at query start", so earlier cached ranges and concurrent additions never leak into rows.
     */
    @Test void rewriteKeepsEachReferenceInsideItsOwnWindow() throws Exception {
        String topic = TestStack.uniqueTopic("rewrite.orders"), many = TestStack.uniqueTopic("rewrite-many");
        TestStack.createTopic(topic, 2, Map.of());
        TestStack.createTopic(many, 33, Map.of());
        for (int p = 0; p < 2; p++) for (int n = 0; n < 10; n++) send(topic, p, "k" + n, "v" + n, null);
        for (int p = 0; p < 33; p++) send(many, p, "key", "value", null);
        try (var cache = CacheContext.acquire(config("append.default.window", "1h", "append.default.window.max.records", "2"));
             var other = CacheContext.acquire(config())) {
            sync(other, topic); // Warm the entire cache, including rows older than this query's default.
            String sql = "SELECT _partition,_offset FROM " + q(topic) + " ORDER BY _partition,_offset";
            AtomicInteger attempts = new AtomicInteger();
            var result = query(cache, sql, new QueryPipeline.Hooks() {
                @Override public void afterSync(List<QueryPipeline.Window> windows) throws SQLException {
                    attempts.incrementAndGet();
                    for (int p = 0; p < 2; p++) send(topic, p, "new", "new", null);
                    sync(other, topic); // Add rows newer than the end already captured for this query.
                }
            });
            assertEquals(List.of("0|8", "0|9", "1|8", "1|9"), rows(result));
            assertEquals(1, attempts.get());
            String joined = "WITH recent AS (SELECT x.* FROM " + q(topic) + " x) "
                    + "SELECT r._partition,r._offset,y._offset FROM recent r JOIN " + q(topic)
                    + " y ON r._partition=y._partition WHERE y._offset BETWEEN 0 AND 1 ORDER BY 1,2,3";
            assertEquals(List.of("0|9|0", "0|9|1", "0|10|0", "0|10|1", "1|9|0", "1|9|1", "1|10|0", "1|10|1"),
                    rows(query(cache, joined)));
            var expected = new ArrayList<String>();
            for (int p = 0; p < 33; p++) expected.add(p + "|0");
            assertEquals(expected, rows(query(cache, "SELECT _partition,_offset FROM " + q(many)
                    + " WHERE _offset>=0 ORDER BY _partition")));
        }
    }

    /** PLAN §10.1 Readers: check generation and range coverage before preparing on the same read
     * snapshot; "One replan per query"; compacted partitions may be newer but must reach their synced end.
     */
    @Test void lostWindowsReplanOnceAndCompactedReadsKeepTheirSnapshot() throws Exception {
        String rebuilt = TestStack.uniqueTopic("read-rebuild"), pruned = TestStack.uniqueTopic("read-prune");
        for (String topic : List.of(rebuilt, pruned)) {
            TestStack.createTopic(topic, 1, Map.of());
            for (int n = 0; n < 3; n++) send(topic, 0, "k" + n, "old" + n, null);
        }
        try (var logs = LogCapture.capture();
             var cache = CacheContext.acquire(config()); var other = CacheContext.acquire(config())) {
            AtomicInteger rebuilds = new AtomicInteger();
            var replacement = query(cache, "SELECT _value FROM " + q(rebuilt) + " WHERE _offset>=0", new QueryPipeline.Hooks() {
                @Override public void afterSync(List<QueryPipeline.Window> windows) throws SQLException {
                    if (rebuilds.incrementAndGet() != 1) return;
                    TestStack.recreateTopic(rebuilt, 1, Map.of());
                    send(rebuilt, 0, "key", "replacement", null);
                    sync(other, rebuilt);
                }
            });
            assertEquals(List.of("\"replacement\""), rows(replacement)); assertEquals(2, rebuilds.get());
            AtomicInteger prunes = new AtomicInteger();
            var retained = query(cache, "SELECT _offset FROM " + q(pruned) + " WHERE _offset>=0 ORDER BY _offset", new QueryPipeline.Hooks() {
                @Override public void afterSync(List<QueryPipeline.Window> windows) throws SQLException {
                    if (prunes.incrementAndGet() != 1) return;
                    prune(pruned, 1); sync(other, pruned);
                }
            });
            assertEquals(List.of("1", "2"), rows(retained)); assertEquals(2, prunes.get());
            AtomicInteger losses = new AtomicInteger();
            SQLException lost = assertThrows(SQLException.class, () -> query(cache,
                    "SELECT a._value,b._value FROM " + q(rebuilt) + " a JOIN " + q(pruned)
                            + " b ON 1=1 WHERE a._offset>=0 AND b._offset>=0", new QueryPipeline.Hooks() {
                        @Override public void afterSync(List<QueryPipeline.Window> windows) throws SQLException {
                            if (losses.incrementAndGet() == 1) {
                                TestStack.recreateTopic(rebuilt, 1, Map.of()); send(rebuilt, 0, "key", "newer", null); sync(other, rebuilt);
                            } else { prune(pruned, 2); sync(other, pruned); }
                        }
                    }));
            assertEquals(2, losses.get()); assertEquals("40001", lost.getSQLState()); assertTrue(lost.getMessage().contains(pruned));

            String compacted = TestStack.uniqueTopic("read-compacted");
            TestStack.createTopic(compacted, 2, Map.of("cleanup.policy", "compact"));
            for (int p = 0; p < 2; p++) send(compacted, p, "key", "old" + p, null);
            AtomicInteger committedPartition = new AtomicInteger(-1);
            try (var child = new Child(cache.path(), compacted)) {
                var mixed = query(cache, "SELECT _partition,_offset,_value FROM " + q(compacted)
                        + " WHERE _offset>=0 ORDER BY _partition", new QueryPipeline.Hooks() {
                    @Override public void afterSync(List<QueryPipeline.Window> windows) throws SQLException {
                        for (int p = 0; p < 2; p++) send(compacted, p, "key", "new" + p, null);
                        child.release();
                        String line = child.line();
                        assertTrue(line.startsWith("COMMITTED "), line);
                        committedPartition.set(Integer.parseInt(line.substring(10)));
                    }
                    @Override public void afterCheck(Connection reader) throws SQLException {
                        child.release(); child.done(); // Finish writes while the reader holds its checked snapshot.
                    }
                });
                assertEquals(committedPartition.get() == 0 ? List.of("0|1|\"new0\"", "1|0|\"old1\"")
                        : List.of("0|0|\"old0\"", "1|1|\"new1\""), rows(mixed));
                assertEquals(List.of("0|1|\"new0\"", "1|1|\"new1\""), rows(query(cache,
                        "SELECT _partition,_offset,_value FROM " + q(compacted) + " WHERE _offset>=0 ORDER BY _partition")));
            }
            assertEquals(2, logs.lines().stream().filter(line -> line.startsWith(
                    "WARN io.github.ifnesi.kafcacheql.cache.TopicRegistry: Rebuilding topic " + rebuilt + ": topic ID changed from ")).count());
        }
    }

    private static void prune(String topic, long before) {
        try { TestStack.admin().deleteRecords(Map.of(new TopicPartition(topic, 0), RecordsToDelete.beforeOffset(before)))
                .all().get(20, TimeUnit.SECONDS); }
        catch (Exception e) { throw new AssertionError(e); }
    }

    private static final class Child implements AutoCloseable {
        private final Process process;
        private final BufferedReader output;
        Child(Path file, String topic) throws Exception {
            process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-Xmx1g", "-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"), "-cp",
                    System.getProperty("java.class.path"), WindowProcess.class.getName(), file.toString(), topic)
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            assertEquals("READY", line());
        }
        void release() throws SQLException {
            try { process.getOutputStream().write('\n'); process.getOutputStream().flush(); }
            catch (java.io.IOException e) { throw new SQLException(e); }
        }
        String line() throws SQLException {
            try { return CompletableFuture.supplyAsync(() -> {
                try { return output.readLine(); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
            }).get(20, TimeUnit.SECONDS); }
            catch (Exception e) { throw new SQLException("Waiting for competing sync", e); }
        }
        void done() throws SQLException {
            assertTrue(line().startsWith("DONE "));
            try { assertTrue(process.waitFor(20, TimeUnit.SECONDS)); assertEquals(0, process.exitValue()); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new SQLException(e); }
        }
        @Override public void close() throws java.io.IOException { process.destroyForcibly(); output.close(); }
    }
}
