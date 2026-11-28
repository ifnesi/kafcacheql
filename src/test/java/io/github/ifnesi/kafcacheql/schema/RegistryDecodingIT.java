package io.github.ifnesi.kafcacheql.schema;

import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Real Kafka ingestion; the controlled HTTP peer exposes I/O that normal Registry responses cannot hold. */
class RegistryDecodingIT {
    @TempDir Path dir;
    @BeforeAll static void stack() { TestStack.requireUp(); }

    private DriverConfig config(String file, String... settings) throws Exception {
        var p = new Properties();
        p.setProperty("cache.path", dir.resolve(file).toString());
        p.setProperty("schema.registry.url", TestStack.REGISTRY);
        for (int i = 0; i < settings.length; i += 2) p.setProperty(settings[i], settings[i + 1]);
        return DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
    }

    /** PLAN §7.3: "use.latest.version and use.schema.id are refused at connect, with a clear message."
     * Exercise the public JDBC entry point, before any client or cache is opened.
     */
    @Test void connectRefusesSchemaOverridesBecauseTheBytesNameTheSchema() {
        var driver = new io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver();
        for (var override : Map.of("use.latest.version", "true", "use.schema.id", "1").entrySet()) {
            var properties = new Properties();
            properties.setProperty(override.getKey(), override.getValue());
            SQLException failure = assertThrows(SQLException.class,
                    () -> driver.connect("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, properties));
            assertEquals("08001", failure.getSQLState());
            assertTrue(failure.getMessage().contains(override.getKey()), failure.getMessage());
            assertTrue(failure.getMessage().contains("own bytes"), failure.getMessage());
        }
    }

    /** PLAN §10.1: on cancellation "the worker stops waiting", while the decoder finishes the
     * batch and drops it; "schemas it fetched stay cached". A real Registry failure still fails (§7.1).
     * A guarded worker write must finish before the held HTTP response is released.
     */
    @Test void registryCancellationAndDeadlineReleaseTheWorkerAndAccessFailuresStillFail() throws Exception {
        String blocked = TestStack.uniqueTopic("registry-held"), plain = TestStack.uniqueTopic("after-registry");
        String denied = TestStack.uniqueTopic("registry-denied");
        for (String topic : List.of(blocked, plain, denied)) TestStack.createTopic(topic, 1, Map.of());
        TestStack.sendBytes(denied, TestStack.utf8("key"), ByteBuffer.allocate(6).put((byte) 0).putInt(54321).put((byte) 0).array(), null);
        var requested = new LinkedBlockingQueue<Boolean>();
        var requests = new java.util.concurrent.atomic.AtomicInteger();
        var release = new java.util.concurrent.atomic.AtomicReference<>(new CountDownLatch(1));
        AtomicBoolean fail = new AtomicBoolean();
        var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        try (var handlers = Executors.newVirtualThreadPerTaskExecutor()) {
            server.setExecutor(handlers);
            server.createContext("/", exchange -> {
                boolean schemaLookup = exchange.getRequestURI().getPath().startsWith("/schemas/ids/");
                if (schemaLookup) {
                    requests.incrementAndGet();
                    requested.add(true);
                }
                boolean rejected = fail.get();
                try {
                    if (!rejected && schemaLookup) release.get().await();
                    byte[] response = TestStack.utf8(rejected
                            ? "{\"error_code\":401,\"message\":\"Registry access denied\"}"
                            : schemaLookup ? "{\"schema\":\"\\\"string\\\"\",\"schemaType\":\"AVRO\"}" : "[]");
                    exchange.sendResponseHeaders(rejected ? 401 : 200, response.length);
                    exchange.getResponseBody().write(response);
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                finally { exchange.close(); }
            });
            server.start();
            try {
                for (boolean deadline : List.of(false, true)) {
                    int previous = deadline ? 1 : 0;
                    int schema = 12345 + previous;
                    requested.clear();
                    requests.set(0);
                    release.set(new CountDownLatch(1));
                    fail.set(false);
                    try (var cache = CacheContext.acquire(config("io.db", "schema.registry.url",
                            "http://localhost:" + server.getAddress().getPort(),
                            "use.apache.http.client", Boolean.toString(deadline)));
                         var next = new Execution(Duration.ofSeconds(20))) {
                        boolean completed = false;
                        try {
                            TestStack.sendBytes(blocked, TestStack.utf8("key"),
                                    ByteBuffer.allocate(6).put((byte) 0).putInt(schema).put((byte) 0).array(), null);
                            TestStack.sendString(plain, "key", deadline ? "after-deadline" : "after-cancel", null);
                            var ordinary = cache.plan(plain, next);
                            try (var execution = new Execution(deadline ? Duration.ofSeconds(2) : Duration.ZERO)) {
                                var plan = cache.plan(blocked, execution);
                                var job = CompletableFuture.supplyAsync(() -> {
                                    try { return cache.sync(plan, execution); }
                                    catch (SQLException e) { throw new CompletionException(e); }
                                });
                                assertEquals(Boolean.TRUE, requested.poll(10, TimeUnit.SECONDS));
                                if (!deadline) execution.cancel();
                                var failed = assertThrows(java.util.concurrent.ExecutionException.class, () -> job.get(3, TimeUnit.SECONDS));
                                Throwable cause = failed.getCause();
                                if (cause instanceof CompletionException) cause = cause.getCause();
                                assertInstanceOf(SQLException.class, cause);
                                if (deadline) assertInstanceOf(SQLTimeoutException.class, cause);
                                assertEquals(deadline ? "HYT00" : "57014", ((SQLException) cause).getSQLState());
                                long id = cache.read(c -> {
                                    try (var s = c.createStatement(); var rows = s.executeQuery(
                                            "SELECT id FROM \"$topics\" WHERE topic='" + blocked + "'")) {
                                        assertTrue(rows.next()); return rows.getLong(1);
                                    }
                                });
                                var barrier = CompletableFuture.supplyAsync(() -> {
                                    try { return cache.write(blocked, id, next, c -> null); }
                                    catch (SQLException e) { throw new CompletionException(e); }
                                });
                                barrier.get(1, TimeUnit.SECONDS);
                                assertEquals(1, release.get().getCount(), "the Registry call is still held");
                                release.get().countDown();
                                long plainId = cache.sync(ordinary, next);
                                assertEquals(previous + 1, (int) cache.read(c -> count(c, "SELECT count(*) FROM \"$t" + plainId + "\"")));
                                // This sync waited for the old decoder batch to finish. Its rows,
                                // ranges and schema copies must still be absent until a new sync.
                                assertEquals(previous, (int) cache.read(c -> count(c, "SELECT count(*) FROM \"$t" + id + "\"")));
                                assertEquals(previous, (int) cache.read(c -> count(c,
                                        "SELECT coalesce(max(end_offset),0) FROM \"$ranges\" WHERE topic_ref=" + id)));
                                assertEquals(0, (int) cache.read(c -> count(c, "SELECT count(*) FROM \"$schemas\" WHERE ref='" + schema + "'")));
                                cache.sync(cache.plan(blocked, next), next);
                                assertEquals(previous + 1, (int) cache.read(c -> count(c, "SELECT count(*) FROM \"$t" + id + "\"")));
                                assertEquals(1, requests.get(), "the cancelled batch's schema stays in the one client's cache");
                            }
                            fail.set(true);
                            SQLException failure = assertThrows(SQLException.class, () -> cache.sync(cache.plan(denied, next), next));
                            assertTrue(failure.getMessage().contains("Registry access denied"), failure.getMessage());
                            assertEquals(0, (int) cache.read(c -> count(c,
                                    "SELECT count(*) FROM \"$ranges\" r JOIN \"$topics\" t ON r.topic_ref=t.id WHERE t.topic='" + denied + "'")));
                            completed = true;
                        } finally {
                            if (!completed) release.get().countDown();
                        }
                    }
                }
            } finally { release.get().countDown(); server.stop(0); }
        }
    }
    private static int count(Connection reader, String query) throws SQLException {
        try (var s = reader.createStatement(); var rows = s.executeQuery(query)) {
            assertTrue(rows.next()); return rows.getInt(1);
        }
    }
}
