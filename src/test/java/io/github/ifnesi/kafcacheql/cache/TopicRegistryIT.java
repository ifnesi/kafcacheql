package io.github.ifnesi.kafcacheql.cache;

import static io.github.ifnesi.kafcacheql.cache.CacheStoreTest.query;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import io.github.ifnesi.kafcacheql.kafka.KafkaClients;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The driver's JDBC connection is not built yet. Exercise registration with real broker metadata and
 * read its schema and data over the cache's SQLite JDBC readers; no sync/consumer/decoder is involved.
 */
class TopicRegistryIT {
    private static DriverConfig config;
    private static KafkaClients.Lease lease;

    @TempDir
    Path dir;

    @BeforeAll
    static void stack() throws Exception {
        TestStack.requireUp();
        config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, null);
        lease = KafkaClients.acquire(config);
    }

    @AfterAll
    static void closeClients() {
        if (lease != null) {
            lease.close();
        }
    }

    private CacheStore cache() throws SQLException {
        return CacheStore.open(dir.resolve("cache.db"), Optional.of(lease.clients().clusterId()));
    }

    private static TopicInfo create(String base, String policy) throws SQLException {
        String name = TestStack.uniqueTopic(base);
        TestStack.createTopic(name, 2, Map.of("cleanup.policy", policy));
        return lease.clients().describeTopic(name, config).orElseThrow();
    }

    private static TopicRegistry.Registration register(CacheStore cache, TopicInfo info) throws SQLException {
        OptionalLong observed;
        try (Connection r = cache.newReader()) {
            observed = TopicRegistry.generation(r, info.name());
        }
        return TopicRegistry.register(cache, observed, info, TopicSettings.NONE, 1).orElseThrow();
    }

    /** 0004b acceptance: "append and compacted tables as specified". PLAN §6.4's complete DDL;
     * §6.5: "The mode can be overridden with topic.<name>.mode." §6.3: pruned_below defaults to 0.
     */
    @Test
    void appendAndCompactedTablesHaveTheSpecifiedColumnsAndIndexes() throws Exception {
        try (CacheStore cache = cache(); Connection r = cache.newReader()) {
            for (int variant = 0; variant < 4; variant++) {
                String policy = variant % 2 == 0 ? "delete" : "compact,delete";
                TopicInfo broker = create("storage", policy);
                Properties p = new Properties();
                p.setProperty("topic." + broker.name() + ".key.format", "string");
                p.setProperty("topic." + broker.name() + ".value.format", "json");
                if (variant >= 2) {
                    p.setProperty("topic." + broker.name() + ".mode", variant == 2 ? "compacted" : "append");
                }
                DriverConfig overrides = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
                OptionalLong observed = TopicRegistry.generation(r, broker.name());
                TopicInfo info = lease.clients().describeTopic(broker.name(), overrides).orElseThrow();
                Mode expected = variant == 1 || variant == 2 ? Mode.COMPACTED : Mode.APPEND;
                assertEquals(expected, info.mode());
                TopicRegistry.Registration registered = TopicRegistry.register(cache, observed, info,
                        overrides.topic(info.name()), 7).orElseThrow();
                long id = registered.id();
                String table = "$t" + id;
                assertEquals(List.of(
                        "_partition|INTEGER|1|1", "_offset|INTEGER|1|2", "_timestamp|INTEGER|1|0",
                        "_key_raw|BLOB|0|0", "_key|TEXT|0|0", "_key_schema_id|TEXT|0|0", "_value|TEXT|0|0",
                        "_value_schema_id|TEXT|0|0", "_headers|TEXT|0|0", "_decode_error|TEXT|0|0"), query(r,
                        "SELECT name, type, \"notnull\", pk FROM pragma_table_info('" + table + "') ORDER BY cid"));
                assertEquals(List.of("1"), query(r, "SELECT wr FROM pragma_table_list WHERE name = '" + table + "'"));
                assertEquals(expected == Mode.COMPACTED ? List.of(table + "_key|1", table + "_ts|0")
                                : List.of(table + "_ts|0"), query(r,
                        "SELECT name, \"unique\" FROM pragma_index_list('" + table + "') WHERE origin = 'c' ORDER BY name"));
                assertEquals(List.of("_timestamp"), query(r, "SELECT name FROM pragma_index_info('" + table + "_ts')"));
                if (expected == Mode.COMPACTED) {
                    assertEquals(List.of("_partition", "_key_raw"), query(r,
                            "SELECT name FROM pragma_index_info('" + table + "_key') ORDER BY seqno"));
                }
                assertEquals(List.of(info.name() + "|" + info.name() + "|" + info.topicId() + "|" + expected.value()
                                + "|2|CreateTime|" + info.deleteRetentionMs() + "|string|json|7|null"), query(r,
                        "SELECT topic, view_name, topic_uuid, mode, partition_count, timestamp_type, delete_retention_ms, "
                                + "key_format, value_format, decoder_version, deleted_ms FROM \"$topics\" WHERE id = " + id));
                assertEquals(List.of("0|null|0", "1|null|0"), query(r,
                        "SELECT partition_no, caught_up_at_ms, pruned_below FROM \"$partitions\" WHERE topic_ref = " + id
                                + " ORDER BY partition_no"));
                assertEquals(List.of("0"), query(r,
                        "SELECT count(*) FROM pragma_table_info('$topics') WHERE name = 'last_check_ms'"));
                try (Statement w = cache.writer().createStatement()) {
                    w.execute("INSERT INTO \"" + table + "\" (_partition, _offset, _timestamp, _key_raw) VALUES (0, 1, 10, x'01')");
                    assertThrows(SQLException.class, () -> w.execute("INSERT INTO \"" + table
                            + "\" (_partition, _offset, _timestamp) VALUES (0, 1, 20)"), "composite primary key");
                    String sameKey = "INSERT INTO \"" + table
                            + "\" (_partition, _offset, _timestamp, _key_raw) VALUES (1, 2, 20, x'01')";
                    w.execute(sameKey); // PLAN §6.5: the same key in a second partition is a second row.
                    String samePartitionKey = "INSERT INTO \"" + table
                            + "\" (_partition, _offset, _timestamp, _key_raw) VALUES (0, 3, 20, x'01')";
                    if (expected == Mode.COMPACTED) {
                        assertThrows(SQLException.class, () -> w.execute(samePartitionKey), "partition and raw-key identity");
                    } else {
                        w.execute(samePartitionKey);
                    }
                }
                assertFalse(registered.warning().isPresent());
            }
        }
    }

    /** 0004b acceptance: "Orders and orders get distinct view names". PLAN §6.2: "the second view gets
     * a $ suffix ... and the driver emits a warning." Further collisions must remain distinct too.
     */
    @Test
    void caseCollisionsGetDistinctViewsAndWarnings() throws Exception {
        String first = TestStack.uniqueTopic("Orders");
        List<String> names = List.of(first, first.toLowerCase(Locale.ROOT), first.toUpperCase(Locale.ROOT));
        try (CacheStore cache = cache(); Connection r = cache.newReader()) {
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                TestStack.createTopic(name, 1, Map.of());
                OptionalLong observed = TopicRegistry.generation(r, name);
                TopicInfo info = lease.clients().describeTopic(name, config).orElseThrow();
                TopicRegistry.Registration registered = TopicRegistry.register(cache, observed, info, TopicSettings.NONE, 1)
                        .orElseThrow();
                String view = name + (i == 0 ? "" : "$" + (i + 1));
                assertEquals(view, registered.viewName());
                assertEquals(i > 0, registered.warning().isPresent());
                if (i > 0) {
                    String warning = registered.warning().orElseThrow().getMessage();
                    assertTrue(warning.contains(name) && warning.contains(view), warning);
                }
                try (Statement w = cache.writer().createStatement()) {
                    w.execute("INSERT INTO \"$t" + registered.id() + "\" (_partition, _offset, _timestamp) VALUES (0, " + i + ", 1)");
                }
                assertEquals(List.of(Integer.toString(i)), query(r, "SELECT _offset FROM \"" + view + "\""));
            }
            assertEquals(List.of("3|3"), query(r,
                    "SELECT count(*), count(DISTINCT view_name COLLATE NOCASE) FROM \"$topics\""));
        }
    }

    /** PLAN §6.2: "a topic named that way gets a view with the underscore after sqlite made a hyphen"
     * "and a warning. If that name is taken too, the $2 rule below applies." The prefix matches in any case.
     */
    @Test
    void sqlitePrefixesBecomeHyphensWithWarningsAndUseTheExistingCollisionRule() throws Exception {
        try (CacheStore cache = cache(); Connection r = cache.newReader()) {
            for (boolean hyphenFirst : List.of(false, true)) {
                String tail = TestStack.uniqueTopic("orders_with_details");
                String lower = "sqlite_" + tail;
                String upper = "SQLITE_" + tail;
                String mixed = "sQlItE_" + tail;
                String hyphen = "sqlite-" + tail;
                List<String> topics = hyphenFirst ? List.of(hyphen, lower, upper, mixed)
                        : List.of(lower, upper, mixed, hyphen);
                List<String> views = hyphenFirst
                        ? List.of(hyphen, hyphen + "$2", "SQLITE-" + tail + "$3", "sQlItE-" + tail + "$4")
                        : List.of(hyphen, "SQLITE-" + tail + "$2", "sQlItE-" + tail + "$3", hyphen + "$4");
                for (int i = 0; i < topics.size(); i++) {
                    String topic = topics.get(i);
                    String view = views.get(i);
                    TestStack.createTopic(topic, 1, Map.of());
                    OptionalLong observed = TopicRegistry.generation(r, topic);
                    TopicInfo info = lease.clients().describeTopic(topic, config).orElseThrow();
                    TopicRegistry.Registration registered = TopicRegistry.register(cache, observed, info, TopicSettings.NONE, 1)
                            .orElseThrow();
                    assertEquals(view, registered.viewName());
                    assertEquals(!topic.equals(view), registered.warning().isPresent());
                    if (!topic.equals(view)) {
                        var warning = registered.warning().orElseThrow();
                        assertEquals("01000", warning.getSQLState());
                        assertTrue(warning.getMessage().contains(topic) && warning.getMessage().contains(view),
                                warning.getMessage());
                        if (!topic.equals(hyphen)) {
                            assertTrue(warning.getMessage().contains("sqlite_"), warning.getMessage());
                        }
                    }
                    assertEquals(List.of(topic + "|" + view), query(r,
                            "SELECT topic, view_name FROM \"$topics\" WHERE id = " + registered.id()));
                    try (Statement w = cache.writer().createStatement()) {
                        w.execute("INSERT INTO \"$t" + registered.id()
                                + "\" (_partition, _offset, _timestamp) VALUES (0, " + i + ", 1)");
                    }
                    assertEquals(List.of(Integer.toString(i)), query(r, "SELECT _offset FROM \"" + view + "\""));
                }
            }
            // The exception changes only a leading sqlite_: other occurrences and near matches stay exact.
            for (String base : List.of("other_sqlite_orders", "sqliteorders", "sqlite-orders")) {
                String topic = TestStack.uniqueTopic(base);
                TestStack.createTopic(topic, 1, Map.of());
                OptionalLong observed = TopicRegistry.generation(r, topic);
                TopicInfo info = lease.clients().describeTopic(topic, config).orElseThrow();
                TopicRegistry.Registration registered = TopicRegistry.register(cache, observed, info, TopicSettings.NONE, 1)
                        .orElseThrow();
                assertEquals(topic, registered.viewName());
                assertTrue(registered.warning().isEmpty());
                assertEquals(List.of("0"), query(r, "SELECT count(*) FROM \"" + topic + "\""));
            }
        }
    }

    /** 0004b acceptance: "registering twice changes nothing". PLAN §10.1: when another process registered
     * first "with the same Kafka topic ID, the query uses that generation".
     */
    @Test
    void registeringTwicePreservesTheGenerationAndAllItsContents() throws Exception {
        TopicInfo info = create("idempotent", "delete");
        try (CacheStore cache = cache(); Connection r = cache.newReader(); Statement w = cache.writer().createStatement()) {
            TopicRegistry.Registration first = register(cache, info);
            w.execute("INSERT INTO \"$t" + first.id() + "\" (_partition, _offset, _timestamp) VALUES (0, 4, 1)");
            w.execute("UPDATE \"$partitions\" SET caught_up_at_ms = 100, pruned_below = 2 WHERE topic_ref = " + first.id());
            w.execute("INSERT INTO \"$ranges\" VALUES (" + first.id() + ", 0, 2, 5)");
            List<String> before = snapshot(r);
            for (OptionalLong observed : List.of(OptionalLong.empty(), OptionalLong.of(first.id()))) {
                TopicRegistry.Registration again = TopicRegistry.register(cache, observed, info, TopicSettings.NONE, 1)
                        .orElseThrow();
                assertEquals(first.id(), again.id());
                assertEquals(first.viewName(), again.viewName());
                assertEquals(before, snapshot(r));
                assertEquals(List.of("4"), query(r, "SELECT _offset FROM \"" + info.name() + "\""));
                assertEquals(List.of("2|5"), query(r, "SELECT start_offset, end_offset FROM \"$ranges\""));
                assertEquals(List.of("100|2", "100|2"), query(r,
                        "SELECT caught_up_at_ms, pruned_below FROM \"$partitions\" ORDER BY partition_no"));
            }
        }
    }

    /** 0004b added acceptance: "two processes registering one topic at once leave one row".
     * PLAN §10.1: "SQLite's write lock serialises their transactions."
     */
    @Test
    void twoProcessesRegisterOneGeneration() throws Exception {
        TopicInfo info = create("registration-race", "delete");
        try (CacheStore cache = cache(); Statement lock = cache.writer().createStatement()) {
            Process a = child(cache, info.name());
            Process b = child(cache, info.name());
            try (BufferedReader outA = a.inputReader(StandardCharsets.UTF_8);
                 BufferedReader outB = b.inputReader(StandardCharsets.UTF_8)) {
                assertEquals("READY", line(outA));
                assertEquals("READY", line(outB));
                lock.execute("BEGIN IMMEDIATE");
                for (Process child : List.of(a, b)) {
                    child.getOutputStream().write('\n');
                    child.getOutputStream().flush();
                }
                assertEquals("REGISTERING", line(outA));
                assertEquals("REGISTERING", line(outB));
                lock.execute("COMMIT");
                assertTrue(a.waitFor(30, TimeUnit.SECONDS));
                assertTrue(b.waitFor(30, TimeUnit.SECONDS));
                assertEquals(0, a.exitValue());
                assertEquals(0, b.exitValue());
                String result = line(outA);
                assertEquals(result, line(outB));
                assertTrue(result.startsWith("RESULT "), result);
                try (Connection r = cache.newReader()) {
                    List<String> registered = query(r, "SELECT id, view_name FROM \"$topics\"");
                    assertEquals(1, registered.size());
                    assertEquals("RESULT " + registered.getFirst(), result);
                    assertEquals(List.of("2"), query(r, "SELECT count(*) FROM \"$partitions\""));
                    assertEquals(List.of("1"), query(r,
                            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name GLOB '$t[0-9]*'"));
                }
            } finally {
                a.destroyForcibly();
                b.destroyForcibly();
            }
        }
    }

    /** 0004b added acceptance: "after a topic's row is deleted, registering it again gives a new id".
     * Later note / PLAN §10.1: replace a deleted row only for "the generation observed"; a stale
     * "never registered" observation cannot pass after registration and deletion (the absent-ABA).
     */
    @Test
    void deletedRegistrationsGetNewIdsAndRejectStaleObservations() throws Exception {
        TopicInfo info = create("deleted", "delete");
        try (CacheStore cache = cache(); Connection r = cache.newReader()) {
            long original = register(cache, info).id();
            deleteFixture(cache, info.name(), original, false);
            long afterRemoval = register(cache, info).id();
            assertTrue(afterRemoval > original, "a committed AUTOINCREMENT id is never reused");

            deleteFixture(cache, info.name(), afterRemoval, true);
            assertEquals(OptionalLong.of(afterRemoval), TopicRegistry.generation(r, info.name()), "deleted row stays visible");
            List<String> deleted = snapshot(r);
            assertTrue(TopicRegistry.register(cache, OptionalLong.empty(), info, TopicSettings.NONE, 1).isEmpty());
            assertEquals(deleted, snapshot(r), "stale never-registered observation leaves the tombstone intact");
            long replacement = TopicRegistry.register(cache, OptionalLong.of(afterRemoval), info, TopicSettings.NONE, 1)
                    .orElseThrow().id();
            assertTrue(replacement > afterRemoval);
            assertEquals(List.of("null"), query(r, "SELECT deleted_ms FROM \"$topics\""));
            deleteFixture(cache, info.name(), replacement, true);
            List<String> newerDeletion = snapshot(r);
            assertTrue(TopicRegistry.register(cache, OptionalLong.of(afterRemoval), info, TopicSettings.NONE, 1).isEmpty());
            assertEquals(newerDeletion, snapshot(r), "an older deleted generation cannot replace a newer one");
        }
    }

    /** 0004b added acceptance: "a rolled-back registration's id is never used". PLAN §10.1:
     * "A new id is used only once its registration has committed, since a rolled-back allocation can be handed out again."
     */
    @Test
    void aFailedCommitDoesNotPublishAnIdOrLeaveStorageBehind() throws Exception {
        TopicInfo info = create("rollback", "delete");
        try (CacheStore cache = cache(); Connection r = cache.newReader(); Statement w = cache.writer().createStatement()) {
            List<String> before = snapshot(r);
            // A real COMMIT failure, after the allocation and DDL: the trigger's deferred FK cannot resolve.
            w.execute("""
                    CREATE TEMP TRIGGER fail_registration AFTER INSERT ON "$topics" BEGIN
                      INSERT INTO "$ranges" VALUES (-1, 0, 0, 1);
                    END
                    """);
            w.execute("PRAGMA defer_foreign_keys = ON");
            assertThrows(SQLException.class,
                    () -> TopicRegistry.register(cache, OptionalLong.empty(), info, TopicSettings.NONE, 1));
            assertEquals(before, snapshot(r), "failed commit rolls back the row, table, indexes, view and sequence");
            assertTrue(TopicRegistry.generation(r, info.name()).isEmpty());
            assertTrue(cache.writer().getAutoCommit(), "JDBC auto-commit remains on");
            w.execute("DROP TRIGGER fail_registration");
            TopicRegistry.Registration committed = register(cache, info);
            assertEquals(1, committed.id(), "SQLite can reallocate the rolled-back id");
            assertEquals(OptionalLong.of(committed.id()), TopicRegistry.generation(r, info.name()));
            assertEquals(List.of("0"), query(r, "SELECT count(*) FROM \"" + committed.viewName() + "\""));
        }
    }

    /** 0004b added acceptance: "a registration that finds the topic already registered under a different
     * Kafka topic ID replans". PLAN §10.1: the generation is captured before the Kafka calls it guards.
     */
    @Test
    void aDifferentKafkaTopicIdRequiresAReplanWithoutChangingTheCache() throws Exception {
        String name = TestStack.uniqueTopic("recreated");
        TestStack.createTopic(name, 1, Map.of());
        try (CacheStore cache = cache(); Connection r = cache.newReader()) {
            OptionalLong neverRegistered = TopicRegistry.generation(r, name);
            TopicInfo older = lease.clients().describeTopic(name, config).orElseThrow();
            TestStack.recreateTopic(name, 1, Map.of());
            TopicInfo newer = lease.clients().describeTopic(name, config).orElseThrow();
            assertNotEquals(older.topicId(), newer.topicId());
            TopicRegistry.Registration current = TopicRegistry.register(cache, neverRegistered, newer, TopicSettings.NONE, 1)
                    .orElseThrow();
            List<String> before = snapshot(r);
            for (OptionalLong observed : List.of(neverRegistered, OptionalLong.of(current.id()))) {
                assertTrue(TopicRegistry.register(cache, observed, older, TopicSettings.NONE, 1).isEmpty(), "replan");
                assertEquals(before, snapshot(r), "obsolete Kafka observation changes nothing");
            }
        }
    }

    private static List<String> snapshot(Connection r) throws SQLException {
        List<String> rows = query(r, """
                SELECT name, type, sql FROM sqlite_master ORDER BY name
                """);
        rows.addAll(query(r, "SELECT * FROM \"$topics\" ORDER BY id"));
        rows.addAll(query(r, "SELECT * FROM sqlite_sequence ORDER BY name"));
        return rows;
    }

    /** Fixture for the future deletion worker: no deletion implementation belongs to 0004b. */
    private static void deleteFixture(CacheStore cache, String view, long id, boolean retainRow) throws SQLException {
        try (Statement w = cache.writer().createStatement()) {
            w.execute("BEGIN IMMEDIATE");
            w.execute("DROP VIEW \"" + view + "\"");
            w.execute("DROP TABLE \"$t" + id + "\"");
            w.execute("DELETE FROM \"$ranges\" WHERE topic_ref = " + id);
            w.execute("DELETE FROM \"$partitions\" WHERE topic_ref = " + id);
            w.execute(retainRow ? "UPDATE \"$topics\" SET deleted_ms = 1 WHERE id = " + id
                    : "DELETE FROM \"$topics\" WHERE id = " + id);
            w.execute("COMMIT");
        }
    }

    private Process child(CacheStore cache, String topic) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), TopicRegistrationProcess.class.getName(),
                cache.path().toString(), lease.clients().clusterId(), topic, TestStack.BOOTSTRAP)
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
    }

    private static String line(BufferedReader reader) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return reader.readLine();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }).get(30, TimeUnit.SECONDS);
    }
}
