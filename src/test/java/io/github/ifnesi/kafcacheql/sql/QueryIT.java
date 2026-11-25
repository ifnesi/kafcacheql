package io.github.ifnesi.kafcacheql.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryIT {
    @TempDir Path dir;

    /** PLAN §8.2: "Not yet cached: read the topic config, create its storage table and view, and register it";
     * §4.3: topics.exclude is the "Regex of topics hidden from getTables()";
     * §6.2: internal tables "are always queryable".
     * Until 0011b, exercise the pipeline entry point and read its registered views with SQLite JDBC.
     */
    @Test void registersUncachedTopicsAndResolvesExcludedAndInternalNames() throws Exception {
        TestStack.requireUp();
        String topic = TestStack.uniqueTopic("query.Orders"), excluded = TestStack.uniqueTopic("query-hidden");
        for (String name : List.of(topic, excluded)) {
            TestStack.createTopic(name, 1, Map.of());
            TestStack.sendString(name, "key", "value", null);
        }
        Properties p = new Properties();
        p.setProperty("cache.path", dir.resolve("cache.db").toString());
        p.setProperty("topics.exclude", "query-hidden.*");
        var config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, p);
        try (var cache = CacheContext.acquire(config); var execution = new Execution(Duration.ofSeconds(20))) {
            assertEquals(0, count(cache, "\"$topics\""));
            var query = Query.parseAndResolve("SELECT * FROM \"" + topic.toLowerCase(Locale.ROOT) + "\" o JOIN \""
                    + excluded + "\" h ON o._key=h._key", name -> cache.resolveTable(name, execution));
            assertEquals(List.of(topic, excluded), query.references().stream().map(r -> r.target().topic().orElseThrow()).toList());
            assertEquals(2, count(cache, "\"$topics\""));
            assertEquals(0, count(cache, "\"" + topic + "\"")); // Resolution registers; only sync fetches records.
            for (var reference : query.references()) {
                String name = reference.target().topic().orElseThrow();
                execution.run(false, () -> cache.sync(cache.plan(name, execution), execution));
                assertEquals(1, count(cache, "\"" + reference.target().viewName() + "\""));
            }
            var wildcard = QueryPipeline.execute(cache, "SELECT \"" + topic + "\".* FROM \"" + topic + "\"",
                    Map.of(), execution);
            assertEquals(1, wildcard.rows().size());
            assertEquals("\"key\"", wildcard.rows().getFirst().get(wildcard.columns().indexOf("_key")));
            assertEquals("\"value\"", wildcard.rows().getFirst().get(wildcard.columns().indexOf("_value")));
            var cached = Query.parseAndResolve("SELECT * FROM \"" + topic.toUpperCase(Locale.ROOT) + "\"",
                    name -> cache.resolveTable(name, execution));
            assertEquals(topic, cached.references().getFirst().target().topic().orElseThrow());
            var internals = Query.parseAndResolve("SELECT * FROM \"$TOPICS\" JOIN \"$ranges\" ON 1=1 JOIN \"$t1\" ON 1=1",
                    name -> cache.resolveTable(name, execution));
            assertTrue(internals.references().stream().allMatch(r -> r.target().topic().isEmpty()));
            String unknown = TestStack.uniqueTopic("query-missing");
            var error = assertThrows(SQLException.class, () -> Query.parseAndResolve("SELECT * FROM \"" + unknown + "\"",
                    name -> cache.resolveTable(name, execution)));
            assertTrue(error.getMessage().contains(unknown));
            assertEquals("42S02", error.getSQLState());
            assertEquals(2, count(cache, "\"$topics\""));
        }
    }

    private static long count(CacheContext.Lease cache, String table) throws SQLException {
        return cache.read(c -> {
            try (var s = c.createStatement(); var rows = s.executeQuery("SELECT count(*) FROM " + table)) {
                assertTrue(rows.next());
                return rows.getLong(1);
            }
        });
    }
}
