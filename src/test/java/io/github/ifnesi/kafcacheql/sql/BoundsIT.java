package io.github.ifnesi.kafcacheql.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.DriverConfig;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.testkit.TestStack;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.Function;

class BoundsIT {
    @TempDir Path dir;

    /** PLAN §8.3: "The driver may fetch more than needed, never less. SQLite applies the full WHERE".
     * Original SQL over every generated row is the oracle for the complete pipeline, including folding.
     * Give random() one controlled value per execution, shared by the oracle and the pipeline.
     */
    @Test void randomisedWindowsContainEveryRowSQLiteCanReturn() throws Exception {
        TestStack.requireUp();
        Random random = new Random(0x0008b);
        AtomicLong sample = new AtomicLong();
        long base = System.currentTimeMillis() - 120_000;
        String orders = TestStack.uniqueTopic("bounds-orders"), payments = TestStack.uniqueTopic("bounds-payments");
        Properties properties = new Properties();
        properties.setProperty("cache.path", dir.resolve("cache.db").toString());
        properties.setProperty("append.default.window", "all");
        var config = DriverConfig.load("jdbc:kafcacheql://" + TestStack.BOOTSTRAP, properties);
        int returned = 0;
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite::memory:");
             var cache = CacheContext.acquire(config); var s = sqlite.createStatement()) {
            controlledRandom(sqlite, sample);
            // This serial flow uses one pooled reader for both planning and final execution.
            cache.read(c -> { controlledRandom(c, sample); return null; });
            for (String table : List.of("orders", "payments")) {
                String topic = table.equals("orders") ? orders : payments;
                TestStack.createTopic(topic, 4, Map.of());
                s.execute("CREATE TABLE " + table + " (_partition INTEGER, _offset INTEGER, _timestamp INTEGER, _value TEXT)");
                try (var insert = sqlite.prepareStatement("INSERT INTO " + table + " VALUES (?,?,?,?)")) {
                    for (int row = 0; row < 150; row++) {
                        int partition = random.nextInt(4);
                        long timestamp = base + random.nextInt(80) - 20;
                        String value = random.nextBoolean() ? "ok" : "bad";
                        var record = TestStack.send(topic, partition, timestamp, TestStack.utf8("k" + row), TestStack.utf8(value), null);
                        insert.setInt(1, partition);
                        insert.setLong(2, record.offset());
                        insert.setLong(3, timestamp);
                        insert.setString(4, "\"" + value + "\"");
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
            }
            for (int step = 0; step < 600; step++) {
                String a = predicate(random, "o.", base), b = predicate(random, "p.", base);
                String sql = switch (step % 4) {
                    case 0 -> "SELECT o._partition,o._offset,o._timestamp FROM orders o WHERE " + a + " ORDER BY 1,2";
                    case 1 -> "SELECT o._partition,o._offset,o._timestamp,p._partition,p._offset,p._timestamp "
                            + "FROM orders o JOIN payments p ON o._offset=p._offset WHERE (" + a + ") AND (" + b + ") ORDER BY 1,2,4,5";
                    case 2 -> "WITH c AS (SELECT o.* FROM orders o WHERE " + a
                            + ") SELECT _partition,_offset,_timestamp,_value FROM c WHERE _offset>=?1 ORDER BY 1,2";
                    default -> "SELECT _partition,_offset,_timestamp,_value FROM (SELECT o.* FROM orders o WHERE " + a
                            + ") q WHERE _offset<?1 ORDER BY 1,2";
                };
                Map<Integer, ?> parameters = Collections.singletonMap(1, switch (step % 5) {
                    case 0 -> null;
                    case 1 -> "5";
                    case 2 -> "not numeric";
                    case 3 -> 7.5;
                    default -> 15L;
                });
                sample.set(random.nextLong());
                var expected = rows(sqlite, sql, parameters);
                String kafkaSql = sql.replace("orders", StorageDdl.quote(orders)).replace("payments", StorageDdl.quote(payments));
                try (var execution = new Execution(Duration.ofSeconds(30))) {
                    assertEquals(expected, QueryPipeline.execute(cache, kafkaSql, parameters, execution).rows(), sql);
                }
                returned += expected.size();
            }
        }
        assertTrue(returned > 1000, "The independent oracle must actually see rows");
    }

    private static List<List<Object>> rows(Connection sqlite, String sql, Map<Integer, ?> parameters) throws SQLException {
        try (var statement = sqlite.prepareStatement(sql)) {
            if (statement.getParameterMetaData().getParameterCount() > 0) statement.setObject(1, parameters.get(1));
            try (var rows = statement.executeQuery()) {
                var result = new ArrayList<List<Object>>();
                while (rows.next()) {
                    var row = new ArrayList<Object>();
                    for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) row.add(rows.getObject(i));
                    result.add(row);
                }
                return result;
            }
        }
    }

    private static void controlledRandom(Connection sqlite, AtomicLong sample) throws SQLException {
        Function.create(sqlite, "random", new Function() {
            @Override protected void xFunc() throws SQLException { result(sample.get()); }
        }, 0);
    }

    private static String predicate(Random random, String alias, long base) {
        long n = random.nextInt(70) - 10;
        String column = alias + List.of("_partition", "_offset", "_timestamp").get(random.nextInt(3));
        if (column.endsWith("_timestamp")) n += base;
        String atom = switch (random.nextInt(13)) {
            case 0 -> column + " BETWEEN " + n + " AND " + (n + random.nextInt(20));
            case 1 -> column + " IN (" + n + ", " + (n + 2) + ", ?1)";
            case 2 -> "abs(" + column + ") >= " + n;
            case 3 -> column + " >= ?1";
            case 4 -> n + " < " + column;
            case 5 -> column + " <= abs(" + n + ")+3";
            case 6 -> column + " >= " + n + ".5";
            case 7 -> column + " < (SELECT " + n + ")";
            case 8 -> column + " < abs(random() % 70)";
            case 9 -> column + " BETWEEN 1 AND 3 = TRUE";
            default -> column + " " + List.of("<", "<=", "=", ">", ">=").get(random.nextInt(5)) + " " + n;
        };
        return switch (random.nextInt(5)) {
            case 0 -> "NOT (" + atom + ")";
            case 1 -> "(" + atom + " OR " + alias + "_value='\"ok\"')";
            case 2 -> "(" + atom + ") AND " + alias + "_partition IN (0,2)";
            default -> "(" + atom + ")";
        };
    }
}
