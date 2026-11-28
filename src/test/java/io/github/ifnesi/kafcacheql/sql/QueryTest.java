package io.github.ifnesi.kafcacheql.sql;

import static org.junit.jupiter.api.Assertions.*;

import io.github.ifnesi.kafcacheql.cache.TopicRegistry.ResolvedTable;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.Test;

class QueryTest {
    /** PLAN §8.1: "Other DML/DDL, PRAGMA, ATTACH and multiple statements are rejected before anything runs."
     * The resolver is the only handoff to Kafka/cache work; it must never be reached for these inputs.
     */
    @Test void rejectsOtherStatementsBeforeResolvingAnything() {
        Query.Resolver untouched = name -> { throw new AssertionError("Touched Kafka/cache for " + name); };
        for (String sql : List.of("INSERT INTO orders VALUES (1)", "UPDATE orders SET status = 'x'",
                "DELETE FROM orders", "CREATE TABLE orders (id INT)", "DROP VIEW orders", "PRAGMA user_version",
                "ATTACH DATABASE 'other.db' AS other", "BEGIN", "VACUUM", "EXPLAIN SELECT * FROM orders",
                "VALUES (1)", "SELECT * FROM orders; SELECT * FROM payments", "SELECT * FROM orders; DELETE FROM orders",
                "WITH x AS (SELECT * FROM orders) DELETE FROM orders", "SELECT * INTO copy FROM orders",
                "WITH x AS (DELETE FROM orders RETURNING *) SELECT * FROM x")) {
            assertThrows(SQLException.class, () -> Query.parseAndResolve(sql, untouched), sql);
        }
        // §8.1: unparseable SELECTs retain their cause, with driver-owned diagnostics and SQLState.
        for (String sql : List.of("SELECT * FROM", "SELECT * FROM orders WHERE _key GLOB 'a*'")) {
            var error = assertThrows(SQLException.class, () -> Query.parseAndResolve(sql, untouched), sql);
            io.github.ifnesi.kafcacheql.testkit.SqlErrors.assertReadable(error.getMessage());
            assertEquals("42000", error.getSQLState());
            assertInstanceOf(JSQLParserException.class, error.getCause());
            assertTrue(error.getMessage().startsWith("SQL syntax error at line "), error.getMessage());
            assertFalse(error.getMessage().contains("jsqlparser"), error.getMessage());
        }
    }

    /** PLAN §8.2: "Determine referenced topics via their view names"; §6.2: "SQLite identifiers are
     * case-insensitive". Aliases and scoped CTEs are SQL names, not additional Kafka topics.
     */
    @Test void resolvesExampleQueriesAndKeepsEachTableReference() throws Exception {
        var names = new TreeMap<String, ResolvedTable>(String.CASE_INSENSITIVE_ORDER);
        for (String name : List.of("orders", "payments.v1")) {
            names.put(name, new ResolvedTable(name, Optional.of(name), Optional.empty()));
        }
        names.put("$ranges", new ResolvedTable("$ranges", Optional.empty(), Optional.empty()));
        Query.Resolver resolver = name -> {
            assertTrue(names.containsKey(name), "Unexpected real table: " + name);
            return names.get(name);
        };
        var examples = List.of(
                "SELECT status, count(*) FROM \"orders\" WHERE _timestamp >= unixepoch('now','-15 minutes')*1000 GROUP BY status",
                "SELECT status, count(*) FROM (SELECT * FROM orders WHERE (_partition=0 AND _offset>=1 AND _offset<9)) AS orders GROUP BY status",
                "SELECT o._offset, p._value FROM Orders o JOIN \"payments.v1\" p ON o._key=p._key",
                "WITH \"Recent\" AS (SELECT * FROM orders) SELECT * FROM recent r JOIN \"payments.v1\" p ON r._key=p._key",
                "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM \"payments.v1\" p WHERE p._key=orders._key)",
                "SELECT * FROM (WITH orders AS (SELECT * FROM \"payments.v1\") SELECT * FROM ORDERS) q JOIN orders o ON 1=1",
                "SELECT * FROM orders a JOIN orders b ON a._key=b._key",
                "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM R WHERE n<2) SELECT * FROM r JOIN ORDERS o ON 1=1",
                "SELECT * FROM main.\"ORDERS\" JOIN \"$RANGES\" r ON 1=1",
                "SELECT _value -> 'status', _value ->> 'status' FROM [payments.v1]");
        var expected = List.of(List.of("orders"), List.of("orders"), List.of("orders", "payments.v1"),
                List.of("orders", "payments.v1"), List.of("orders", "payments.v1"),
                List.of("payments.v1", "orders"), List.of("orders", "orders"), List.of("orders"),
                List.of("orders", "$ranges"), List.of("payments.v1"));
        for (int i = 0; i < examples.size(); i++) {
            var query = Query.parseAndResolve(examples.get(i), resolver);
            assertEquals(expected.get(i), query.references().stream().map(r -> r.target().viewName()).toList(), examples.get(i));
            if (i == 2) assertEquals(List.of("o", "p"), query.references().stream()
                    .map(r -> r.table().getAlias().getName()).toList());
        }
    }
}
