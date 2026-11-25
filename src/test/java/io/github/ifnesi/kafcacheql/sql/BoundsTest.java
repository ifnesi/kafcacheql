package io.github.ifnesi.kafcacheql.sql;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry.ResolvedTable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BoundsTest {
    private static final ImmutableRangeSet<Long> ALL = ImmutableRangeSet.of(Range.all());
    private static final Bounds UNBOUNDED = new Bounds(ALL, ALL, ALL, false);
    private record Shape(String sql, Map<Integer, ?> parameters, List<Bounds> expected) {
        Shape(String sql, Bounds... expected) { this(sql, Map.of(), List.of(expected)); }
    }

    private static Query query(String sql) throws Exception {
        return Query.parseAndResolve(sql, name -> new ResolvedTable(name,
                name.startsWith("$") ? Optional.empty() : Optional.of(name), Optional.empty()));
    }

    private static Bounds offset(Range<Long> range) { return new Bounds(ALL, ImmutableRangeSet.of(range), ALL, true); }
    private static Bounds time(Range<Long> range) { return new Bounds(ALL, ALL, ImmutableRangeSet.of(range), true); }

    /** PLAN §8.3: "Bounds are only taken from top-level AND conditions in the same SELECT as the table
     * reference. The column must clearly belong to that reference." Constants are evaluated on SQLite;
     * "A _partition bound on its own doesn't switch off the default window".
     */
    @Test void sqlShapesHaveThePlannedBounds() throws Exception {
        var shapes = List.of(
                new Shape("SELECT * FROM orders", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _offset>=5 AND _offset<12", offset(Range.closedOpen(5L, 12L))),
                new Shape("SELECT * FROM orders WHERE _offset>=5 OR _partition=1", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE NOT (_offset<5)", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _partition=1 AND (_offset<5 OR _offset>10)",
                        new Bounds(ImmutableRangeSet.of(Range.singleton(1L)), ALL, ALL, false)),
                new Shape("SELECT * FROM orders WHERE (((_offset)>=5) AND (_offset<=12 AND (_partition=1)))",
                        new Bounds(ImmutableRangeSet.of(Range.singleton(1L)), ImmutableRangeSet.of(Range.closed(5L, 12L)), ALL, true)),
                new Shape("SELECT * FROM orders WHERE 5 < _offset AND 12 >= _offset", offset(Range.openClosed(5L, 12L))),
                new Shape("SELECT * FROM orders WHERE 5 = _offset", offset(Range.singleton(5L))),
                new Shape("SELECT * FROM orders WHERE _partition IN (0,2) AND _offset IN (1,4,9) AND _timestamp BETWEEN 10 AND 20",
                        new Bounds(ImmutableRangeSet.<Long>builder().add(Range.singleton(0L)).add(Range.singleton(2L)).build(),
                                ImmutableRangeSet.<Long>builder().add(Range.singleton(1L)).add(Range.singleton(4L)).add(Range.singleton(9L)).build(),
                                ImmutableRangeSet.of(Range.closed(10L, 20L)), true)),
                new Shape("SELECT * FROM orders WHERE _offset BETWEEN 3 AND 7", offset(Range.closed(3L, 7L))),
                new Shape("SELECT * FROM orders WHERE _offset BETWEEN 1 AND 3 = TRUE", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _offset BETWEEN 1 AND (2 OR 3)", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _timestamp=10", time(Range.singleton(10L))),
                new Shape("SELECT * FROM orders WHERE _timestamp>10 AND _timestamp<=20", time(Range.openClosed(10L, 20L))),
                new Shape("SELECT * FROM orders WHERE _offset NOT IN (1,2) AND _timestamp NOT BETWEEN 10 AND 20", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _partition>1 AND _timestamp IN (10,20) AND _offset!=7", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE abs(_offset)>5 AND status='ok'", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _offset>=coalesce(_timestamp,5)", UNBOUNDED),
                new Shape("SELECT * FROM orders WHERE _offset>=(SELECT 5)", UNBOUNDED),
                new Shape("SELECT * FROM orders o JOIN payments p ON o._key=p._key WHERE o._offset>=5 AND p._timestamp<20",
                        offset(Range.atLeast(5L)), time(Range.lessThan(20L))),
                new Shape("SELECT * FROM orders o JOIN orders p ON o._key=p._key WHERE o._offset>=5 AND p._offset<10",
                        offset(Range.atLeast(5L)), offset(Range.lessThan(10L))),
                new Shape("SELECT * FROM orders o JOIN payments p ON o._key=p._key WHERE _offset>=5", UNBOUNDED, UNBOUNDED),
                new Shape("SELECT * FROM orders o LEFT JOIN payments p ON p._offset<5 WHERE o._offset>=10",
                        offset(Range.atLeast(10L)), UNBOUNDED),
                new Shape("WITH c AS (SELECT * FROM orders WHERE _offset>=3) SELECT * FROM c WHERE _offset>=10",
                        offset(Range.atLeast(3L))),
                new Shape("SELECT * FROM (SELECT * FROM orders WHERE _offset<8) q WHERE _offset>=3", offset(Range.lessThan(8L))),
                new Shape("WITH c AS (SELECT * FROM payments) SELECT * FROM orders o JOIN c ON 1=1 WHERE _offset>=3",
                        UNBOUNDED, UNBOUNDED),
                new Shape("SELECT * FROM orders o WHERE _offset>=3 AND EXISTS (SELECT 1 FROM payments p WHERE p._timestamp<20 AND p._offset=o._offset)",
                        offset(Range.atLeast(3L)), time(Range.lessThan(20L))),
                new Shape("SELECT * FROM \"orders.v1\" AS \"O\" WHERE \"o\".\"_OFFSET\">=5", offset(Range.atLeast(5L))),
                new Shape("SELECT * FROM \"orders.v1\" WHERE \"orders.v1\"._offset>=5", offset(Range.atLeast(5L))),
                new Shape("SELECT * FROM \"orders.v1\" WHERE \"main\".\"orders.v1\"._offset>=5", offset(Range.atLeast(5L))),
                new Shape("SELECT ? FROM orders WHERE _offset>? AND _timestamp<=?", Map.of(1, 999, 2, 5L, 3, 20L),
                        List.of(new Bounds(ALL, ImmutableRangeSet.of(Range.greaterThan(5L)), ImmutableRangeSet.of(Range.atMost(20L)), true))),
                new Shape("SELECT * FROM orders WHERE _offset>=abs(-7)+3 AND _timestamp<unixepoch('1970-01-01T00:00:10Z')*1000",
                        new Bounds(ALL, ImmutableRangeSet.of(Range.atLeast(10L)), ImmutableRangeSet.of(Range.lessThan(10000L)), true)),
                new Shape("SELECT * FROM orders WHERE _offset<=2.5", offset(Range.atMost(2L))),
                new Shape("SELECT * FROM orders WHERE _offset>=?", Collections.singletonMap(1, null),
                        List.of(new Bounds(ALL, ImmutableRangeSet.of(), ALL, true))),
                new Shape("SELECT * FROM orders WHERE _offset>=8 AND _offset<4", new Bounds(ALL, ImmutableRangeSet.of(), ALL, true)),
                new Shape("SELECT * FROM orders WHERE _offset>'not numeric'", new Bounds(ALL, ALL, ALL, true)),
                new Shape("SELECT * FROM orders WHERE _offset>=:named", UNBOUNDED),
                new Shape("SELECT * FROM \"$ranges\" WHERE _partition=1", UNBOUNDED));
        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            for (var shape : shapes) {
                var bounds = Bounds.extract(query(shape.sql()), sqlite, shape.parameters());
                assertEquals(shape.expected(), List.copyOf(bounds.values()), shape.sql());
            }
        }
    }

}
