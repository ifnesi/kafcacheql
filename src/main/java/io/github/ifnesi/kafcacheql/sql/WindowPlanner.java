package io.github.ifnesi.kafcacheql.sql;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncPlan;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import org.apache.kafka.common.record.TimestampType;

/** One topic observation shared by all its SQL references. All output intervals are half-open offsets. */
final class WindowPlanner {
    private final CacheContext.Lease cache;
    private final SyncPlan base;
    private final Execution execution;
    private final long startedAt;
    private final Map<Long, Map<Integer, OptionalLong>> times = new HashMap<>();

    WindowPlanner(CacheContext.Lease cache, SyncPlan base, Execution execution, long startedAt) {
        this.cache = cache; this.base = base; this.execution = execution; this.startedAt = startedAt;
    }

    Map<Integer, ImmutableRangeSet<Long>> windows(Bounds bounds, List<SQLWarning> warnings) throws SQLException {
        if (base.topic().mode() == Mode.COMPACTED) return base.windows();
        boolean defaults = !bounds.hasWindowBound();
        var window = cache.config().appendDefaultWindow(base.topic().name());
        long maxRecords = cache.config().appendDefaultWindowMaxRecords(base.topic().name());
        if (defaults) {
            String description = window.isAll() ? "all" : "last " + window.duration().orElseThrow().toString()
                    .substring(2).toLowerCase(Locale.ROOT) + ", ≤" + maxRecords + " records/partition";
            warnings.add(new SQLWarning(base.topic().name() + ": default window applied (" + description
                    + "); add a `_timestamp`/`_offset` bound to override", "01000"));
        }
        var result = new TreeMap<Integer, ImmutableRangeSet<Long>>();
        for (var partition : base.ends().offsets().entrySet()) {
            int number = partition.getKey();
            if (!bounds.partitions().contains((long) number)) continue;
            long end = partition.getValue();
            var wanted = TreeRangeSet.create(bounds.offsets().subRangeSet(Range.closedOpen(0L, end)));
            var timestamps = TreeRangeSet.<Long>create();
            for (var range : bounds.timestamps().subRangeSet(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE)).asRanges()) {
                var discrete = range.canonical(DiscreteDomain.longs());
                long from = discrete.hasLowerBound() && discrete.lowerEndpoint() > 0
                        ? at(discrete.lowerEndpoint(), number, end) : 0;
                long to = discrete.hasUpperBound() && base.topic().timestampType() == TimestampType.LOG_APPEND_TIME
                        ? at(discrete.upperEndpoint(), number, end) : end;
                if (from < to) timestamps.add(Range.closedOpen(from, to));
            }
            wanted.removeAll(timestamps.complement());
            if (defaults && !window.isAll()) {
                long from = Math.max(at(startedAt - window.duration().orElseThrow().toMillis(), number, end), end - maxRecords);
                wanted.remove(Range.lessThan(from));
            }
            var canonical = ImmutableRangeSet.<Long>builder();
            for (var range : wanted.asRanges()) {
                var discrete = range.canonical(DiscreteDomain.longs());
                if (!discrete.isEmpty()) canonical.add(discrete);
            }
            result.put(number, canonical.build());
        }
        return result;
    }

    private long at(long timestamp, int partition, long end) throws SQLException {
        if (timestamp <= 0) return base.logStarts().get(partition);
        var offsets = times.get(timestamp);
        if (offsets == null) {
            offsets = cache.offsetsForTimes(base.topic(), timestamp, execution);
            times.put(timestamp, offsets);
        }
        return Math.min(end, offsets.get(partition).orElse(end));
    }
}
