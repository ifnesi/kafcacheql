package io.github.ifnesi.kafcacheql.sql;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
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
    private record Counted(long start, long records) {}
    private final Map<Integer, Counted> tailCounts = new HashMap<>();

    WindowPlanner(CacheContext.Lease cache, SyncPlan base, Execution execution, long startedAt) {
        this.cache = cache; this.base = base; this.execution = execution; this.startedAt = startedAt;
    }

    Map<Integer, ImmutableRangeSet<Long>> windows(Bounds bounds, List<SQLWarning> warnings) throws SQLException {
        if (base.topic().mode() == Mode.COMPACTED) return base.windows();
        boolean defaults = !bounds.hasWindowBound() && bounds.tailRecords().isEmpty();
        var window = cache.config().appendDefaultWindow(base.topic().name());
        long maxRecords = cache.config().appendDefaultWindowMaxRecords(base.topic().name());
        if (defaults && !window.isAll()) {
            String description = "last " + window.duration().orElseThrow().toString()
                    .substring(2).toLowerCase(Locale.ROOT) + ", ≤" + maxRecords + " records/partition";
            warnings.add(new SQLWarning(base.topic().name() + ": default window applied (" + description
                    + "); add a `_timestamp`/`_offset` bound to override", "01000"));
        }
        // Apply offset/partition bounds and the record cap before using cached timestamps. These are
        // the candidate intervals whose coverage must be proved before time can narrow them offline.
        var candidates = new TreeMap<Integer, ImmutableRangeSet<Long>>();
        for (var partition : base.ends().offsets().entrySet()) {
            int number = partition.getKey();
            if (!bounds.partitions().contains((long) number)) continue;
            long end = partition.getValue();
            var wanted = TreeRangeSet.<Long>create();
            for (var range : bounds.offsets().subRangeSet(Range.atLeast(0L)).asRanges()) {
                var discrete = range.canonical(DiscreteDomain.longs());
                long limit = discrete.hasUpperBound() ? (cache.config().offline() ? discrete.upperEndpoint() : Math.min(end, discrete.upperEndpoint())) : end;
                long from = discrete.lowerEndpoint();
                if (from < limit) wanted.add(Range.closedOpen(from, limit));
            }
            if (defaults && !window.isAll()) wanted.remove(Range.lessThan(end - maxRecords));
            if (bounds.tailRecords().isPresent()) {
                wanted.remove(Range.lessThan(Math.max(base.logStarts().get(number), end - bounds.tailRecords().getAsLong())));
            }
            if (cache.config().offline() && (defaults || bounds.offsets().encloses(Range.all()))) {
                wanted.remove(Range.lessThan(base.logStarts().get(number)));
            }
            candidates.put(number, ImmutableRangeSet.copyOf(wanted));
        }
        SyncPlan lookup = base.withWindows(candidates);
        // One planner serves multiple references: an offline proof belongs to this reference's candidates.
        if (cache.config().offline()) times.clear();
        var newest = cache.config().offline() && defaults && !window.isAll() ? cache.newestTimestamps(lookup, execution) : Map.<Integer, Long>of();
        var result = new TreeMap<Integer, ImmutableRangeSet<Long>>();
        for (var partition : candidates.entrySet()) {
            int number = partition.getKey();
            long end = base.ends().offsets().get(number);
            var wanted = TreeRangeSet.create(partition.getValue());
            var timestamps = TreeRangeSet.<Long>create();
            for (var range : bounds.timestamps().subRangeSet(Range.closed(Long.MIN_VALUE, Long.MAX_VALUE)).asRanges()) {
                var discrete = range.canonical(DiscreteDomain.longs());
                long from = discrete.hasLowerBound() && discrete.lowerEndpoint() > 0
                        ? at(discrete.lowerEndpoint(), number, end, lookup) : 0;
                long to = discrete.hasUpperBound() && base.topic().timestampType() == TimestampType.LOG_APPEND_TIME
                        ? at(discrete.upperEndpoint(), number, end, lookup) : cache.config().offline() ? Long.MAX_VALUE : end;
                if (from < to) timestamps.add(Range.closedOpen(from, to));
            }
            wanted.removeAll(timestamps.complement());
            if (defaults && !window.isAll()) {
                long anchor = cache.config().offline() ? newest.getOrDefault(number, 0L) : startedAt;
                long from = at(anchor - window.duration().orElseThrow().toMillis(), number, end, lookup);
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

    /** Count each fetched slice once, on a checked snapshot, before extending only its shortfall. */
    SyncPlan extendTail(SyncPlan plan, long generation, long records) throws SQLException {
        if (base.topic().mode() == Mode.COMPACTED || records == 0) return plan;
        return cache.read(execution, reader -> {
            cache.validate(reader, plan, generation);
            Map<Integer, ImmutableRangeSet<Long>> expanded = null;
            try (var p = reader.prepareStatement("SELECT count(*) FROM (SELECT 1 FROM "
                    + StorageDdl.quote(StorageDdl.tableName(generation))
                    + " WHERE _partition=? AND _offset>=? AND _offset<? LIMIT ?)")) {
                for (var partition : plan.windows().entrySet()) {
                    if (partition.getValue().isEmpty()) continue;
                    int number = partition.getKey();
                    var range = partition.getValue().span();
                    long start = range.lowerEndpoint(), logStart = plan.logStarts().get(number);
                    if (start == logStart) continue;
                    Counted previous = tailCounts.get(number);
                    long count = previous == null ? 0 : previous.records();
                    if (count == records) continue;
                    p.setInt(1, number); p.setLong(2, start);
                    p.setLong(3, previous == null ? range.upperEndpoint() : previous.start());
                    p.setLong(4, records - count);
                    try (var rows = p.executeQuery()) { rows.next(); count += rows.getLong(1); }
                    tailCounts.put(number, new Counted(start, count));
                    if (count < records) {
                        if (expanded == null) expanded = new TreeMap<>(plan.windows());
                        expanded.put(number, ImmutableRangeSet.of(Range.closedOpen(
                                Math.max(logStart, start - (records - count)), range.upperEndpoint())));
                    }
                }
            }
            return expanded == null ? plan : SyncPlan.create(plan.topic(), OptionalLong.of(generation), false,
                    expanded, plan.ends(), plan.logStarts(), plan.source());
        });
    }

    private long at(long timestamp, int partition, long end, SyncPlan lookup) throws SQLException {
        if (timestamp <= 0) return base.logStarts().get(partition);
        var offsets = times.get(timestamp);
        if (offsets == null) {
            offsets = cache.offsetsForTimes(lookup, timestamp, execution);
            times.put(timestamp, offsets);
        }
        return Math.min(end, offsets.get(partition).orElse(end));
    }
}
