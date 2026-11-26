package io.github.ifnesi.kafcacheql.cache;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/** Partition progress and append retention. Mutations belong to the worker's guarded transaction. */
public final class PartitionStore {
    private PartitionStore() {}

    /** Capture once before fetching: later completion timestamps must not hide an expired baseline. */
    public static Map<Integer, Long> safetyBaselines(Connection reader, long generation) throws SQLException {
        var baselines = new HashMap<Integer, Long>();
        try (var p = reader.prepareStatement("""
                SELECT p.partition_no, coalesce(p.caught_up_at_ms, t.created_ms)
                FROM "$partitions" p JOIN "$topics" t ON t.id=p.topic_ref WHERE p.topic_ref=?
                """)) {
            p.setLong(1, generation);
            try (var rows = p.executeQuery()) {
                while (rows.next()) baselines.put(rows.getInt(1), rows.getLong(2));
            }
        }
        return baselines;
    }

    public static long floor(Connection reader, long generation, int partition) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement(
                "SELECT pruned_below FROM \"$partitions\" WHERE topic_ref=? AND partition_no=?")) {
            p.setLong(1, generation); p.setInt(2, partition);
            try (var rows = p.executeQuery()) {
                if (!rows.next()) throw new SQLException("Missing cached partition " + partition + " of generation " + generation);
                return rows.getLong(1);
            }
        }
    }

    /** Also shared by the coalesced, generation-guarded caught-up background write. */
    public static void caughtUp(Connection writer, long generation, int partition, long capturedAt) throws SQLException {
        try (PreparedStatement p = writer.prepareStatement("""
                UPDATE "$partitions" SET caught_up_at_ms = ?
                WHERE topic_ref = ? AND partition_no = ?
                AND (caught_up_at_ms IS NULL OR caught_up_at_ms < ?)
                """)) {
            p.setLong(1, capturedAt); p.setLong(2, generation); p.setInt(3, partition);
            p.setLong(4, capturedAt); p.executeUpdate();
        }
    }

    /** Includes interrupted cleanup: a committed floor may still have rows awaiting deletion. */
    public static boolean needsPruning(Connection reader, long generation, Map<Integer, Long> starts) throws SQLException {
        String table = StorageDdl.quote(StorageDdl.tableName(generation));
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT 1 FROM "$partitions" p WHERE topic_ref=? AND partition_no=?
                AND (pruned_below < ? OR EXISTS (SELECT 1 FROM %s
                    WHERE _partition=p.partition_no AND _offset < max(p.pruned_below, ?)))
                """.formatted(table))) {
            p.setLong(1, generation);
            for (var start : starts.entrySet()) {
                p.setInt(2, start.getKey()); p.setLong(3, start.getValue()); p.setLong(4, start.getValue());
                try (var rows = p.executeQuery()) { if (rows.next()) return true; }
            }
            return false;
        }
    }

    /** All observed floors and range trims commit together, before the first row-deletion chunk. */
    public static void raiseFloors(Connection writer, long generation, Map<Integer, Long> starts) throws SQLException {
        try (PreparedStatement p = writer.prepareStatement("""
                UPDATE "$partitions" SET pruned_below=?
                WHERE topic_ref=? AND partition_no=? AND pruned_below < ?
                """)) {
            p.setLong(2, generation);
            for (var start : starts.entrySet()) {
                p.setLong(1, start.getValue()); p.setInt(3, start.getKey()); p.setLong(4, start.getValue());
                if (p.executeUpdate() != 0) RangeStore.removeBelow(writer, generation, start.getKey(), start.getValue());
            }
        }
    }

    public static int deleteChunk(Connection writer, long generation, int partition, int size) throws SQLException {
        String table = StorageDdl.quote(StorageDdl.tableName(generation));
        // The WITHOUT ROWID table's primary key bounds this scan; SQLite owns the limited deletion.
        return deleteChunk(writer, generation, """
                    SELECT _partition,_offset FROM %s WHERE _partition=? AND _offset < (
                        SELECT pruned_below FROM "$partitions" WHERE topic_ref=? AND partition_no=?)
                    ORDER BY _offset LIMIT ?
                """.formatted(table), List.of(partition, generation, partition, size), false);
    }

    /** The ceiling is the next topic's oldest timestamp, so a chunk preserves global timestamp order. */
    public static int evictChunk(Connection writer, long generation, long ceiling, int size) throws SQLException {
        return deleteChunk(writer, generation, "SELECT _partition,_offset FROM " + StorageDdl.quote(StorageDdl.tableName(generation))
                + " WHERE _timestamp<=? ORDER BY _timestamp LIMIT ?", List.of(ceiling, size), true);
    }

    private static int deleteChunk(Connection writer, long generation, String selection, List<?> parameters, boolean trimRanges)
            throws SQLException {
        String sql = "DELETE FROM " + StorageDdl.quote(StorageDdl.tableName(generation))
                + " WHERE (_partition,_offset) IN (" + selection + ")" + (trimRanges ? " RETURNING _partition,_offset" : "");
        try (var p = writer.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) p.setObject(i + 1, parameters.get(i));
            int deleted = 0;
            if (trimRanges) {
                var removed = new HashMap<Integer, RangeSet<Long>>();
                try (var r = p.executeQuery()) {
                    while (r.next()) {
                        long offset = r.getLong(2);
                        removed.computeIfAbsent(r.getInt(1), ignored -> TreeRangeSet.create()).add(Range.closedOpen(offset, offset + 1));
                        deleted++;
                    }
                }
                for (var entry : removed.entrySet()) for (var range : entry.getValue().asRanges())
                    RangeStore.remove(writer, generation, entry.getKey(), range.lowerEndpoint(), range.upperEndpoint());
            } else deleted = p.executeUpdate(); // Retention already trimmed the ranges when it raised the floor.
            if (deleted != 0) TypedViews.refresh(writer, generation);
            return deleted;
        }
    }
}
