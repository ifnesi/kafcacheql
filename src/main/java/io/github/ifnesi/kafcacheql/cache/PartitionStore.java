package io.github.ifnesi.kafcacheql.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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
        try (PreparedStatement p = writer.prepareStatement("""
                DELETE FROM %s WHERE _partition=? AND _offset IN (
                    SELECT _offset FROM %s WHERE _partition=? AND _offset < (
                        SELECT pruned_below FROM "$partitions" WHERE topic_ref=? AND partition_no=?)
                    ORDER BY _offset LIMIT ?)
                """.formatted(table, table))) {
            p.setInt(1, partition); p.setInt(2, partition); p.setLong(3, generation);
            p.setInt(4, partition); p.setInt(5, size);
            return p.executeUpdate();
        }
    }
}
