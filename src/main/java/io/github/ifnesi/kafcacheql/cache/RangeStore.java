package io.github.ifnesi.kafcacheql.cache;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Half-open offset ranges persisted per topic generation and partition. Guava owns the interval algebra;
 * SQLite holds its merged representation. Mutations must run under the caller's IMMEDIATE transaction,
 * together with the data and generation check they describe; this class never commits independently.
 */
public final class RangeStore {
    private RangeStore() {}

    /** A mutable range set for planning on the caller's read snapshot. */
    public static RangeSet<Long> load(Connection reader, long topicId, int partition) throws SQLException {
        RangeSet<Long> ranges = TreeRangeSet.create();
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT start_offset, end_offset FROM "$ranges" WHERE topic_ref = ? AND partition_no = ?
                """)) {
            bindPartition(p, topicId, partition);
            try (ResultSet rs = p.executeQuery()) {
                while (rs.next()) {
                    ranges.add(Range.closedOpen(rs.getLong(1), rs.getLong(2)));
                }
            }
        }
        return ranges;
    }

    /** Adds [start, end), touching only the stored ranges it overlaps or adjoins. */
    public static void add(Connection writer, long topicId, int partition, long start, long end) throws SQLException {
        nonnegative(start);
        Range<Long> added = Range.closedOpen(start, end);
        if (added.isEmpty()) {
            return;
        }
        RangeSet<Long> merged = TreeRangeSet.create();
        merged.add(added);
        try (PreparedStatement p = writer.prepareStatement("""
                DELETE FROM "$ranges"
                WHERE topic_ref = ? AND partition_no = ? AND start_offset <= ? AND end_offset >= ?
                RETURNING start_offset, end_offset
                """)) {
            bindPartition(p, topicId, partition);
            p.setLong(3, end);
            p.setLong(4, start);
            try (ResultSet rs = p.executeQuery()) {
                while (rs.next()) {
                    merged.add(Range.closedOpen(rs.getLong(1), rs.getLong(2)));
                }
            }
        }
        Range<Long> range = merged.span();
        try (PreparedStatement p = writer.prepareStatement("INSERT INTO \"$ranges\" VALUES (?, ?, ?, ?)")) {
            bindPartition(p, topicId, partition);
            p.setLong(3, range.lowerEndpoint());
            p.setLong(4, range.upperEndpoint());
            p.executeUpdate();
        }
    }

    /** Needed minus cached, without changing needed. Inputs are bounded, nonnegative, half-open ranges. */
    public static RangeSet<Long> missing(Connection reader, long topicId, int partition, RangeSet<Long> needed)
            throws SQLException {
        RangeSet<Long> missing = TreeRangeSet.create(needed);
        if (missing.isEmpty()) {
            return missing;
        }
        Range<Long> span = missing.span();
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT start_offset, end_offset FROM "$ranges"
                WHERE topic_ref = ? AND partition_no = ? AND start_offset < ? AND end_offset > ?
                """)) {
            bindPartition(p, topicId, partition);
            p.setLong(3, span.upperEndpoint());
            p.setLong(4, span.lowerEndpoint());
            try (ResultSet rs = p.executeQuery()) {
                while (rs.next()) {
                    missing.remove(Range.closedOpen(rs.getLong(1), rs.getLong(2)));
                }
            }
        }
        return missing;
    }

    /** Removes everything below offset, clipping the one range that can cross it. */
    public static void removeBelow(Connection writer, long topicId, int partition, long offset) throws SQLException {
        nonnegative(offset);
        try (PreparedStatement p = writer.prepareStatement("""
                DELETE FROM "$ranges" WHERE topic_ref = ? AND partition_no = ? AND end_offset <= ?
                """)) {
            bindPartition(p, topicId, partition);
            p.setLong(3, offset);
            p.executeUpdate();
        }
        try (PreparedStatement p = writer.prepareStatement("""
                UPDATE "$ranges" SET start_offset = ?
                WHERE topic_ref = ? AND partition_no = ? AND start_offset < ?
                """)) {
            p.setLong(1, offset);
            p.setLong(2, topicId);
            p.setInt(3, partition);
            p.setLong(4, offset);
            p.executeUpdate();
        }
    }

    private static void bindPartition(PreparedStatement p, long topicId, int partition) throws SQLException {
        p.setLong(1, topicId);
        p.setInt(2, partition);
    }

    private static void nonnegative(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("An offset must be nonnegative");
        }
    }
}
