package io.github.ifnesi.kafcacheql.cache;

import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import io.github.ifnesi.kafcacheql.schema.AvroJsonConverter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Topic registration on the cache writer (the sync worker's connection). Observations are captured on a
 * reader before contacting Kafka. Under the write lock, a stale observation is rejected or adopts an
 * already registered, live generation with the same Kafka topic ID. Nothing contacts Kafka here.
 */
public final class TopicRegistry {
    private TopicRegistry() {}

    /** A committed generation, its view, and any naming warning for the calling statement. */
    public record Registration(long id, String viewName, Optional<SQLWarning> warning) {
        public String storageTable() {
            return StorageDdl.tableName(id);
        }
    }

    private record Row(long id, String viewName, String topicUuid, boolean deleted) {}

    public record Generation(long id, boolean deleted, int partitions) {}

    /** Includes deleted rows: only a name that has never been registered is absent. */
    public static OptionalLong generation(Connection reader, String topic) throws SQLException {
        Optional<Generation> found = observe(reader, topic);
        return found.isPresent() ? OptionalLong.of(found.get().id()) : OptionalLong.empty();
    }

    public static Optional<Generation> observe(Connection reader, String topic) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement(
                "SELECT id, deleted_ms, partition_count FROM \"$topics\" WHERE topic = ?")) {
            p.setString(1, topic);
            try (ResultSet rs = p.executeQuery()) {
                return rs.next() ? Optional.of(new Generation(rs.getLong(1), rs.getObject(2) != null, rs.getInt(3)))
                        : Optional.empty();
            }
        }
    }

    /**
     * Registers only a never-registered name or the observed deleted generation. An empty result tells
     * the caller to replan. The public registration, including its id and warning, is made only after
     * COMMIT: AUTOINCREMENT can reuse an allocation whose transaction rolled back.
     */
    public static Optional<Registration> register(CacheStore cache, OptionalLong observedGeneration,
            TopicInfo topic, TopicSettings settings) throws SQLException {
        return register(cache, observedGeneration, topic, settings, AvroJsonConverter.VERSION);
    }

    /** Explicit version for registration/rebuild fixtures representing an older decoder. */
    public static Optional<Registration> register(CacheStore cache, OptionalLong observedGeneration,
            TopicInfo topic, TopicSettings settings, int decoderVersion) throws SQLException {
        return register(cache, observedGeneration, topic, settings, decoderVersion, null);
    }

    public static Optional<Registration> register(CacheStore cache, OptionalLong observedGeneration,
            TopicInfo topic, TopicSettings settings, int decoderVersion, CacheStore.Check check) throws SQLException {
        Connection writer = cache.writer();
        Optional<Row> committed = CacheStore.writeTransaction(writer, check, () -> {
            Optional<Row> current = find(writer, topic.name());
            if (current.isPresent()) {
                Row row = current.get();
                if (!row.deleted()) {
                    if (observedGeneration.isPresent() && observedGeneration.getAsLong() != row.id()) return Optional.empty();
                    if (!row.topicUuid().equals(topic.topicId().toString())) return Optional.empty();
                    addPartitions(writer, row.id(), topic.partitions());
                    return current;
                }
                if (observedGeneration.isEmpty() || observedGeneration.getAsLong() != row.id()) {
                    return Optional.empty();
                }
                // Deletion already removed the storage, view, partitions and ranges. Its row alone stays
                // to guard a later registration, which now replaces it under this same write lock.
                try (PreparedStatement p = writer.prepareStatement("DELETE FROM \"$topics\" WHERE id = ?")) {
                    p.setLong(1, row.id());
                    p.executeUpdate();
                }
            } else if (observedGeneration.isPresent()) {
                return Optional.empty();
            }
            String view = viewName(writer, topic.name());
            long id = insert(writer, topic, settings, decoderVersion, view);
            StorageDdl.create(writer, id, topic.mode(), view);
            insertPartitions(writer, id, 0, topic.partitions());
            return Optional.of(new Row(id, view, topic.topicId().toString(), false));
        });
        return committed.map(row -> new Registration(row.id(), row.viewName(), namingWarning(topic.name(), row.viewName())));
    }

    private static int partitionCount(Connection reader, long generation) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement("SELECT partition_count FROM \"$topics\" WHERE id=?")) {
            p.setLong(1, generation);
            try (ResultSet rows = p.executeQuery()) {
                if (!rows.next()) throw new SQLException("Missing topic generation " + generation);
                return rows.getInt(1);
            }
        }
    }

    /** Inside the worker's generation-checked transaction; an older observation never lowers the count. */
    public static void addPartitions(Connection writer, long generation, int count) throws SQLException {
        int before = partitionCount(writer, generation);
        if (before >= count) return;
        insertPartitions(writer, generation, before, count);
        try (PreparedStatement p = writer.prepareStatement("UPDATE \"$topics\" SET partition_count=? WHERE id=?")) {
            p.setInt(1, count); p.setLong(2, generation); p.executeUpdate();
        }
    }

    private static void insertPartitions(Connection writer, long generation, int from, int to) throws SQLException {
        try (PreparedStatement p = writer.prepareStatement(
                "INSERT INTO \"$partitions\" (topic_ref, partition_no) VALUES (?, ?)")) {
            p.setLong(1, generation);
            for (int partition = from; partition < to; partition++) {
                p.setInt(2, partition); p.addBatch();
            }
            p.executeBatch();
        }
    }

    private static Optional<Row> find(Connection reader, String topic) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT id, view_name, topic_uuid, deleted_ms FROM "$topics" WHERE topic = ?
                """)) {
            p.setString(1, topic);
            try (ResultSet rs = p.executeQuery()) {
                return rs.next() ? Optional.of(new Row(rs.getLong(1), rs.getString(2), rs.getString(3),
                        rs.getObject(4) != null)) : Optional.empty();
            }
        }
    }

    private static String viewName(Connection writer, String topic) throws SQLException {
        String base = hasReservedPrefix(topic) ? topic.substring(0, 6) + '-' + topic.substring(7) : topic;
        try (PreparedStatement p = writer.prepareStatement(
                "SELECT 1 FROM \"$topics\" WHERE view_name = ? COLLATE NOCASE")) {
            String candidate = base;
            for (int suffix = 2; ; suffix++) {
                p.setString(1, candidate);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next()) {
                        return candidate;
                    }
                }
                candidate = base + "$" + suffix;
            }
        }
    }

    private static boolean hasReservedPrefix(String topic) {
        return topic.regionMatches(true, 0, "sqlite_", 0, 7);
    }

    private static Optional<SQLWarning> namingWarning(String topic, String view) {
        if (topic.equals(view)) {
            return Optional.empty();
        }
        String reason = hasReservedPrefix(topic) ? "SQLite reserves names starting with sqlite_"
                : "SQLite identifiers are case-insensitive";
        return Optional.of(new SQLWarning("Topic " + topic + " uses view \"" + view + "\" because " + reason, "01000"));
    }

    private static long insert(Connection writer, TopicInfo topic, TopicSettings settings, int decoderVersion,
                               String view) throws SQLException {
        try (PreparedStatement p = writer.prepareStatement("""
                INSERT INTO "$topics" (topic, view_name, topic_uuid, mode, partition_count, timestamp_type,
                                       delete_retention_ms, key_format, value_format, decoder_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id
                """)) {
            p.setString(1, topic.name());
            p.setString(2, view);
            p.setString(3, topic.topicId().toString());
            p.setString(4, topic.mode().value());
            p.setInt(5, topic.partitions());
            p.setString(6, topic.timestampType().toString());
            p.setLong(7, topic.deleteRetentionMs());
            p.setString(8, settings.keyFormat().orElse(Format.AUTO).value());
            p.setString(9, settings.valueFormat().orElse(Format.AUTO).value());
            p.setInt(10, decoderVersion);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Registration of topic " + topic.name() + " returned no id");
                }
                return rs.getLong(1);
            }
        }
    }
}
