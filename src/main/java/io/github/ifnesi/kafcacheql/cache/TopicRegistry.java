package io.github.ifnesi.kafcacheql.cache;

import io.github.ifnesi.kafcacheql.config.Format;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.config.TopicSettings;
import io.github.ifnesi.kafcacheql.kafka.TopicInfo;
import io.github.ifnesi.kafcacheql.schema.AvroJsonConverter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.OptionalLong;

/**
 * Topic registration on the cache writer (the sync worker's connection). Observations are captured on a
 * reader before contacting Kafka. Under the write lock, a stale observation is rejected or adopts an
 * already registered, live generation with the same Kafka topic ID. Nothing contacts Kafka here.
 */
public final class TopicRegistry {
    private static final Logger log = LoggerFactory.getLogger(TopicRegistry.class);

    private TopicRegistry() {}

    /** A committed generation, its view, and any naming warning for the calling statement. */
    public record Registration(long id, String viewName, Optional<SQLWarning> warning) {
        public String storageTable() {
            return StorageDdl.tableName(id);
        }
    }

    private record Row(long id, String viewName, String topicUuid, boolean deleted) {}

    private record Change(Row row, String reason) {}

    public record Generation(long id, boolean deleted, int partitions) {}

    /** A SQL view's Kafka topic, or an internal cache table with no topic to sync. */
    public record ResolvedTable(String viewName, Optional<String> topic, Optional<SQLWarning> warning) {}

    public static Optional<ResolvedTable> resolve(Connection reader, String name) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT s.name, t.topic FROM sqlite_schema s
                LEFT JOIN "$topics" t ON s.name = t.view_name AND t.deleted_ms IS NULL
                WHERE s.name = ? COLLATE NOCASE AND s.type IN ('table', 'view')
                  AND (t.topic IS NOT NULL OR substr(s.name, 1, 1) = '$')
                """)) {
            p.setString(1, name);
            try (ResultSet rs = p.executeQuery()) {
                return rs.next() ? Optional.of(new ResolvedTable(rs.getString(1), Optional.ofNullable(rs.getString(2)),
                        Optional.empty())) : Optional.empty();
            }
        }
    }

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
        return register(cache, observedGeneration, topic, settings, decoderVersion, check, List.of(topic.name()));
    }

    /** Discovery and registration allocate from the same topic set, under the writer lock at registration. */
    public static Optional<Registration> register(CacheStore cache, OptionalLong observedGeneration,
            TopicInfo topic, TopicSettings settings, int decoderVersion, CacheStore.Check check,
            Collection<String> knownTopics) throws SQLException {
        return prepare(cache, observedGeneration, true, topic, settings, decoderVersion, null, check, knownTopics);
    }

    /** Register or replace using the same policy the caller checked on its read snapshot. */
    public static Optional<Registration> prepare(CacheStore cache, OptionalLong observedGeneration, boolean observedDeleted,
            TopicInfo topic, TopicSettings settings, Map<Integer, Long> logStarts,
            CacheStore.Check check) throws SQLException {
        return prepare(cache, observedGeneration, observedDeleted, topic, settings, AvroJsonConverter.VERSION,
                logStarts, check, List.of(topic.name()));
    }

    private static Optional<Registration> prepare(CacheStore cache, OptionalLong observedGeneration, boolean observedDeleted,
            TopicInfo topic, TopicSettings settings, int decoderVersion, Map<Integer, Long> logStarts,
            CacheStore.Check check, Collection<String> knownTopics) throws SQLException {
        Connection writer = cache.writer();
        Optional<Change> committed = CacheStore.writeTransaction(writer, check, () -> {
            Optional<Row> current = find(writer, topic.name());
            String reason = null;
            if (current.isPresent()) {
                Row row = current.get();
                if (observedGeneration.isPresent() && observedGeneration.getAsLong() != row.id()) return Optional.empty();
                if (!row.deleted()) {
                    if (logStarts == null) {
                        if (!row.topicUuid().equals(topic.topicId().toString())) return Optional.empty();
                    } else {
                        reason = rebuildReason(writer, row.id(), topic, settings, logStarts).orElse(null);
                    }
                    if (reason == null) {
                        addPartitions(writer, row.id(), topic.partitions());
                        return Optional.of(new Change(row, null));
                    }
                    // An absent observation may adopt a matching registration, but never replace one.
                    if (observedGeneration.isEmpty()) return Optional.empty();
                    removeStorage(writer, row);
                } else if (observedGeneration.isEmpty() || !observedDeleted) return Optional.empty();
                try (PreparedStatement p = writer.prepareStatement("DELETE FROM \"$topics\" WHERE id = ?")) {
                    p.setLong(1, row.id()); p.executeUpdate();
                }
            } else if (observedGeneration.isPresent()) return Optional.empty();
            String view = current.isPresent() ? current.get().viewName() : viewNames(writer, knownTopics).get(topic.name());
            long id = insert(writer, topic, settings, decoderVersion, view);
            StorageDdl.create(writer, id, topic.mode(), view);
            insertPartitions(writer, id, 0, topic.partitions());
            return Optional.of(new Change(new Row(id, view, topic.topicId().toString(), false), reason));
        });
        return committed.map(change -> {
            if (change.reason() != null) log.warn("Rebuilding topic {}: {}", topic.name(), change.reason());
            Row row = change.row();
            return new Registration(row.id(), row.viewName(), namingWarning(topic.name(), row.viewName()));
        });
    }

    /** Pure read-side check, repeated on current rows under the write lock before a replacement. */
    public static Optional<String> rebuildReason(Connection reader, long generation, TopicInfo topic,
            TopicSettings settings, Map<Integer, Long> logStarts) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT topic_uuid, mode, key_format, value_format, decoder_version
                FROM "$topics" WHERE id=? AND deleted_ms IS NULL
                """)) {
            p.setLong(1, generation);
            try (ResultSet row = p.executeQuery()) {
                if (!row.next()) return Optional.empty();
                String[] names = {"topic ID", "mode", "key.format", "value.format", "decoder_version"};
                String[] expected = {topic.topicId().toString(), topic.mode().value(),
                        settings.keyFormat().orElse(Format.AUTO).value(), settings.valueFormat().orElse(Format.AUTO).value(),
                        Integer.toString(AvroJsonConverter.VERSION)};
                for (int i = 0; i < names.length; i++) {
                    String before = row.getString(i + 1);
                    if (!before.equals(expected[i])) return Optional.of(names[i] + " changed from " + before + " to " + expected[i]);
                }
            }
        }
        if (topic.mode() != Mode.COMPACTED) return Optional.empty();
        long now = System.currentTimeMillis();
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT p.partition_no, coalesce(p.caught_up_at_ms, t.created_ms), (SELECT min(start_offset) FROM "$ranges" r
                    WHERE r.topic_ref=p.topic_ref AND r.partition_no=p.partition_no), p.caught_up_at_ms IS NULL
                FROM "$partitions" p JOIN "$topics" t ON t.id=p.topic_ref WHERE p.topic_ref=?
                """)) {
            p.setLong(1, generation);
            try (ResultSet rows = p.executeQuery()) {
                while (rows.next()) {
                    int partition = rows.getInt(1);
                    Long start = logStarts.get(partition);
                    if (start != null && rows.getObject(3) != null && rows.getLong(3) < start) {
                        return Optional.of("cached offset " + rows.getLong(3) + " below log start " + start + " in partition " + partition);
                    }
                    // Until the first complete sync there can already be committed rows. Their safety
                    // baseline is the generation's creation, not a fabricated caught-up timestamp.
                    if (now - rows.getLong(2) > topic.deleteRetentionMs()) {
                        return Optional.of((rows.getBoolean(4) ? "first added " : "last full sync ")
                                + duration(now - rows.getLong(2)) + " ago, longer than delete.retention.ms ("
                                + duration(topic.deleteRetentionMs()) + ")");
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static String duration(long milliseconds) {
        return java.time.Duration.ofMillis(milliseconds).toString().substring(2).toLowerCase(java.util.Locale.ROOT);
    }

    /** Keep the deleted generation as an observation guard; no Kafka operation occurs under the lock. */
    public static boolean delete(CacheStore cache, String topic, OptionalLong observed, CacheStore.Check check) throws SQLException {
        return CacheStore.writeTransaction(cache.writer(), check, () -> {
            Optional<Row> current = find(cache.writer(), topic);
            if (current.isEmpty()) return observed.isEmpty();
            Row row = current.get();
            if (observed.isEmpty() || observed.getAsLong() != row.id()) return false;
            if (!row.deleted()) {
                removeStorage(cache.writer(), row);
                try (PreparedStatement p = cache.writer().prepareStatement("UPDATE \"$topics\" SET deleted_ms=? WHERE id=?")) {
                    p.setLong(1, System.currentTimeMillis()); p.setLong(2, row.id()); p.executeUpdate();
                }
            }
            return true;
        });
    }

    private static void removeStorage(Connection writer, Row row) throws SQLException {
        StorageDdl.drop(writer, row.id(), row.viewName());
        for (String table : java.util.List.of("$ranges", "$partitions")) {
            try (PreparedStatement p = writer.prepareStatement("DELETE FROM " + StorageDdl.quote(table) + " WHERE topic_ref=?")) {
                p.setLong(1, row.id()); p.executeUpdate();
            }
        }
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

    /** Predict names without DDL, preserving every registered name (including deleted generations). */
    public static Map<String, String> viewNames(Connection reader, Collection<String> topics) throws SQLException {
        var registered = new HashMap<String, String>();
        var used = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT topic, view_name FROM \"$topics\"")) {
            while (rows.next()) { registered.put(rows.getString(1), rows.getString(2)); used.add(rows.getString(2)); }
        }
        var names = new HashMap<String, String>();
        for (String topic : new TreeSet<>(topics)) {
            String view = registered.get(topic);
            if (view == null) {
                String base = baseViewName(topic);
                view = base;
                for (int suffix = 2; !used.add(view); suffix++) view = base + "$" + suffix;
            }
            names.put(topic, view);
        }
        return Map.copyOf(names);
    }

    public static String baseViewName(String topic) {
        return hasReservedPrefix(topic) ? topic.substring(0, 6) + '-' + topic.substring(7) : topic;
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
