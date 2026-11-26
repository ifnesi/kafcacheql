package io.github.ifnesi.kafcacheql.cache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;

/** Eviction-policy observations on the worker's connection; no transaction or retained process-local state. */
public final class EvictionStore {
    private EvictionStore() {}

    public record Topic(String name, long generation) {}
    public record Append(Topic topic, long timestamp, long ceiling) {}
    private record Head(Topic topic, long timestamp) {}

    /** Called under the write lock; each timestamp index supplies only its first row. */
    public static Optional<Append> oldestAppend(Connection writer) throws SQLException {
        var heads = new ArrayList<Head>();
        try (var s = writer.createStatement(); var topics = s.executeQuery(
                "SELECT topic,id FROM \"$topics\" WHERE mode='append' AND deleted_ms IS NULL ORDER BY id")) {
            while (topics.next()) {
                var topic = new Topic(topics.getString(1), topics.getLong(2));
                try (var q = writer.createStatement(); var r = q.executeQuery("SELECT _timestamp FROM "
                        + StorageDdl.quote(StorageDdl.tableName(topic.generation())) + " ORDER BY _timestamp LIMIT 1")) {
                    if (r.next()) heads.add(new Head(topic, r.getLong(1)));
                }
            }
        }
        if (heads.isEmpty()) return Optional.empty();
        heads.sort(Comparator.comparingLong(Head::timestamp));
        return Optional.of(new Append(heads.getFirst().topic(), heads.getFirst().timestamp(), heads.size() == 1 ? Long.MAX_VALUE : heads.get(1).timestamp()));
    }

    public static Optional<Topic> oldestCompacted(Connection reader) throws SQLException {
        try (var s = reader.createStatement(); var r = s.executeQuery("""
                SELECT topic,id FROM "$topics" WHERE mode='compacted' AND deleted_ms IS NULL
                ORDER BY last_query_ms,id LIMIT 1
                """)) {
            return r.next() ? Optional.of(new Topic(r.getString(1), r.getLong(2))) : Optional.empty();
        }
    }

    /** A stale or out-of-order disposable observation never changes a newer generation or timestamp. */
    public static void queried(Connection writer, String topic, long generation, long at) throws SQLException {
        try (var p = writer.prepareStatement("""
                UPDATE "$topics" SET last_query_ms=? WHERE topic=? AND id=? AND deleted_ms IS NULL AND last_query_ms<?
                """)) {
            p.setLong(1, at); p.setString(2, topic); p.setLong(3, generation); p.setLong(4, at); p.executeUpdate();
        }
    }
}
