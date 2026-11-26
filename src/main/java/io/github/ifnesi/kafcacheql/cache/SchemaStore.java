package io.github.ifnesi.kafcacheql.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Comparator;

/** Immutable schemas, addressed only by record ID or GUID. No registry access. */
public final class SchemaStore {
    private SchemaStore() {}

    public record StoredSchema(String ref, String type, String schemaText, String refs, long fetchedMs) {}

    /** A Protobuf schema can contain several messages; other formats have no message index. */
    public record RecordType(String ref, String messageIndex) implements Comparable<RecordType> {
        private static final Comparator<RecordType> ORDER = Comparator.comparing(RecordType::ref)
                .thenComparing(RecordType::messageIndex, Comparator.nullsFirst(Comparator.naturalOrder()));
        @Override public int compareTo(RecordType other) { return ORDER.compare(this, other); }
    }

    public static Optional<StoredSchema> find(Connection reader, String ref) throws SQLException {
        try (PreparedStatement p = reader.prepareStatement("""
                SELECT type, schema_text, refs, fetched_ms FROM "$schemas" WHERE ref = ?
                """)) {
            p.setString(1, ref);
            try (ResultSet rs = p.executeQuery()) {
                return rs.next() ? Optional.of(new StoredSchema(ref, rs.getString(1), rs.getString(2),
                        rs.getString(3), rs.getLong(4))) : Optional.empty();
            }
        }
    }

    /** Called inside CacheStore.writeTransaction, alongside the batch's other changes. */
    public static void store(Connection writer, StoredSchema schema) throws SQLException {
        try (PreparedStatement p = writer.prepareStatement("""
                INSERT INTO "$schemas" (ref, type, schema_text, refs, fetched_ms) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(ref) DO NOTHING
                """)) {
            p.setString(1, schema.ref());
            p.setString(2, schema.type());
            p.setString(3, schema.schemaText());
            p.setString(4, schema.refs());
            p.setLong(5, schema.fetchedMs());
            p.executeUpdate();
        }
    }

}
