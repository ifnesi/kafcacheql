package io.github.ifnesi.kafcacheql.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/** Immutable schemas and the newest observed subject versions. No registry access. */
public final class SchemaStore {
    private SchemaStore() {}

    public record StoredSchema(String ref, String type, String schemaText, String refs, long fetchedMs) {}

    public record SubjectSchema(String subject, int version, StoredSchema schema) {}

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

    /** Stores the schema and advances the subject inside the caller's explicit write transaction. */
    public static void store(Connection writer, SubjectSchema subject) throws SQLException {
        store(writer, subject.schema());
        try (PreparedStatement p = writer.prepareStatement("""
                INSERT INTO "$subjects" (subject, version, schema_ref, fetched_ms) VALUES (?, ?, ?, ?)
                ON CONFLICT(subject) DO UPDATE SET version = excluded.version,
                    schema_ref = excluded.schema_ref, fetched_ms = excluded.fetched_ms
                WHERE excluded.version > "$subjects".version
                """)) {
            p.setString(1, subject.subject());
            p.setInt(2, subject.version());
            p.setString(3, subject.schema().ref());
            p.setLong(4, subject.schema().fetchedMs());
            p.executeUpdate();
        }
    }
}
