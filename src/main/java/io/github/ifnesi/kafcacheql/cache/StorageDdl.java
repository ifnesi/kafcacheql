package io.github.ifnesi.kafcacheql.cache;

import io.github.ifnesi.kafcacheql.config.Mode;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

/** The fixed record layout; schema evolution changes views, never these storage tables. */
public final class StorageDdl {
    static final List<String> METADATA = List.of("_partition", "_offset", "_timestamp", "_key", "_value", "_headers",
            "_key_schema_id", "_value_schema_id", "_decode_error");

    private StorageDdl() {}

    public static String tableName(long id) {
        return "$t" + id;
    }

    public static String quote(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    static void drop(Connection writer, long id, String viewName) throws SQLException {
        try (Statement s = writer.createStatement()) {
            s.execute("DROP VIEW " + quote(viewName));
            s.execute("DROP TABLE " + quote(tableName(id)));
        }
    }

    /** Caller owns the write transaction; id is still private to that transaction until COMMIT. */
    static void create(Connection writer, long id, Mode mode, String viewName) throws SQLException {
        String name = tableName(id);
        String table = quote(name);
        try (Statement s = writer.createStatement()) {
            s.execute("CREATE TABLE " + table + """
                     (
                      _partition       INTEGER NOT NULL,
                      _offset          INTEGER NOT NULL,
                      _timestamp       INTEGER NOT NULL,
                      _key_raw         BLOB,
                      _key             TEXT,
                      _key_schema_id   TEXT,
                      _key_message_index TEXT,
                      _value           TEXT,
                      _value_schema_id TEXT,
                      _value_message_index TEXT,
                      _headers         TEXT,
                      _decode_error    TEXT,
                      PRIMARY KEY (_partition, _offset)
                    ) WITHOUT ROWID
                    """);
            s.execute("CREATE INDEX " + quote(name + "_ts") + " ON " + table + "(_timestamp)");
            for (String side : List.of("key", "value")) {
                String column = "_" + side + "_schema_id";
                s.execute("CREATE INDEX " + quote(name + "_" + side + "_schema") + " ON " + table
                        + "(" + column + ",_" + side + "_message_index) WHERE " + column + " IS NOT NULL");
            }
            if (mode == Mode.COMPACTED) {
                s.execute("CREATE UNIQUE INDEX " + quote(name + "_key") + " ON " + table + "(_partition, _key_raw)");
            }
        }
        TypedViews.refresh(writer, id);
    }

    private static String viewName(Connection reader, long id) throws SQLException {
        try (var p = reader.prepareStatement("SELECT view_name FROM \"$topics\" WHERE id=?")) {
            p.setLong(1, id);
            try (var rows = p.executeQuery()) {
                if (!rows.next()) throw new SQLException("Missing topic generation " + id);
                return rows.getString(1);
            }
        }
    }

    private static String viewHeader(String view, String signature) {
        // JSON escapes line breaks; the comment preserves schema IDs and inferred fields in the cache file.
        return "CREATE VIEW " + quote(view) + " -- schemas: " + signature + "\nAS SELECT ";
    }

    static Optional<String> viewSignature(Connection reader, long id) throws SQLException {
        String view = viewName(reader, id);
        try (var p = reader.prepareStatement("SELECT sql FROM sqlite_schema WHERE type='view' AND name=?")) {
            p.setString(1, view);
            try (var rows = p.executeQuery()) {
                if (!rows.next()) return Optional.empty();
                String sql = rows.getString(1), prefix = "CREATE VIEW " + quote(view) + " -- schemas: ";
                int end = sql.indexOf('\n', prefix.length());
                if (!sql.startsWith(prefix) || end < 0) throw new SQLException("Missing layout signature for view " + view);
                return Optional.of(sql.substring(prefix.length(), end));
            }
        }
    }

    static void view(Connection writer, long id, List<String> columns, String signature) throws SQLException {
        String view = viewName(writer, id);
        String projection = String.join(", ", METADATA);
        if (!columns.isEmpty()) projection += ", " + String.join(", ", columns);
        try (var s = writer.createStatement()) {
            s.execute("DROP VIEW IF EXISTS " + quote(view));
            s.execute(viewHeader(view, signature) + projection + " FROM " + quote(tableName(id)));
        }
    }
}
