package io.github.ifnesi.kafcacheql.cache;

import io.github.ifnesi.kafcacheql.config.Mode;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** The fixed record layout; schema evolution changes views, never these storage tables. */
public final class StorageDdl {
    private StorageDdl() {}

    static String tableName(long id) {
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
                      _value           TEXT,
                      _value_schema_id TEXT,
                      _headers         TEXT,
                      _decode_error    TEXT,
                      PRIMARY KEY (_partition, _offset)
                    ) WITHOUT ROWID
                    """);
            s.execute("CREATE INDEX " + quote(name + "_ts") + " ON " + table + "(_timestamp)");
            if (mode == Mode.COMPACTED) {
                s.execute("CREATE UNIQUE INDEX " + quote(name + "_key") + " ON " + table + "(_partition, _key_raw)");
            }
            // Before any decoding/schema work, the topic exposes just its metadata and JSON columns.
            s.execute("CREATE VIEW " + quote(viewName) + " AS SELECT "
                    + "_partition, _offset, _timestamp, _key, _value, _headers, "
                    + "_key_schema_id, _value_schema_id, _decode_error FROM " + table);
        }
    }
}
