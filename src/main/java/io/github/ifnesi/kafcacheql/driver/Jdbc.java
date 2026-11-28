package io.github.ifnesi.kafcacheql.driver;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;
import static io.github.ifnesi.kafcacheql.util.ControlCharacters.escape;

import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.cache.TypedViews;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.config.ConfigErrors;
import io.github.ifnesi.kafcacheql.sql.QueryPipeline;
import io.github.ifnesi.kafcacheql.sql.Query;
import java.nio.file.Path;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

/** JDBC wrappers: unsupported operations are explicit; schema-derived values are checked before conversion. */
final class Jdbc {
    private Jdbc() {}

    static Connection connection(CacheContext.Lease cache, String url) { return new ConnectionHandler(cache, url).proxy; }
    private static SQLFeatureNotSupportedException unsupported(String name) {
        return new SQLFeatureNotSupportedException(sentence("KafCacheQL does not support " + name), "0A000");
    }
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(DriverLoader.LOADER, new Class<?>[] {type}, handler));
    }
    private static Object call(Method method, Object target, Object[] args) throws SQLException {
        try { return method.invoke(target, args); }
        catch (IllegalAccessException e) { throw new SQLException(sentence("Calling JDBC " + method.getName() + " failed"), e); }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof SQLException sql) throw ConfigErrors.plain(sql);
            if (e.getCause() instanceof RuntimeException runtime) throw runtime;
            if (e.getCause() instanceof Error error) throw error;
            throw new SQLException(sentence("Calling JDBC " + method.getName() + " failed"), e.getCause());
        }
    }
    private abstract static class Handler implements InvocationHandler {
        @Override public final Object invoke(Object self, Method method, Object[] args) throws SQLException {
            if (method.getDeclaringClass() == Object.class) return switch (method.getName()) {
                case "equals" -> self == args[0];
                case "hashCode" -> System.identityHashCode(self);
                case "toString" -> "KafCacheQL " + self.getClass().getInterfaces()[0].getSimpleName();
                default -> throw new AssertionError(method);
            };
            if (method.getName().equals("isWrapperFor")) return ((Class<?>) args[0]).isInstance(self);
            if (method.getName().equals("unwrap")) {
                if (((Class<?>) args[0]).isInstance(self)) return self;
                throw new SQLException(sentence("Not a wrapper for " + ((Class<?>) args[0]).getName()));
            }
            try { return DriverLoader.call(() -> invoke(method, args == null ? new Object[0] : args)); }
            catch (SQLException failure) { throw ConfigErrors.plain(failure); }
        }
        abstract Object invoke(Method method, Object[] args) throws SQLException;
    }

    private static final class ConnectionHandler extends Handler {
        final CacheContext.Lease cache;
        final String url;
        final Connection proxy = proxy(Connection.class, this);
        final Set<StatementHandler> statements = ConcurrentHashMap.newKeySet();
        final Set<MetadataRows> metadataResults = ConcurrentHashMap.newKeySet();
        final MetadataHandler metadata = new MetadataHandler(this);
        volatile boolean closed;
        ConnectionHandler(CacheContext.Lease cache, String url) { this.cache = cache; this.url = url; }
        void check() throws SQLException { if (closed) throw new SQLException(sentence("Connection is closed"), "08003"); }
        @Override Object invoke(Method method, Object[] args) throws SQLException {
            String name = method.getName();
            if (name.equals("isClosed")) return closed;
            if (name.equals("close")) { close(); return null; }
            if (name.equals("isValid")) {
                int timeout = (int) args[0];
                if (timeout < 0) throw new SQLException(sentence("Negative validation timeout"));
                if (closed) return false;
                try (var execution = new Execution(Duration.ofSeconds(timeout))) { return cache.isValid(execution); }
                catch (SQLException e) { return false; }
            }
            check();
            return switch (name) {
                case "createStatement", "prepareStatement" -> {
                    boolean prepared = name.equals("prepareStatement");
                    if (prepared && args[0] == null) throw new SQLException(sentence("SQL is null"), "42000");
                    int offset = prepared ? 1 : 0;
                    if (args.length > offset) {
                        if (args.length < offset + 2 || (int) args[offset] != ResultSet.TYPE_FORWARD_ONLY
                                || (int) args[offset + 1] != ResultSet.CONCUR_READ_ONLY
                                || (args.length > offset + 2 && (int) args[offset + 2] != ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
                            throw unsupported("these statement options");
                        }
                    }
                    synchronized (this) {
                        check();
                        var statement = new StatementHandler(this, prepared ? (String) args[0] : null);
                        statements.add(statement);
                        yield statement.proxy;
                    }
                }
                case "isReadOnly", "getAutoCommit" -> true;
                case "setReadOnly", "setAutoCommit", "commit", "rollback", "clearWarnings" -> {
                    if (args.length != 0 && name.equals("rollback")) throw unsupported("savepoints");
                    yield null;
                }
                case "getWarnings" -> null;
                case "getMetaData" -> metadata.proxy;
                case "getCatalog" -> "main";
                case "getTransactionIsolation" -> Connection.TRANSACTION_NONE;
                case "setTransactionIsolation" -> {
                    if ((int) args[0] != Connection.TRANSACTION_NONE) throw unsupported("transactions"); yield null;
                }
                case "getHoldability" -> ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "setHoldability" -> { if ((int) args[0] != ResultSet.CLOSE_CURSORS_AT_COMMIT) throw unsupported(name); yield null; }
                case "getSchema" -> "main";
                case "setSchema", "setCatalog" -> {
                    if (args[0] != null && !args[0].equals("main")) throw unsupported(name); yield null;
                }
                case "nativeSQL" -> args[0];
                case "getClientInfo" -> {
                    var info = new java.util.Properties();
                    info.setProperty("cache.path", cache.path().toString());
                    info.setProperty("bootstrap.servers", cache.config().bootstrapServers().orElse(""));
                    info.setProperty("offline", Boolean.toString(cache.config().offline()));
                    yield args.length == 0 ? info : info.getProperty((String) args[0]);
                }
                case "setClientInfo" -> throw new SQLClientInfoException(sentence("Client info is read-only."), null, Map.of());
                case "getTypeMap" -> new HashMap<String, Class<?>>();
                case "setTypeMap" -> { if (!((Map<?, ?>) args[0]).isEmpty()) throw unsupported(name); yield null; }
                case "getNetworkTimeout" -> 0;
                default -> throw unsupported(name);
            };
        }
        void close() throws SQLException {
            synchronized (this) { if (closed) return; closed = true; }
            SQLException failure = null;
            for (var statement : new ArrayList<>(statements)) {
                try { statement.close(); } catch (SQLException e) { failure = append(failure, e); }
            }
            for (var rows : new ArrayList<>(metadataResults)) {
                try { rows.close(); } catch (SQLException e) { failure = append(failure, e); }
            }
            try { cache.close(); } catch (SQLException e) { failure = append(failure, e); }
            if (failure != null) throw failure;
        }
    }

    /** SQLite supplies JDBC layouts and SQL capabilities; topics supply the logical tables. */
    private static final class MetadataHandler extends Handler {
        private static final String ABSENT = "$metadata$template$";
        final ConnectionHandler connection;
        final DatabaseMetaData proxy = proxy(DatabaseMetaData.class, this);
        private Set<String> topics = Set.of();
        private long refreshSequence;
        private long publishedSequence;
        MetadataHandler(ConnectionHandler connection) { this.connection = connection; }

        @Override Object invoke(Method method, Object[] args) throws SQLException {
            connection.check();
            String name = method.getName();
            switch (name) {
                case "getConnection": return connection.proxy;
                case "getURL": return connection.url;
                case "isReadOnly": return true;
                case "supportsTransactions", "supportsSavepoints", "supportsBatchUpdates", "supportsGetGeneratedKeys",
                        "supportsMultipleTransactions", "supportsDataDefinitionAndDataManipulationTransactions",
                        "supportsDataManipulationTransactionsOnly", "dataDefinitionCausesTransactionCommit",
                        "dataDefinitionIgnoredInTransactions", "supportsStoredFunctionsUsingCallSyntax",
                        "autoCommitFailureClosesAllResultSets", "generatedKeyAlwaysReturned": return false;
                case "getRowIdLifetime": return RowIdLifetime.ROWID_UNSUPPORTED;
                case "getDefaultTransactionIsolation": return Connection.TRANSACTION_NONE;
                case "supportsTransactionIsolationLevel": return (int) args[0] == Connection.TRANSACTION_NONE;
                case "supportsResultSetType": return (int) args[0] == ResultSet.TYPE_FORWARD_ONLY;
                case "supportsResultSetConcurrency": return (int) args[0] == ResultSet.TYPE_FORWARD_ONLY && (int) args[1] == ResultSet.CONCUR_READ_ONLY;
                case "getResultSetHoldability": return ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "supportsResultSetHoldability": return (int) args[0] == ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "getIdentifierQuoteString", "getSearchStringEscape": return name.equals("getIdentifierQuoteString") ? "\"" : "\\";
                case "getDriverName", "getDatabaseProductName": return "KafCacheQL";
                case "getDriverMajorVersion", "getDatabaseMajorVersion": return KafCacheQLDriver.MAJOR_VERSION;
                case "getDriverMinorVersion", "getDatabaseMinorVersion": return KafCacheQLDriver.MINOR_VERSION;
                case "getDriverVersion", "getDatabaseProductVersion": return KafCacheQLDriver.VERSION;
                default: break;
            }
            Set<String> listed = name.equals("getTables") ? refreshTopics(null) : topicSnapshot();
            Map<String, String> keys = name.equals("getPrimaryKeys") ? primaryKeys((String) args[2]) : Map.of();
            Object result = connection.cache.read(reader -> {
                DatabaseMetaData sqlite = reader.getMetaData();
                try (var match = reader.prepareStatement("SELECT ? LIKE ? ESCAPE '\\'")) {
                    return switch (name) {
                        case "getCatalogs" -> {
                            var rows = copy(sqlite.getCatalogs()); add(rows, Map.of("TABLE_CAT", "main")); yield rows;
                        }
                        case "getSchemas" -> {
                            var rows = empty(new Column("TABLE_SCHEM", JDBCType.VARCHAR), new Column("TABLE_CATALOG", JDBCType.VARCHAR));
                            if (args.length == 0 || namespace(match, args[0], args[1])) add(rows, Map.of("TABLE_SCHEM", "main", "TABLE_CATALOG", "main"));
                            yield rows;
                        }
                        case "getTableTypes" -> {
                            try (var s = reader.createStatement()) { yield copy(s.executeQuery("SELECT 'TABLE' AS TABLE_TYPE")); }
                        }
                        case "getTables" -> tables(reader, sqlite, match, args, listed, List.of(), null);
                        case "getColumns" -> columns(reader, sqlite, match, args, listed);
                        case "getPrimaryKeys" -> {
                            CachedRowSet rows;
                            try (var template = copy(sqlite.getPrimaryKeys(null, null, "$meta"))) { rows = template.createCopySchema(); }
                            String table = (String) args[2];
                            if (namespace(match, args[0], args[1]) && !keys.isEmpty()) {
                                for (var key : keys.entrySet()) {
                                    add(rows, Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", key.getKey(),
                                            "COLUMN_NAME", key.getValue().equals("compacted") ? "_key" : "_offset", "KEY_SEQ", 2));
                                    add(rows, Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", key.getKey(),
                                            "COLUMN_NAME", "_partition", "KEY_SEQ", 1));
                                }
                            } else if (namespace(match, args[0], args[1]) && table != null && table.startsWith("$") && connection.cache.config().metadataShowInternal()) {
                                rows.close(); rows = copy(sqlite.getPrimaryKeys(null, null, table)); normalize(rows);
                            }
                            yield rows;
                        }
                        case "getIndexInfo" -> copy(sqlite.getIndexInfo(null, null, ABSENT, false, false));
                        case "getImportedKeys", "getExportedKeys", "getCrossReference" -> copy(sqlite.getImportedKeys(null, null, ABSENT));
                        // sqlite-jdbc 3.54.1.0 leaves these unimplemented. Layouts are the JDK
                        // DatabaseMetaData contract; no functions, pseudo columns or client info are advertised.
                        case "getFunctions" -> empty(
                                new Column("FUNCTION_CAT", JDBCType.VARCHAR), new Column("FUNCTION_SCHEM", JDBCType.VARCHAR),
                                new Column("FUNCTION_NAME", JDBCType.VARCHAR), new Column("REMARKS", JDBCType.VARCHAR),
                                new Column("FUNCTION_TYPE", JDBCType.SMALLINT), new Column("SPECIFIC_NAME", JDBCType.VARCHAR));
                        case "getFunctionColumns" -> empty(
                                new Column("FUNCTION_CAT", JDBCType.VARCHAR), new Column("FUNCTION_SCHEM", JDBCType.VARCHAR),
                                new Column("FUNCTION_NAME", JDBCType.VARCHAR), new Column("COLUMN_NAME", JDBCType.VARCHAR),
                                new Column("COLUMN_TYPE", JDBCType.SMALLINT), new Column("DATA_TYPE", JDBCType.INTEGER),
                                new Column("TYPE_NAME", JDBCType.VARCHAR), new Column("PRECISION", JDBCType.INTEGER),
                                new Column("LENGTH", JDBCType.INTEGER), new Column("SCALE", JDBCType.SMALLINT),
                                new Column("RADIX", JDBCType.SMALLINT), new Column("NULLABLE", JDBCType.SMALLINT),
                                new Column("REMARKS", JDBCType.VARCHAR), new Column("CHAR_OCTET_LENGTH", JDBCType.INTEGER),
                                new Column("ORDINAL_POSITION", JDBCType.INTEGER), new Column("IS_NULLABLE", JDBCType.VARCHAR),
                                new Column("SPECIFIC_NAME", JDBCType.VARCHAR));
                        case "getPseudoColumns" -> empty(
                                new Column("TABLE_CAT", JDBCType.VARCHAR), new Column("TABLE_SCHEM", JDBCType.VARCHAR),
                                new Column("TABLE_NAME", JDBCType.VARCHAR), new Column("COLUMN_NAME", JDBCType.VARCHAR),
                                new Column("DATA_TYPE", JDBCType.INTEGER), new Column("COLUMN_SIZE", JDBCType.INTEGER),
                                new Column("DECIMAL_DIGITS", JDBCType.INTEGER), new Column("NUM_PREC_RADIX", JDBCType.INTEGER),
                                new Column("COLUMN_USAGE", JDBCType.VARCHAR), new Column("REMARKS", JDBCType.VARCHAR),
                                new Column("CHAR_OCTET_LENGTH", JDBCType.INTEGER), new Column("IS_NULLABLE", JDBCType.VARCHAR));
                        case "getClientInfoProperties" -> empty(
                                new Column("NAME", JDBCType.VARCHAR), new Column("MAX_LEN", JDBCType.INTEGER),
                                new Column("DEFAULT_VALUE", JDBCType.VARCHAR), new Column("DESCRIPTION", JDBCType.VARCHAR));
                        default -> {
                            Object answer = call(method, sqlite, args);
                            yield answer instanceof ResultSet rows ? copy(rows) : answer;
                        }
                    };
                }
            });
            if (!(result instanceof CachedRowSet rows)) return result;
            rows.beforeFirst();
            synchronized (connection) {
                if (connection.closed) { rows.close(); connection.check(); }
                var handler = new MetadataRows(connection, rows);
                connection.metadataResults.add(handler);
                return handler.proxy;
            }
        }

        private record Table(String topic, String view, long generation, String mode, int partitions) {}
        private record Key(String topic, String view, String mode) {}
        private record Column(String name, JDBCType type) {}

        private static CachedRowSet empty(Column... columns) throws SQLException {
            var metadata = new RowSetMetaDataImpl();
            metadata.setColumnCount(columns.length);
            for (int i = 1; i <= columns.length; i++) {
                Column column = columns[i - 1];
                metadata.setColumnName(i, column.name());
                metadata.setColumnLabel(i, column.name());
                metadata.setColumnType(i, column.type().getVendorTypeNumber());
                metadata.setColumnTypeName(i, column.type().getName());
                metadata.setNullable(i, ResultSetMetaData.columnNullableUnknown);
            }
            var rows = RowSetProvider.newFactory().createCachedRowSet();
            rows.setMetaData(metadata);
            return rows;
        }

        private Map<String, String> primaryKeys(String table) throws SQLException {
            boolean pattern = table == null || table.contains("%") || table.contains("_");
            if (table != null && table.startsWith("$")
                    && (!pattern || connection.cache.read(r -> internalNames(r).contains(table)))) return Map.of();
            var snapshot = topicSnapshot();
            String exact = table == null ? null : connection.cache.read(r -> knownViews(r, cached(r), snapshot).entrySet().stream()
                    .filter(e -> e.getValue().equalsIgnoreCase(table)).map(Map.Entry::getKey).findFirst().orElse(null));
            boolean all = exact == null && pattern;
            var topicNames = all ? refreshTopics(null) : snapshot;
            if (all) {
                // A real name containing '_' still names one topic, rather than being a navigator pattern.
                exact = table == null ? null : connection.cache.read(r -> knownViews(r, cached(r), topicNames).entrySet().stream()
                        .filter(e -> e.getValue().equalsIgnoreCase(table)).map(Map.Entry::getKey).findFirst().orElse(null));
                if (exact != null) all = false;
            }
            boolean listed = all;
            String named = exact == null ? table : exact;
            var selected = connection.cache.read(r -> {
                var stored = cached(r);
                var views = knownViews(r, stored, topicNames);
                var result = new ArrayList<Key>();
                for (String topic : listed ? topicNames : Set.of(named)) {
                    if (!visible(topic)) continue;
                    Table cached = stored.get(topic);
                    result.add(new Key(topic, listed ? views.get(topic) : table, cached == null ? null : cached.mode()));
                }
                return result;
            });
            var modes = new HashMap<String, String>();
            var missing = new java.util.HashSet<String>();
            for (var key : selected) {
                if (key.mode() == null) missing.add(key.topic()); else modes.put(key.topic(), key.mode());
            }
            if (listed) {
                connection.cache.topicModes(missing).forEach((topic, mode) -> modes.put(topic, mode.value()));
            } else if (!missing.isEmpty()) {
                connection.cache.describeTopic(named).ifPresent(info -> modes.put(named, info.mode().value()));
            }
            var result = new TreeMap<String, String>();
            for (var key : selected) {
                String mode = modes.get(key.topic());
                if (mode != null) result.put(key.view(), mode);
            }
            return result;
        }
        private Set<String> refreshTopics(Execution execution) throws SQLException {
            if (execution != null) execution.check();
            long sequence;
            synchronized (this) { sequence = ++refreshSequence; }
            // No metadata monitor is held across Kafka. The cache captures generations before
            // listing and queues guarded background drops after a successful reply, as for Refresh.
            var listed = Set.copyOf(connection.cache.listTopics(execution));
            synchronized (this) {
                if (sequence > publishedSequence) {
                    topics = listed;
                    publishedSequence = sequence;
                }
            }
            // This call answers from its own Kafka snapshot; later getColumns calls use the newest published list.
            return listed;
        }

        private synchronized Set<String> topicSnapshot() { return topics; }

        CachedRowSet showTables(List<String> patterns, Execution execution) throws SQLException {
            var listed = refreshTopics(execution);
            return connection.cache.read(execution, reader -> {
                var result = empty(new Column("table_name", JDBCType.VARCHAR), new Column("mode", JDBCType.VARCHAR),
                        new Column("partitions", JDBCType.INTEGER), new Column("cached_rows", JDBCType.BIGINT), new Column("cached_bytes", JDBCType.BIGINT));
                var stored = cached(reader);
                try (var match = reader.prepareStatement("SELECT ? LIKE ? ESCAPE '\\'");
                     var tables = tables(reader, reader.getMetaData(), match, new Object[]{null, null, null, null}, listed, patterns, execution)) {
                    var byView = new HashMap<String, Table>(); stored.values().forEach(t -> byView.put(t.view(), t));
                    tables.beforeFirst();
                    while (tables.next()) {
                        execution.check(); String name = tables.getString("TABLE_NAME");
                        if (name.startsWith("$")) continue;
                        var values = new HashMap<String, Object>(); values.put("table_name", name);
                        Table table = byView.get(name);
                        if (table != null) {
                            values.put("mode", table.mode()); values.put("partitions", table.partitions());
                            try (var s = reader.createStatement(); var count = s.executeQuery("SELECT count(*) FROM " + StorageDdl.quote(StorageDdl.tableName(table.generation())))) {
                                count.next(); values.put("cached_rows", count.getLong(1));
                            }
                            try (var p = reader.prepareStatement("SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name IN (SELECT name FROM sqlite_schema WHERE tbl_name=?)")) {
                                p.setString(1, StorageDdl.tableName(table.generation()));
                                try (var size = p.executeQuery()) { size.next(); values.put("cached_bytes", size.getLong(1)); }
                            }
                        }
                        add(result, values);
                    }
                }
                result.beforeFirst(); return result;
            });
        }

        CachedRowSet describe(String name, Execution execution) throws SQLException {
            var snapshot = topicSnapshot();
            boolean known = connection.cache.config().offline() || connection.cache.read(execution, reader ->
                    knownViews(reader, cached(reader), snapshot).values().stream().anyMatch(view -> view.equalsIgnoreCase(name))
                    || connection.cache.config().metadataShowInternal() && internalNames(reader).contains(name));
            var listed = known ? snapshot : refreshTopics(execution);
            return connection.cache.read(execution, reader -> {
                var result = empty(new Column("column_name", JDBCType.VARCHAR), new Column("type", JDBCType.VARCHAR));
                String exact = name.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
                try (var match = reader.prepareStatement("SELECT ? LIKE ? ESCAPE '\\'");
                     var columns = columns(reader, reader.getMetaData(), match, new Object[]{null, null, exact, null}, listed)) {
                    columns.beforeFirst();
                    while (columns.next()) { execution.check(); add(result, Map.of("column_name", columns.getString("COLUMN_NAME"), "type", columns.getString("TYPE_NAME"))); }
                }
                if (result.size() == 0) { result.close(); throw new SQLException(sentence("Unknown table " + name), "42S02"); }
                result.beforeFirst(); return result;
            });
        }

        /** Resolve all patterns against the same filtered list before any sync is allowed to fetch. */
        Map<String, String> syncTopics(List<String> patterns, Execution execution) throws SQLException {
            var listed = refreshTopics(execution);
            return connection.cache.read(execution, reader -> {
                var names = new TreeMap<String, String>();
                var views = TopicRegistry.viewNames(reader, listed);
                var stored = cached(reader);
                for (var entry : views.entrySet()) if (visible(entry.getKey()) && (patterns.size() > 0 || stored.containsKey(entry.getKey())))
                    names.put(entry.getValue(), entry.getKey());
                return matchingTopics(reader, names, patterns, execution, true);
            });
        }

        /** SHOW and SYNC share SQLite's case-sensitive GLOB; only SYNC requires every pattern to match. */
        private static Map<String, String> matchingTopics(Connection reader, Map<String, String> names,
                List<String> patterns, Execution execution, boolean requireMatch) throws SQLException {
            if (patterns.isEmpty()) return names;
            var selected = new TreeMap<String, String>();
            try (var match = reader.prepareStatement("SELECT ? GLOB ?")) {
                for (String pattern : patterns) {
                    boolean found = false;
                    for (var entry : names.entrySet()) {
                        if (execution != null) execution.check();
                        if (matches(match, entry.getKey(), pattern)) {
                            selected.put(entry.getKey(), entry.getValue()); found = true;
                        }
                    }
                    if (requireMatch && !found) throw new SQLException(sentence("SYNC pattern matches no topic: " + pattern), "42S02");
                }
            }
            return selected;
        }

        private static Map<String, String> knownViews(Connection reader, Map<String, Table> stored, Set<String> listed) throws SQLException {
            var known = new java.util.HashSet<>(listed);
            known.addAll(stored.keySet());
            return TopicRegistry.viewNames(reader, known);
        }
        private static Map<String, Table> cached(Connection reader) throws SQLException {
            var result = new HashMap<String, Table>();
            try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT topic, view_name, id, mode, partition_count FROM " + StorageDdl.quote("$topics") + " WHERE deleted_ms IS NULL")) {
                while (rows.next()) result.put(rows.getString(1), new Table(rows.getString(1), rows.getString(2), rows.getLong(3), rows.getString(4), rows.getInt(5)));
            }
            return result;
        }
        private boolean visible(String topic) {
            var config = connection.cache.config();
            return config.topicsInclude().matcher(topic).matches() && !config.topicsExclude().matcher(topic).matches();
        }
        private static boolean matches(PreparedStatement match, String value, Object pattern) throws SQLException {
            if (pattern == null) return true;
            match.setString(1, value); match.setString(2, (String) pattern);
            try (var row = match.executeQuery()) { return row.next() && row.getBoolean(1); }
        }
        private static boolean namespace(PreparedStatement match, Object catalog, Object schema) throws SQLException {
            return (catalog == null || catalog.equals("main")) && matches(match, "main", schema);
        }
        private static List<String> internalNames(Connection reader) throws SQLException {
            var names = new ArrayList<String>();
            try (var s = reader.createStatement(); var rows = s.executeQuery("SELECT name FROM sqlite_schema WHERE type='table' AND substr(name,1,1)='$'")) {
                while (rows.next()) names.add(rows.getString(1));
            }
            return names;
        }
        private CachedRowSet tables(Connection reader, DatabaseMetaData sqlite, PreparedStatement match, Object[] args,
                                    Set<String> listed, List<String> patterns, Execution execution) throws SQLException {
            var rows = copy(sqlite.getTables(null, null, ABSENT, null));
            if (!namespace(match, args[0], args[1]) || args[3] != null && !List.of((String[]) args[3]).contains("TABLE")) return rows;
            var stored = cached(reader);
            var names = new TreeMap<String, String>();
            var views = TopicRegistry.viewNames(reader, listed);
            for (String topic : listed) {
                if (!visible(topic)) continue;
                Table table = stored.get(topic);
                names.put(views.get(topic),
                        table == null ? "" : table.mode() + ", " + table.partitions() + " partitions");
            }
            if (connection.cache.config().metadataShowInternal()) {
                for (String name : internalNames(reader)) names.put(name, "");
            }
            for (var entry : matchingTopics(reader, names, patterns, execution, false).entrySet()) if (matches(match, entry.getKey(), args[2])) add(rows,
                    Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", entry.getKey(), "TABLE_TYPE", "TABLE", "REMARKS", entry.getValue()));
            return rows;
        }

        private CachedRowSet columns(Connection reader, DatabaseMetaData sqlite, PreparedStatement match, Object[] args, Set<String> listed) throws SQLException {
            var result = copy(sqlite.getColumns(null, null, ABSENT, null));
            if (!namespace(match, args[0], args[1])) return result;
            var tables = new TreeMap<String, Table>(String.CASE_INSENSITIVE_ORDER);
            var stored = cached(reader);
            var views = knownViews(reader, stored, listed);
            for (String topic : views.keySet()) {
                if (!visible(topic)) continue;
                Table table = stored.get(topic);
                tables.put(views.get(topic), table);
            }
            if (connection.cache.config().metadataShowInternal()) {
                for (String name : internalNames(reader)) tables.put(name, null);
            }
            for (var entry : tables.entrySet()) {
                String name = entry.getKey();
                if (!matches(match, name, args[2])) continue;
                Table table = entry.getValue();
                var types = new HashMap<String, JDBCType>();
                var layout = table == null ? TypedViews.columns(new TypedViews.Layout(List.of())) : TypedViews.columns(reader, table.generation());
                for (var column : layout) types.put(column.name(), column.type());
                if (table != null || name.startsWith("$")) {
                    try (var columns = sqlite.getColumns(null, null, name, null)) {
                        var metadata = columns.getMetaData();
                        while (columns.next()) {
                            String column = columns.getString("COLUMN_NAME");
                            if (!name.equals(columns.getString("TABLE_NAME")) || !matches(match, column, args[3])) continue;
                            var values = new HashMap<String, Object>();
                            for (int i = 1; i <= metadata.getColumnCount(); i++) values.put(metadata.getColumnName(i), columns.getObject(i));
                            values.put("TABLE_CAT", "main"); values.put("TABLE_SCHEM", "main");
                            if (table != null) { values.put("DATA_TYPE", types.get(column).getVendorTypeNumber()); values.put("TYPE_NAME", types.get(column).getName()); }
                            add(result, values);
                        }
                    }
                } else {
                    int ordinal = 0;
                    for (var column : layout) {
                        ordinal++;
                        if (matches(match, column.name(), args[3])) add(result, Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", name,
                                "COLUMN_NAME", column.name(), "DATA_TYPE", column.type().getVendorTypeNumber(), "TYPE_NAME", column.type().getName(),
                                "NULLABLE", DatabaseMetaData.columnNullableUnknown, "ORDINAL_POSITION", ordinal, "IS_NULLABLE", "", "IS_AUTOINCREMENT", "NO"));
                    }
                }
            }
            return result;
        }
        private static CachedRowSet copy(ResultSet source) throws SQLException {
            try (source) { var rows = RowSetProvider.newFactory().createCachedRowSet(); rows.populate(source); return rows; }
        }
        private static void add(CachedRowSet rows, Map<String, ?> values) throws SQLException {
            rows.afterLast(); rows.moveToInsertRow();
            for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) rows.updateObject(i, values.get(rows.getMetaData().getColumnName(i)));
            rows.insertRow(); rows.moveToCurrentRow();
        }
        private static void normalize(CachedRowSet rows) throws SQLException {
            while (rows.next()) { rows.updateString("TABLE_CAT", "main"); rows.updateString("TABLE_SCHEM", "main"); rows.updateRow(); }
        }
    }

    /** Disconnected metadata rows release their SQLite reader before reaching the caller. */
    private static final class MetadataRows extends Handler {
        final ConnectionHandler connection;
        final CachedRowSet rows;
        final ResultSet proxy = proxy(ResultSet.class, this);
        boolean closed;
        MetadataRows(ConnectionHandler connection, CachedRowSet rows) { this.connection = connection; this.rows = rows; }
        @Override synchronized Object invoke(Method method, Object[] args) throws SQLException {
            String name = method.getName();
            if (name.equals("close")) { close(); return null; }
            if (name.equals("isClosed")) return closed;
            connection.check();
            if (closed) throw new SQLException(sentence("Result set is closed"), "24000");
            checkResultOperation(name);
            return switch (name) {
                case "getStatement" -> null;
                case "getType" -> ResultSet.TYPE_FORWARD_ONLY;
                case "getConcurrency" -> ResultSet.CONCUR_READ_ONLY;
                case "getHoldability" -> ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "getMetaData" -> readOnlyMetadata(rows.getMetaData());
                default -> call(method, rows, args);
            };
        }
        synchronized void close() throws SQLException {
            if (closed) return;
            closed = true;
            try { rows.close(); } finally { connection.metadataResults.remove(this); }
        }
    }

    private record Binding(Method method, Object[] args) {
        Object value() { return method.getName().equals("setNull") ? null : args[1]; }
    }
    private static final class StatementHandler extends Handler {
        final ConnectionHandler connection;
        final String sql;
        final Statement proxy;
        final ReentrantLock use = new ReentrantLock();
        final Map<Integer, Binding> bindings = new HashMap<>();
        volatile Execution execution;
        volatile boolean closed;
        ResultHandler result;
        SQLWarning warnings;
        int fetchSize, timeout;
        int updateCount = -1;
        long maxRows;
        boolean closeOnCompletion;
        StatementHandler(ConnectionHandler connection, String sql) {
            this.connection = connection; this.sql = sql;
            proxy = sql == null ? proxy(Statement.class, this) : proxy(PreparedStatement.class, this);
        }
        void check() throws SQLException {
            connection.check();
            if (closed) throw new SQLException(sentence("Statement is closed"), "07000");
        }
        @Override Object invoke(Method method, Object[] args) throws SQLException {
            String name = method.getName();
            if (name.equals("isClosed")) return closed;
            if (name.equals("close")) { close(); return null; }
            if (name.equals("cancel")) { check(); var active = execution; if (active != null) active.cancel(); return null; }
            use.lock();
            try {
                check();
                if (method.getDeclaringClass() == PreparedStatement.class && name.startsWith("set")
                        && args.length >= 2 && args[0] instanceof Integer index) {
                    if (index < 1) throw new SQLException(sentence("Parameter index must be positive"), "07009");
                    bindings.put(index, new Binding(method, args.clone())); return null;
                }
                return switch (name) {
                    case "executeQuery", "execute", "executeUpdate", "executeLargeUpdate" -> {
                        if (sql != null && args.length != 0) throw new SQLException(sentence("A prepared statement uses its prepared SQL"));
                        if (sql == null && args.length != 1) throw unsupported("generated keys");
                        String text = sql == null ? (String) args[0] : sql;
                        execute(text, name);
                        if (name.equals("executeUpdate")) yield updateCount;
                        if (name.equals("executeLargeUpdate")) yield (long) updateCount;
                        yield name.equals("execute") ? result != null : result.proxy;
                    }
                    case "getResultSet" -> result == null ? null : result.proxy;
                    case "getConnection" -> connection.proxy;
                    case "addBatch", "executeBatch", "executeLargeBatch" -> {
                        warnings = null; throw unsupported("updates and batches");
                    }
                    case "clearBatch", "setEscapeProcessing" -> null;
                    case "clearParameters" -> { bindings.clear(); yield null; }
                    case "getWarnings" -> warnings;
                    case "clearWarnings" -> { warnings = null; yield null; }
                    case "getUpdateCount" -> updateCount;
                    case "getLargeUpdateCount" -> (long) updateCount;
                    case "getMoreResults" -> {
                        if (args.length > 0 && (int) args[0] == Statement.KEEP_CURRENT_RESULT) throw unsupported("multiple results");
                        if (result != null) result.close(); result = null; updateCount = -1; yield false;
                    }
                    case "setFetchSize" -> { fetchSize = nonnegative((int) args[0], name); yield null; }
                    case "getFetchSize" -> fetchSize;
                    case "setQueryTimeout" -> { timeout = nonnegative((int) args[0], name); yield null; }
                    case "getQueryTimeout" -> timeout;
                    case "setMaxRows", "setLargeMaxRows" -> {
                        long value = ((Number) args[0]).longValue();
                        if (value < 0) throw new SQLException(sentence("Negative maxRows")); maxRows = value; yield null;
                    }
                    case "getMaxRows" -> {
                        if (maxRows > Integer.MAX_VALUE) throw new SQLException(sentence("Use getLargeMaxRows for this limit"), "22003");
                        yield (int) maxRows;
                    }
                    case "getLargeMaxRows" -> maxRows;
                    case "getResultSetType" -> ResultSet.TYPE_FORWARD_ONLY;
                    case "getResultSetConcurrency" -> ResultSet.CONCUR_READ_ONLY;
                    case "getResultSetHoldability" -> ResultSet.CLOSE_CURSORS_AT_COMMIT;
                    case "getFetchDirection" -> ResultSet.FETCH_FORWARD;
                    case "setFetchDirection" -> { if ((int) args[0] != ResultSet.FETCH_FORWARD) throw unsupported(name); yield null; }
                    case "closeOnCompletion" -> { closeOnCompletion = true; yield null; }
                    case "isCloseOnCompletion", "isPoolable" -> name.equals("isCloseOnCompletion") && closeOnCompletion;
                    case "setPoolable" -> null;
                    case "getMaxFieldSize" -> 0;
                    case "setMaxFieldSize" -> { if ((int) args[0] != 0) throw unsupported(name); yield null; }
                    default -> throw unsupported(name);
                };
            } finally { use.unlock(); }
        }
        void execute(String text, String operation) throws SQLException {
            warnings = null; updateCount = -1;
            if (result != null) { result.close(); result = null; }
            var active = new Execution(Duration.ofSeconds(timeout));
            synchronized (this) {
                try { check(); execution = active; }
                catch (SQLException e) { active.close(); throw e; }
            }
            var values = new HashMap<Integer, Object>();
            bindings.forEach((index, binding) -> values.put(index, binding.value()));
            QueryPipeline.Cursor cursor = null;
            try {
                Query query = active.run(connection.cache.config().offline(), () -> Query.parseAndResolve(text, name -> {
                    if (operation.equals("executeUpdate") || operation.equals("executeLargeUpdate")) throw unsupported("updates other than DROP TABLE");
                    return connection.cache.resolveTable(name, active);
                }));
                boolean update = operation.equals("executeUpdate") || operation.equals("executeLargeUpdate");
                if (update && (query.command() == null || query.command().kind() != Query.Kind.DROP)) throw unsupported("updates other than DROP TABLE");
                if (query.command() != null) {
                    if (!bindings.isEmpty()) throw new SQLException(sentence("Cache statements have no parameters"), "07009");
                    var command = query.command();
                    if (command.kind() == Query.Kind.DROP) {
                        if (operation.equals("executeQuery")) throw new SQLException(sentence("DROP TABLE returns an update count"), "42000");
                        boolean dropped = active.run(connection.cache.config().offline(), () -> connection.cache.dropTable(command.table(), active));
                        if (!dropped && !command.ifExists()) throw new SQLException(sentence("Unknown cached table " + command.table()), "42S02");
                        warnings = new SQLWarning(sentence(escape("Topic " + command.table() + " dropped from the local cache; Kafka is unchanged")), "01000");
                        updateCount = 0; finish(active); return;
                    }
                    cursor = switch (command.kind()) {
                        case SHOW -> QueryPipeline.Cursor.detached(connection.metadata.showTables(command.patterns(), active), active, List.of());
                        case DESCRIBE -> QueryPipeline.Cursor.detached(connection.metadata.describe(command.table(), active), active, List.of());
                        case SYNC -> sync(command, active);
                        case DROP -> throw new AssertionError();
                    };
                } else cursor = QueryPipeline.open(connection.cache, query, values, active, fetchSize, maxRows, new QueryPipeline.Bindings() {
                    @Override public void validate(Set<Integer> parameters) throws SQLException {
                        for (int index : bindings.keySet()) {
                            if (!parameters.contains(index)) throw new SQLException(sentence("Unknown parameter " + index), "07009");
                        }
                    }
                    @Override public void bind(PreparedStatement statement) throws SQLException {
                        int count = statement.getParameterMetaData().getParameterCount();
                        for (var entry : bindings.entrySet()) {
                            if (entry.getKey() <= count) call(entry.getValue().method(), statement, entry.getValue().args());
                        }
                    }
                });
                result = new ResultHandler(this, cursor, active);
                SQLWarning tail = null;
                for (var warning : cursor.warnings()) {
                    var copy = new SQLWarning(sentence(warning.getMessage()), warning.getSQLState(), warning.getErrorCode(), warning.getCause());
                    if (tail == null) warnings = copy; else tail.setNextWarning(copy);
                    tail = copy;
                }
            } catch (SQLException | RuntimeException | Error e) {
                if (cursor != null) try { cursor.close(); } catch (SQLException close) { e.addSuppressed(close); }
                finish(active);
                throw e;
            }
        }
        QueryPipeline.Cursor sync(Query.Command command, Execution active) throws SQLException {
            if (connection.cache.config().offline()) throw new SQLException(sentence("SYNC needs Kafka and cannot run offline"), "08006");
            Path lockPath = Path.of(connection.cache.path() + ".sync.lock");
            connection.cache.beginSync();
            try (var channel = java.nio.channels.FileChannel.open(lockPath, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE)) {
                java.nio.channels.FileLock locked;
                try { locked = channel.tryLock(); }
                catch (java.nio.channels.OverlappingFileLockException e) { throw syncLocked(lockPath); }
                if (locked == null) throw syncLocked(lockPath);
                try (locked) {
                    var selected = connection.metadata.syncTopics(command.patterns(), active);
                    var counts = new TreeMap<String, Long>();
                    selected.keySet().forEach(name -> counts.put(name, 0L));
                    return active.run(false, () -> {
                        var plans = new java.util.LinkedHashMap<String, io.github.ifnesi.kafcacheql.sync.SyncPlan>();
                        // Capture every topic's end before fetching the first one.
                        for (var entry : selected.entrySet()) plans.put(entry.getKey(), connection.cache.freshPlan(entry.getValue(), active));
                        var generations = new TreeMap<String, Long>();
                        var checked = new ArrayList<Map.Entry<io.github.ifnesi.kafcacheql.sync.SyncPlan, Long>>();
                        var warnings = new ArrayList<SQLWarning>();
                        for (var entry : plans.entrySet()) {
                            var plan = io.github.ifnesi.kafcacheql.sql.WindowPlanner.refresh(connection.cache, entry.getValue(), active);
                            long generation = connection.cache.sync(plan, active, io.github.ifnesi.kafcacheql.cache.RecordStore::write,
                                    n -> counts.merge(entry.getKey(), n, Long::sum));
                            generations.put(plan.topic().name(), generation);
                            checked.add(Map.entry(plan, generation)); warnings.addAll(plan.warnings());
                        }
                        try (var reader = connection.cache.borrowReader(active)) {
                            for (var entry : checked) connection.cache.validate(reader.connection(), entry.getKey(), entry.getValue());
                            connection.cache.queried(generations, System.currentTimeMillis());
                        }
                        var rows = MetadataHandler.empty(new MetadataHandler.Column("table_name", JDBCType.VARCHAR),
                                new MetadataHandler.Column("fetched_records", JDBCType.BIGINT));
                        for (var count : counts.entrySet()) MetadataHandler.add(rows, Map.of("table_name", count.getKey(), "fetched_records", count.getValue()));
                        rows.beforeFirst(); return QueryPipeline.Cursor.detached(rows, active, warnings);
                    });
                }
            } catch (java.io.IOException e) { throw new SQLException(sentence("Opening SYNC lock for " + connection.cache.path()), e); }
            finally { connection.cache.endSync(); }
        }
        private static SQLException syncLocked(Path path) { return new SQLException(sentence("another SYNC is running on cache " + path), "55006"); }

        void finish(Execution active) {
            active.close();
            synchronized (this) { if (execution == active) execution = null; }
        }
        void close() throws SQLException {
            Execution active;
            synchronized (this) { if (closed) return; closed = true; active = execution; }
            if (active != null) active.cancel();
            use.lock();
            try { if (result != null) result.close(); }
            finally {
                if (active != null) finish(active);
                connection.statements.remove(this); use.unlock();
            }
        }
    }

    private static final class ResultHandler extends Handler {
        private static final Set<String> TYPED_GETTERS = Set.of("getObject", "getBoolean", "getByte", "getShort",
                "getInt", "getLong", "getFloat", "getDouble", "getBigDecimal");
        final StatementHandler owner;
        final QueryPipeline.Cursor cursor;
        final Execution execution;
        final ResultSet proxy = proxy(ResultSet.class, this);
        final ResultSetMetaData metadata;
        final long maxRows;
        boolean closed;
        int row;
        ResultHandler(StatementHandler owner, QueryPipeline.Cursor cursor, Execution execution) throws SQLException {
            this.owner = owner; this.cursor = cursor; this.execution = execution; maxRows = owner.maxRows;
            var source = cursor.rows().getMetaData();
            var copy = new RowSetMetaDataImpl();
            var classes = new HashMap<Integer, String>();
            copy.setColumnCount(source.getColumnCount());
            for (int i = 1; i <= source.getColumnCount(); i++) {
                var type = cursor.types().get(i);
                classes.put(i, type == null ? source.getColumnClassName(i) : objectType(type).getName());
                copy.setColumnName(i, source.getColumnName(i)); copy.setColumnLabel(i, source.getColumnLabel(i));
                copy.setColumnType(i, type == null ? source.getColumnType(i) : type.getVendorTypeNumber());
                copy.setColumnTypeName(i, type == null ? source.getColumnTypeName(i) : type.getName());
                copy.setTableName(i, source.getTableName(i)); copy.setSchemaName(i, source.getSchemaName(i));
                copy.setCatalogName(i, source.getCatalogName(i)); copy.setNullable(i, source.isNullable(i));
                copy.setPrecision(i, source.getPrecision(i)); copy.setScale(i, source.getScale(i));
                copy.setColumnDisplaySize(i, source.getColumnDisplaySize(i)); copy.setSigned(i, source.isSigned(i));
                copy.setCaseSensitive(i, source.isCaseSensitive(i)); copy.setSearchable(i, source.isSearchable(i));
            }
            metadata = readOnlyMetadata(copy, Map.copyOf(classes));
        }
        @Override Object invoke(Method method, Object[] args) throws SQLException {
            owner.use.lock();
            try {
                String name = method.getName();
                if (name.equals("isClosed")) return closed;
                if (name.equals("close")) { close(); return null; }
                if (closed) throw new SQLException(sentence("Result set is closed"), "24000");
                return switch (name) {
                    case "getStatement" -> owner.proxy;
                    case "getMetaData" -> metadata;
                    case "getType" -> ResultSet.TYPE_FORWARD_ONLY;
                    case "getConcurrency" -> ResultSet.CONCUR_READ_ONLY;
                    case "getHoldability" -> ResultSet.CLOSE_CURSORS_AT_COMMIT;
                    case "next" -> {
                        if (cursor.ended()) yield false;
                        try {
                            if (maxRows > 0 && row >= maxRows) { cursor.close(); ended(); yield false; }
                            boolean next = cursor.next();
                            if (next) row++; else ended();
                            yield next;
                        } catch (SQLException e) { fail(e); throw e; }
                    }
                    case "getRow" -> cursor.ended() ? 0 : row;
                    case "getWarnings", "clearWarnings" -> null;
                    case "isBeforeFirst" -> !cursor.ended() && cursor.rows().isBeforeFirst();
                    case "isAfterLast" -> row > 0 && cursor.ended();
                    default -> {
                        checkResultOperation(name);
                        if (cursor.ended()) throw new SQLException(sentence("Result set has no current row"), "24000");
                        try {
                            execution.check();
                            Object value;
                            if (TYPED_GETTERS.contains(name)) {
                                int column = column(args[0]);
                                var type = cursor.types().get(column);
                                value = type == null ? call(method, cursor.rows(), args) : typedGetter(method, args, column, type);
                            } else value = call(method, cursor.rows(), args);
                            execution.check();
                            yield value;
                        } catch (SQLException e) { e = execution.failure(e); fail(e); throw e; }
                    }
                };
            } finally { owner.use.unlock(); }
        }
        int column(Object reference) throws SQLException {
            return reference instanceof Integer index ? index : cursor.rows().findColumn((String) reference);
        }
        Object typedGetter(Method method, Object[] args, int column, JDBCType type) throws SQLException {
            Object value = typedValue(column, type);
            Number number = value instanceof Number n ? n : value instanceof Boolean b ? (b ? 1 : 0) : null;
            return switch (method.getName()) {
                case "getObject" -> {
                    if (args.length == 1 || args[1] instanceof Class<?> target && (value == null || target.isInstance(value))) yield value;
                    if (number != null && args[1] == Double.class) yield number.doubleValue();
                    if (number != null && args[1] == Float.class) yield number.floatValue();
                    yield call(method, cursor.rows(), args);
                }
                case "getBoolean" -> type == JDBCType.BOOLEAN ? value != null && (Boolean) value : call(method, cursor.rows(), args);
                case "getByte" -> value == null ? (byte) 0 : number == null ? call(method, cursor.rows(), args) : number.byteValue();
                case "getShort" -> value == null ? (short) 0 : number == null ? call(method, cursor.rows(), args) : number.shortValue();
                case "getInt" -> value == null ? 0 : number == null ? call(method, cursor.rows(), args) : number.intValue();
                case "getLong" -> value == null ? 0L : number == null ? call(method, cursor.rows(), args) : number.longValue();
                case "getFloat" -> value == null ? 0F : number == null ? call(method, cursor.rows(), args) : number.floatValue();
                case "getDouble" -> value == null ? 0D : number == null ? call(method, cursor.rows(), args) : number.doubleValue();
                case "getBigDecimal" -> type == JDBCType.NUMERIC && args.length == 1 ? value : call(method, cursor.rows(), args);
                default -> call(method, cursor.rows(), args);
            };
        }
        Object typedValue(int column, JDBCType type) throws SQLException {
            if (type == JDBCType.VARCHAR) return cursor.rows().getString(column);
            Object value = cursor.rows().getObject(column);
            if (value == null) return null;
            try {
                return switch (type) {
                    case BIGINT -> integral(value);
                    case INTEGER -> Math.toIntExact(integral(value));
                    // JSON stores non-finite numbers as strings; preserve NaN and the infinities too.
                    case DOUBLE -> value instanceof Number n ? n.doubleValue() : Double.parseDouble(value.toString());
                    case BOOLEAN -> {
                        if (value instanceof Number n && (n.doubleValue() == 0 || n.doubleValue() == 1)) yield n.doubleValue() == 1;
                        String text = value.toString();
                        if (text.equalsIgnoreCase("true") || text.equals("1")) yield true;
                        if (text.equalsIgnoreCase("false") || text.equals("0")) yield false;
                        throw new NumberFormatException("Not a boolean");
                    }
                    case NUMERIC -> new java.math.BigDecimal(value.toString());
                    default -> throw new IllegalArgumentException("Unsupported view type " + type);
                };
            } catch (NumberFormatException | ArithmeticException e) {
                throw new SQLException(sentence("Invalid " + type + " value '" + value + "' in column " + metadata.getColumnLabel(column)), "22018", e);
            }
        }
        private static long integral(Object value) {
            if (value instanceof Integer || value instanceof Long) return ((Number) value).longValue();
            return new java.math.BigDecimal(value.toString()).longValueExact();
        }
        void fail(SQLException failure) {
            try { close(); } catch (SQLException close) { failure.addSuppressed(close); }
        }
        void ended() throws SQLException {
            owner.finish(execution);
            if (owner.closeOnCompletion) owner.close();
        }
        void close() throws SQLException {
            if (closed) return;
            closed = true;
            try { cursor.close(); }
            finally { ended(); }
        }
    }
    private static Class<?> objectType(JDBCType type) {
        return switch (type) {
            case BOOLEAN -> Boolean.class;
            case INTEGER -> Integer.class;
            case BIGINT -> Long.class;
            case DOUBLE -> Double.class;
            case NUMERIC -> java.math.BigDecimal.class;
            case VARCHAR -> String.class;
            default -> throw new IllegalArgumentException("Unsupported view type " + type);
        };
    }
    private static ResultSetMetaData readOnlyMetadata(ResultSetMetaData metadata) {
        return readOnlyMetadata(metadata, Map.of());
    }
    private static ResultSetMetaData readOnlyMetadata(ResultSetMetaData metadata, Map<Integer, String> classes) {
        return proxy(ResultSetMetaData.class, new Handler() {
            @Override Object invoke(Method method, Object[] args) throws SQLException {
                return switch (method.getName()) {
                    case "isReadOnly" -> true;
                    case "isWritable", "isDefinitelyWritable" -> false;
                    case "getColumnClassName" -> classes.containsKey((int) args[0])
                            ? classes.get((int) args[0]) : metadata.getColumnClassName((int) args[0]);
                    default -> call(method, metadata, args);
                };
            }
        });
    }
    private static void checkResultOperation(String name) throws SQLFeatureNotSupportedException {
        if (name.startsWith("update") || Set.of("insertRow", "deleteRow", "moveToInsertRow", "moveToCurrentRow",
                "refreshRow", "cancelRowUpdates", "absolute", "relative", "previous", "first", "last", "beforeFirst", "afterLast").contains(name)) {
            throw unsupported("result-set updates or scrolling");
        }
    }
    private static int nonnegative(int value, String name) throws SQLException {
        if (value < 0) throw new SQLException(sentence("Negative " + name)); return value;
    }
    private static SQLException append(SQLException failure, SQLException next) {
        if (failure == null) return next; failure.addSuppressed(next); return failure;
    }
}
