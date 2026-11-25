package io.github.ifnesi.kafcacheql.driver;

import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry;
import io.github.ifnesi.kafcacheql.cache.TypedViews;
import io.github.ifnesi.kafcacheql.config.DriverLoader;
import io.github.ifnesi.kafcacheql.sql.QueryPipeline;
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

/** JDBC wrappers: unsupported operations are explicit, and SQLite owns binding and value conversion. */
final class Jdbc {
    private Jdbc() {}

    static Connection connection(CacheContext.Lease cache, String url) { return new ConnectionHandler(cache, url).proxy; }
    private static SQLFeatureNotSupportedException unsupported(String name) {
        return new SQLFeatureNotSupportedException("KafCacheQL does not support " + name, "0A000");
    }
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(DriverLoader.LOADER, new Class<?>[] {type}, handler));
    }
    private static Object call(Method method, Object target, Object[] args) throws SQLException {
        try { return method.invoke(target, args); }
        catch (IllegalAccessException e) { throw new SQLException(e); }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof SQLException sql) throw sql;
            if (e.getCause() instanceof RuntimeException runtime) throw runtime;
            if (e.getCause() instanceof Error error) throw error;
            throw new SQLException(e.getCause());
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
                throw new SQLException("Not a wrapper for " + ((Class<?>) args[0]).getName());
            }
            return DriverLoader.call(() -> invoke(method, args == null ? new Object[0] : args));
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
        void check() throws SQLException { if (closed) throw new SQLException("Connection is closed", "08003"); }
        @Override Object invoke(Method method, Object[] args) throws SQLException {
            String name = method.getName();
            if (name.equals("isClosed")) return closed;
            if (name.equals("close")) { close(); return null; }
            if (name.equals("isValid")) {
                int timeout = (int) args[0];
                if (timeout < 0) throw new SQLException("Negative validation timeout");
                if (closed) return false;
                try (var execution = new Execution(Duration.ofSeconds(timeout))) { return cache.isValid(execution); }
                catch (SQLException e) { return false; }
            }
            check();
            return switch (name) {
                case "createStatement", "prepareStatement" -> {
                    boolean prepared = name.equals("prepareStatement");
                    if (prepared && args[0] == null) throw new SQLException("SQL is null", "42000");
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
                case "getClientInfo" -> args.length == 0 ? new java.util.Properties() : null;
                case "setClientInfo" -> throw new SQLClientInfoException();
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
        Set<String> topics = Set.of();
        MetadataHandler(ConnectionHandler connection) { this.connection = connection; }

        @Override synchronized Object invoke(Method method, Object[] args) throws SQLException {
            connection.check();
            String name = method.getName();
            switch (name) {
                case "getConnection": return connection.proxy;
                case "getURL": return connection.url;
                case "isReadOnly": return true;
                case "supportsTransactions", "supportsSavepoints", "supportsBatchUpdates", "supportsGetGeneratedKeys",
                        "supportsMultipleTransactions", "supportsDataDefinitionAndDataManipulationTransactions",
                        "supportsDataManipulationTransactionsOnly", "dataDefinitionCausesTransactionCommit",
                        "dataDefinitionIgnoredInTransactions": return false;
                case "getDefaultTransactionIsolation": return Connection.TRANSACTION_NONE;
                case "supportsTransactionIsolationLevel": return (int) args[0] == Connection.TRANSACTION_NONE;
                case "supportsResultSetType": return (int) args[0] == ResultSet.TYPE_FORWARD_ONLY;
                case "supportsResultSetConcurrency": return (int) args[0] == ResultSet.TYPE_FORWARD_ONLY && (int) args[1] == ResultSet.CONCUR_READ_ONLY;
                case "getResultSetHoldability": return ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "supportsResultSetHoldability": return (int) args[0] == ResultSet.CLOSE_CURSORS_AT_COMMIT;
                case "getIdentifierQuoteString", "getSearchStringEscape": return name.equals("getIdentifierQuoteString") ? "\"" : "\\";
                case "getDriverName": return "KafCacheQL";
                case "getDriverMajorVersion": return KafCacheQLDriver.MAJOR_VERSION;
                case "getDriverMinorVersion": return KafCacheQLDriver.MINOR_VERSION;
                case "getDriverVersion": return KafCacheQLDriver.MAJOR_VERSION + "." + KafCacheQLDriver.MINOR_VERSION;
                default: break;
            }
            if (name.equals("getTables")) {
                topics = Set.copyOf(connection.cache.listTopics());
            }
            // An uncached topic has no stored mode yet. Describing it for its key never registers a table.
            String keyMode = null;
            boolean keyVisible = false;
            if (name.equals("getPrimaryKeys") && args[2] != null && !((String) args[2]).startsWith("$")) {
                Key key = connection.cache.read(r -> {
                    var stored = cached(r);
                    String topic = knownViews(r, stored).entrySet().stream()
                            .filter(e -> e.getValue().equalsIgnoreCase((String) args[2])).map(Map.Entry::getKey)
                            .findFirst().orElse((String) args[2]);
                    Table table = stored.get(topic);
                    return new Key(topic, table == null ? null : table.mode());
                });
                keyVisible = visible(key.topic());
                keyMode = key.mode();
                if (keyMode == null) keyMode = connection.cache.describeTopic(key.topic()).map(t -> t.mode().value()).orElse(null);
            }
            String mode = keyMode;
            boolean showKey = keyVisible;
            Object result = connection.cache.read(reader -> {
                DatabaseMetaData sqlite = reader.getMetaData();
                try (var match = reader.prepareStatement("SELECT ? LIKE ? ESCAPE '\\'")) {
                    return switch (name) {
                        case "getCatalogs" -> {
                            var rows = copy(sqlite.getCatalogs()); add(rows, Map.of("TABLE_CAT", "main")); yield rows;
                        }
                        case "getSchemas" -> {
                            var rows = copy(sqlite.getSchemas());
                            if (args.length == 0 || namespace(match, args[0], args[1])) add(rows, Map.of("TABLE_SCHEM", "main", "TABLE_CATALOG", "main"));
                            yield rows;
                        }
                        case "getTableTypes" -> {
                            try (var s = reader.createStatement()) { yield copy(s.executeQuery("SELECT 'TABLE' AS TABLE_TYPE")); }
                        }
                        case "getTables" -> tables(reader, sqlite, match, args);
                        case "getColumns" -> columns(reader, sqlite, match, args);
                        case "getPrimaryKeys" -> {
                            CachedRowSet rows;
                            try (var template = copy(sqlite.getPrimaryKeys(null, null, "$meta"))) { rows = template.createCopySchema(); }
                            String table = (String) args[2];
                            if (namespace(match, args[0], args[1]) && mode != null && showKey) {
                                add(rows, Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", table,
                                        "COLUMN_NAME", mode.equals("compacted") ? "_key" : "_offset", "KEY_SEQ", 2));
                                add(rows, Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", table,
                                        "COLUMN_NAME", "_partition", "KEY_SEQ", 1));
                            } else if (namespace(match, args[0], args[1]) && table != null && table.startsWith("$") && connection.cache.config().metadataShowInternal()) {
                                rows.close(); rows = copy(sqlite.getPrimaryKeys(null, null, table)); normalize(rows);
                            }
                            yield rows;
                        }
                        case "getIndexInfo" -> copy(sqlite.getIndexInfo(null, null, ABSENT, false, false));
                        case "getImportedKeys", "getExportedKeys", "getCrossReference" -> copy(sqlite.getImportedKeys(null, null, ABSENT));
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
        private record Key(String topic, String mode) {}
        private Map<String, String> knownViews(Connection reader, Map<String, Table> stored) throws SQLException {
            var known = new java.util.HashSet<>(topics);
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
        private CachedRowSet tables(Connection reader, DatabaseMetaData sqlite, PreparedStatement match, Object[] args) throws SQLException {
            var rows = copy(sqlite.getTables(null, null, ABSENT, null));
            if (!namespace(match, args[0], args[1]) || args[3] != null && !List.of((String[]) args[3]).contains("TABLE")) return rows;
            var stored = cached(reader);
            var names = new TreeMap<String, String>();
            var views = TopicRegistry.viewNames(reader, topics);
            for (String topic : topics) {
                if (!visible(topic)) continue;
                Table table = stored.get(topic);
                names.put(views.get(topic),
                        table == null ? "" : table.mode() + ", " + table.partitions() + " partitions");
            }
            if (connection.cache.config().metadataShowInternal()) {
                for (String name : internalNames(reader)) names.put(name, "");
            }
            for (var entry : names.entrySet()) if (matches(match, entry.getKey(), args[2])) add(rows,
                    Map.of("TABLE_CAT", "main", "TABLE_SCHEM", "main", "TABLE_NAME", entry.getKey(), "TABLE_TYPE", "TABLE", "REMARKS", entry.getValue()));
            return rows;
        }

        private CachedRowSet columns(Connection reader, DatabaseMetaData sqlite, PreparedStatement match, Object[] args) throws SQLException {
            var result = copy(sqlite.getColumns(null, null, ABSENT, null));
            if (!namespace(match, args[0], args[1])) return result;
            var tables = new TreeMap<String, Table>(String.CASE_INSENSITIVE_ORDER);
            var stored = cached(reader);
            var views = knownViews(reader, stored);
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
            if (closed) throw new SQLException("Result set is closed", "24000");
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
        long maxRows;
        boolean closeOnCompletion;
        StatementHandler(ConnectionHandler connection, String sql) {
            this.connection = connection; this.sql = sql;
            proxy = sql == null ? proxy(Statement.class, this) : proxy(PreparedStatement.class, this);
        }
        void check() throws SQLException {
            connection.check();
            if (closed) throw new SQLException("Statement is closed", "07000");
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
                    if (index < 1) throw new SQLException("Parameter index must be positive", "07009");
                    bindings.put(index, new Binding(method, args.clone())); return null;
                }
                return switch (name) {
                    case "executeQuery", "execute" -> {
                        if (sql != null && args.length != 0) throw new SQLException("A prepared statement uses its prepared SQL");
                        if (sql == null && args.length != 1) throw unsupported("generated keys");
                        String text = sql == null ? (String) args[0] : sql;
                        execute(text);
                        yield name.equals("execute") ? true : result.proxy;
                    }
                    case "getResultSet" -> result == null ? null : result.proxy;
                    case "getConnection" -> connection.proxy;
                    case "executeUpdate", "executeLargeUpdate", "addBatch", "executeBatch", "executeLargeBatch" -> {
                        warnings = null; throw unsupported("updates and batches");
                    }
                    case "clearBatch", "setEscapeProcessing" -> null;
                    case "clearParameters" -> { bindings.clear(); yield null; }
                    case "getWarnings" -> warnings;
                    case "clearWarnings" -> { warnings = null; yield null; }
                    case "getUpdateCount" -> -1;
                    case "getLargeUpdateCount" -> -1L;
                    case "getMoreResults" -> {
                        if (args.length > 0 && (int) args[0] == Statement.KEEP_CURRENT_RESULT) throw unsupported("multiple results");
                        if (result != null) result.close(); result = null; yield false;
                    }
                    case "setFetchSize" -> { fetchSize = nonnegative((int) args[0], name); yield null; }
                    case "getFetchSize" -> fetchSize;
                    case "setQueryTimeout" -> { timeout = nonnegative((int) args[0], name); yield null; }
                    case "getQueryTimeout" -> timeout;
                    case "setMaxRows", "setLargeMaxRows" -> {
                        long value = ((Number) args[0]).longValue();
                        if (value < 0) throw new SQLException("Negative maxRows"); maxRows = value; yield null;
                    }
                    case "getMaxRows" -> {
                        if (maxRows > Integer.MAX_VALUE) throw new SQLException("Use getLargeMaxRows for this limit", "22003");
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
        void execute(String text) throws SQLException {
            warnings = null;
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
                cursor = QueryPipeline.open(connection.cache, text, values, active, fetchSize, maxRows, new QueryPipeline.Bindings() {
                    @Override public void validate(Set<Integer> parameters) throws SQLException {
                        for (int index : bindings.keySet()) {
                            if (!parameters.contains(index)) throw new SQLException("Unknown parameter " + index, "07009");
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
                    var copy = new SQLWarning(warning.getMessage(), warning.getSQLState(), warning.getErrorCode(), warning.getCause());
                    if (tail == null) warnings = copy; else tail.setNextWarning(copy);
                    tail = copy;
                }
            } catch (SQLException | RuntimeException | Error e) {
                if (cursor != null) try { cursor.close(); } catch (SQLException close) { e.addSuppressed(close); }
                finish(active);
                throw e;
            }
        }
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
        final StatementHandler owner;
        final QueryPipeline.Cursor cursor;
        final Execution execution;
        final ResultSet proxy = proxy(ResultSet.class, this);
        final ResultSetMetaData metadata;
        boolean closed;
        int row;
        ResultHandler(StatementHandler owner, QueryPipeline.Cursor cursor, Execution execution) throws SQLException {
            this.owner = owner; this.cursor = cursor; this.execution = execution;
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
                if (closed) throw new SQLException("Result set is closed", "24000");
                return switch (name) {
                    case "getStatement" -> owner.proxy;
                    case "getMetaData" -> metadata;
                    case "getType" -> ResultSet.TYPE_FORWARD_ONLY;
                    case "getConcurrency" -> ResultSet.CONCUR_READ_ONLY;
                    case "getHoldability" -> ResultSet.CLOSE_CURSORS_AT_COMMIT;
                    case "next" -> {
                        if (cursor.ended()) yield false;
                        try {
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
                        if (cursor.ended()) throw new SQLException("Result set has no current row", "24000");
                        try {
                            execution.check();
                            Object value;
                            if (name.equals("getDouble") || name.equals("getFloat") || name.equals("getObject")
                                    && args.length == 2 && (args[1] == Double.class || args[1] == Float.class)) {
                                int column = column(args[0]);
                                if (cursor.types().get(column) == JDBCType.DOUBLE) {
                                    double number = floating(column);
                                    if (name.equals("getObject") && cursor.rows().wasNull()) value = null;
                                    else if (name.equals("getFloat") || args.length == 2 && args[1] == Float.class) value = (float) number;
                                    else value = number;
                                } else value = call(method, cursor.rows(), args);
                            } else if (name.equals("getObject") && args.length == 1) {
                                int column = column(args[0]);
                                var type = cursor.types().get(column);
                                value = type == null ? cursor.rows().getObject(column) : switch (type) {
                                    case BOOLEAN -> cursor.rows().getBoolean(column);
                                    case INTEGER -> cursor.rows().getInt(column);
                                    case BIGINT -> cursor.rows().getLong(column);
                                    case DOUBLE -> floating(column);
                                    case NUMERIC -> cursor.rows().getBigDecimal(column);
                                    case VARCHAR -> cursor.rows().getString(column);
                                    default -> throw new IllegalArgumentException("Unsupported view type " + type);
                                };
                                if (cursor.rows().wasNull()) value = null;
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
        double floating(int column) throws SQLException {
            Object value = cursor.rows().getObject(column);
            if (value == null) return 0;
            if (value instanceof Number number) return number.doubleValue();
            // JSON stores non-finite Avro numbers as strings; SQLite's numeric getters turn them into zero.
            try { return Double.parseDouble((String) value); }
            catch (NumberFormatException e) { throw new SQLException("Invalid DOUBLE value in column " + column, "22018", e); }
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
        if (value < 0) throw new SQLException("Negative " + name); return value;
    }
    private static SQLException append(SQLException failure, SQLException next) {
        if (failure == null) return next; failure.addSuppressed(next); return failure;
    }
}
