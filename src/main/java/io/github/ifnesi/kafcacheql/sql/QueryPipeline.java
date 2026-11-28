package io.github.ifnesi.kafcacheql.sql;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.github.ifnesi.kafcacheql.cache.TypedViews;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.SyncPlan;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Parse, plan, sync and open a checked read snapshot for one execution. */
public final class QueryPipeline {
    private QueryPipeline() {}

    public record Result(List<String> columns, List<List<Object>> rows, List<SQLWarning> warnings) {}
    record Window(SyncPlan plan, long generation) {}
    interface Hooks {
        default void afterSync(List<Window> windows) throws SQLException {}
        default void afterCheck(Connection reader) throws SQLException {}
    }

    public static Result execute(CacheContext.Lease cache, String sql, Map<Integer, ?> parameters, Execution execution)
            throws SQLException {
        return execute(cache, sql, parameters, execution, new Hooks() {});
    }

    static Result execute(CacheContext.Lease cache, String sql, Map<Integer, ?> parameters, Execution execution, Hooks hooks)
            throws SQLException {
        try (var cursor = open(cache, sql, parameters, execution, 0, 0, objects(parameters), hooks)) {
            var metadata = cursor.rows().getMetaData();
            var columns = new ArrayList<String>(metadata.getColumnCount());
            for (int i = 1; i <= metadata.getColumnCount(); i++) columns.add(metadata.getColumnLabel(i));
            var result = new ArrayList<List<Object>>();
            while (cursor.next()) {
                var row = new ArrayList<Object>(columns.size());
                for (int i = 1; i <= columns.size(); i++) row.add(cursor.rows().getObject(i));
                result.add(Collections.unmodifiableList(row));
            }
            return new Result(List.copyOf(columns), List.copyOf(result), cursor.warnings());
        }
    }

    @FunctionalInterface public interface Bindings {
        void bind(PreparedStatement statement) throws SQLException;
        default void validate(Set<Integer> parameters) throws SQLException {}
    }

    private static Bindings objects(Map<Integer, ?> parameters) {
        return statement -> {
            int count = statement.getParameterMetaData().getParameterCount();
            for (var parameter : parameters.entrySet()) {
                if (parameter.getKey() <= count) statement.setObject(parameter.getKey(), parameter.getValue());
            }
        };
    }

    public static Cursor open(CacheContext.Lease cache, String sql, Map<Integer, ?> parameters, Execution execution,
                              int fetchSize, long maxRows, Bindings bindings) throws SQLException {
        return open(cache, sql, parameters, execution, fetchSize, maxRows, bindings, new Hooks() {});
    }

    private static Cursor open(CacheContext.Lease cache, String sql, Map<Integer, ?> parameters, Execution execution,
                               int fetchSize, long maxRows, Bindings bindings, Hooks hooks) throws SQLException {
        return execution.run(cache.config().offline(), () -> open(cache,
                Query.parseAndResolve(sql, name -> cache.resolveTable(name, execution)), parameters, execution, fetchSize, maxRows, bindings, hooks));
    }

    public static Cursor open(CacheContext.Lease cache, Query query, Map<Integer, ?> parameters, Execution execution,
                              int fetchSize, long maxRows, Bindings bindings) throws SQLException {
        return open(cache, query, parameters, execution, fetchSize, maxRows, bindings, new Hooks() {});
    }
    private static Cursor open(CacheContext.Lease cache, Query query, Map<Integer, ?> parameters, Execution execution,
                              int fetchSize, long maxRows, Bindings bindings, Hooks hooks) throws SQLException {
        long startedAt = System.currentTimeMillis();
        return execution.run(cache.config().offline(), () -> {
            if (query.command() != null) throw new SQLException(sentence("A cache statement requires JDBC execution"), "42000");
            for (int index : query.parameters()) {
                if (!parameters.containsKey(index)) throw new SQLException(sentence("Parameter " + index + " is not bound"), "07001");
            }
            bindings.validate(query.parameters());
            var bounds = cache.read(execution, c -> Bounds.extract(query, c, parameters));
            var topics = new LinkedHashMap<String, List<Query.Reference>>();
            var warnings = new ArrayList<SQLWarning>();
            for (var reference : query.references()) {
                reference.target().warning().ifPresent(warnings::add);
                reference.target().topic().ifPresent(topic -> topics.computeIfAbsent(topic, ignored -> new ArrayList<>()).add(reference));
            }
            var plans = new LinkedHashMap<Query.Reference, SyncPlan>();
            var synced = new ArrayList<Window>();
            for (var topic : topics.entrySet()) {
                SyncPlan base = cache.plan(topic.getKey(), execution);
                Map<Integer, RangeSet<Long>> union = new TreeMap<>();
                var planner = new WindowPlanner(cache, base, execution, startedAt);
                for (var reference : topic.getValue()) {
                    SyncPlan plan = base.withWindows(planner.windows(bounds.get(reference), warnings));
                    plans.put(reference, plan);
                    plan.windows().forEach((partition, ranges) ->
                            union.computeIfAbsent(partition, ignored -> TreeRangeSet.create()).addAll(ranges));
                    warnings.addAll(plan.warnings());
                }
                SyncPlan plan = base.withWindows(union);
                var reference = topic.getValue().getFirst();
                var tail = bounds.get(reference).tailRecords();
                long generation;
                while (true) {
                    generation = cache.sync(plan, execution);
                    if (tail.isEmpty()) break;
                    SyncPlan expanded = planner.extendTail(plan, generation, tail.getAsLong());
                    if (expanded == plan) break;
                    plan = expanded;
                    plans.put(reference, plan);
                }
                synced.add(new Window(plan, generation));
            }
            hooks.afterSync(List.copyOf(synced));
            var reader = cache.borrowReader(execution);
            try {
                var layouts = new LinkedHashMap<String, TypedViews.Layout>();
                for (var window : synced) {
                    cache.validate(reader.connection(), window.plan(), window.generation());
                    if (cache.config().offline()) warnings.add(cache.offlineWarning(reader.connection(), window.plan(), window.generation()));
                    String topic = window.plan().topic().name();
                    var layout = TypedViews.describe(reader.connection(), window.generation());
                    layouts.put(topic, layout);
                    warnings.addAll(TypedViews.warnings(layout, topic));
                }
                hooks.afterCheck(reader.connection());
                var types = ResultTypes.describe(query, reader.connection(), layouts);
                String rewritten = QueryRewrite.rewrite(query, plans);
                execution.check();
                PreparedStatement statement;
                try { statement = reader.prepare(rewritten); }
                catch (SQLException e) { throw preparationFailure(e, reader.connection(), query, synced, layouts); }
                statement.setFetchSize(fetchSize);
                statement.setLargeMaxRows(maxRows);
                bindings.bind(statement);
                ResultSet rows = reader.executeQuery();
                execution.check();
                var generations = new LinkedHashMap<String, Long>();
                for (var window : synced) generations.put(window.plan().topic().name(), window.generation());
                cache.queried(generations, startedAt);
                return new Cursor(reader, rows, execution, List.copyOf(warnings), types);
            } catch (SQLException | RuntimeException | Error e) {
                try { reader.close(); } catch (SQLException close) { e.addSuppressed(close); }
                throw e;
            }
        });
    }

    private static SQLException preparationFailure(SQLException failure, Connection reader, Query query,
            List<Window> windows, Map<String, TypedViews.Layout> layouts) throws SQLException {
        // SQLite has no missing-column result code: it reports SQLITE_ERROR and names the column in the message.
        if (!(failure instanceof org.sqlite.SQLiteException sqlite)
                || sqlite.getResultCode() != org.sqlite.SQLiteErrorCode.SQLITE_ERROR) return failure;
        var missing = java.util.regex.Pattern.compile("\\(no such column: ([^)]+)\\)$").matcher(failure.getMessage());
        if (!missing.find()) return failure;
        String column = missing.group(1);
        int dot = column.lastIndexOf('.');
        String field = column.substring(dot + 1);
        var candidates = new java.util.HashSet<String>();
        for (var reference : query.references()) {
            String qualifier = reference.table().getAlias() == null ? reference.table().getUnquotedName()
                    : reference.table().getAlias().getUnquotedName();
            if (dot < 0 || com.google.common.base.Ascii.equalsIgnoreCase(qualifier, column.substring(0, dot))) {
                reference.target().topic().ifPresent(candidates::add);
            }
        }
        var topics = new ArrayList<String>();
        for (var window : windows) {
            String topic = window.plan().topic().name();
            if (candidates.contains(topic) && layouts.get(topic).columns().isEmpty()
                    && !TypedViews.hasSchemas(reader, window.generation())) topics.add(topic);
        }
        if (topics.isEmpty()) return failure;
        return new SQLException(sentence(io.github.ifnesi.kafcacheql.config.ConfigErrors.detail(failure) + "; column " + column + " isn't known yet for topic(s) " + topics
                + " because no records with a schema are cached; use _value ->> '$." + field + "' to read the JSON value"),
                "42S22", failure.getErrorCode(), failure);
    }

    /** Owns the live reader until exhaustion, an error, or explicit close. Execution belongs to its caller. */
    public static final class Cursor implements AutoCloseable {
        private final CacheContext.Reader reader;
        private final ResultSet rows;
        private final Execution execution;
        private final List<SQLWarning> warnings;
        private final Map<Integer, JDBCType> types;
        private boolean ended;

        private Cursor(CacheContext.Reader reader, ResultSet rows, Execution execution, List<SQLWarning> warnings,
                       Map<Integer, JDBCType> types) {
            this.reader = reader; this.rows = rows; this.execution = execution; this.warnings = warnings; this.types = types;
        }
        public static Cursor detached(ResultSet rows, Execution execution, List<SQLWarning> warnings) {
            return new Cursor(null, rows, execution, List.copyOf(warnings), Map.of());
        }
        public ResultSet rows() { return rows; }
        public List<SQLWarning> warnings() { return warnings; }
        public Map<Integer, JDBCType> types() { return types; }
        public boolean ended() { return ended; }
        public boolean next() throws SQLException {
            if (ended) return false;
            try {
                execution.check();
                boolean next = rows.next();
                execution.check();
                if (!next) close();
                return next;
            } catch (SQLException e) {
                try { close(); } catch (SQLException close) { e.addSuppressed(close); }
                throw execution.failure(e);
            }
        }
        @Override public void close() throws SQLException {
            if (ended) return;
            ended = true;
            if (reader == null) rows.close(); else reader.close();
        }
    }

}
