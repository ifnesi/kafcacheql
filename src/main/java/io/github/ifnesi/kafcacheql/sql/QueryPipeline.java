package io.github.ifnesi.kafcacheql.sql;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.github.ifnesi.kafcacheql.cache.RangeStore;
import io.github.ifnesi.kafcacheql.sync.CacheContext;
import io.github.ifnesi.kafcacheql.sync.Execution;
import io.github.ifnesi.kafcacheql.sync.StaleGeneration;
import io.github.ifnesi.kafcacheql.sync.SyncPlan;
import io.github.ifnesi.kafcacheql.sync.SyncWorker;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Parse, plan, sync and read one execution. JDBC statement/result wrappers will sit above this entry. */
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
        long startedAt = System.currentTimeMillis();
        return execution.run(cache.config().offline(), () -> {
            Query query = Query.parseAndResolve(sql, name -> cache.resolveTable(name, execution));
            var bounds = cache.read(c -> Bounds.extract(query, c, parameters));
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
                synced.add(new Window(plan, cache.sync(plan, execution)));
            }
            String rewritten = QueryRewrite.rewrite(query, plans);
            hooks.afterSync(List.copyOf(synced));
            return cache.read(reader -> {
                for (var window : synced) validate(reader, window);
                hooks.afterCheck(reader);
                execution.check();
                try (var statement = reader.prepareStatement(rewritten)) {
                    int count = statement.getParameterMetaData().getParameterCount();
                    for (var parameter : parameters.entrySet()) {
                        if (parameter.getKey() <= count) statement.setObject(parameter.getKey(), parameter.getValue());
                    }
                    try (var rows = statement.executeQuery()) {
                        var metadata = rows.getMetaData();
                        var columns = new ArrayList<String>(metadata.getColumnCount());
                        for (int i = 1; i <= metadata.getColumnCount(); i++) columns.add(metadata.getColumnLabel(i));
                        var result = new ArrayList<List<Object>>();
                        while (rows.next()) {
                            execution.check();
                            var row = new ArrayList<Object>(columns.size());
                            for (int i = 1; i <= columns.size(); i++) row.add(rows.getObject(i));
                            result.add(Collections.unmodifiableList(row));
                        }
                        return new Result(List.copyOf(columns), List.copyOf(result), List.copyOf(warnings));
                    }
                }
            });
        });
    }

    private static void validate(Connection reader, Window window) throws SQLException {
        String topic = window.plan().topic().name();
        SyncWorker.requireGeneration(reader, topic, window.generation());
        for (var partition : window.plan().windows().entrySet()) {
            if (!RangeStore.missing(reader, window.generation(), partition.getKey(), partition.getValue()).isEmpty()) {
                throw new StaleGeneration(topic);
            }
        }
    }
}
