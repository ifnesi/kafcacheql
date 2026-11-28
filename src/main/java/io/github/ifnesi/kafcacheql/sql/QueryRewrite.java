package io.github.ifnesi.kafcacheql.sql;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.config.Mode;
import io.github.ifnesi.kafcacheql.sync.SyncPlan;
import java.sql.SQLException;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.StringJoiner;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/** Replace actual topic references, leaving CTE names and expression scopes to the parser's tree. */
final class QueryRewrite {
    private QueryRewrite() {}

    static String rewrite(Query query, Map<Query.Reference, SyncPlan> plans) throws SQLException {
        var replacements = new IdentityHashMap<Table, FromItem>();
        var selects = new LinkedHashSet<PlainSelect>();
        for (var entry : plans.entrySet()) {
            Query.Reference reference = entry.getKey();
            SyncPlan plan = entry.getValue();
            String sql = "SELECT \"$data\".* FROM main." + StorageDdl.quote(reference.target().viewName()) + " AS \"$data\"";
            if (plan.topic().mode() == Mode.APPEND) {
                var values = new StringJoiner(",");
                plan.windows().forEach((partition, ranges) -> {
                    for (var range : ranges.asRanges()) {
                        values.add("(" + partition + "," + range.lowerEndpoint() + "," + range.upperEndpoint() + ")");
                    }
                });
                // A zero-length interval also represents an empty window, using the same join.
                if (values.length() == 0) values.add("(0,0,0)");
                sql += " JOIN (VALUES " + values + ") AS \"$window\" ON \"$data\"._partition=\"$window\".column1"
                        + " AND \"$data\"._offset>=\"$window\".column2 AND \"$data\"._offset<\"$window\".column3";
            }
            try {
                var replacement = new ParenthesedSelect();
                replacement.setSelect((Select) CCJSqlParserUtil.parse(sql));
                replacement.setAlias(reference.table().getAlias() == null
                        ? new Alias(StorageDdl.quote(reference.table().getUnquotedName()), true) : reference.table().getAlias());
                replacements.put(reference.table(), replacement);
                selects.add(reference.select());
            } catch (JSQLParserException e) { throw new SQLException(sentence("Cannot build the query for its requested window"), e); }
        }
        for (PlainSelect select : selects) {
            select.setFromItem(replace(select.getFromItem(), replacements));
            joins(select.getJoins(), replacements);
        }
        return query.select().toString();
    }

    private static FromItem replace(FromItem item, Map<Table, FromItem> replacements) {
        if (item instanceof Table table) return replacements.getOrDefault(table, table);
        if (item instanceof ParenthesedFromItem group) {
            group.setFromItem(replace(group.getFromItem(), replacements));
            joins(group.getJoins(), replacements);
        }
        return item;
    }

    private static void joins(java.util.List<Join> joins, Map<Table, FromItem> replacements) {
        if (joins != null) for (Join join : joins) join.setFromItem(replace(join.getFromItem(), replacements));
    }
}
