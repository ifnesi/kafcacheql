package io.github.ifnesi.kafcacheql.sql;

import com.google.common.base.Ascii;
import io.github.ifnesi.kafcacheql.cache.StorageDdl;
import io.github.ifnesi.kafcacheql.cache.TypedViews;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

/** Schema types follow direct columns and wildcards; computed expressions keep SQLite's metadata. */
final class ResultTypes {
    private record Output(String name, JDBCType type) {}
    private record Source(String name, List<Output> columns, List<Source> members) {
        Source(String name, List<Output> columns) { this(name, columns, List.of()); }
    }
    private final Map<Table, List<Output>> tables = new IdentityHashMap<>();
    private final Connection reader;
    private ResultTypes(Connection reader) { this.reader = reader; }

    static Map<Integer, JDBCType> describe(Query query, Connection reader, Map<String, TypedViews.Layout> schemas) throws SQLException {
        var types = new ResultTypes(reader);
        var layouts = new HashMap<String, List<Output>>();
        schemas.forEach((topic, layout) -> layouts.put(topic, TypedViews.columns(layout).stream()
                .map(c -> new Output(c.name(), c.type())).toList()));
        for (var reference : query.references()) {
            List<Output> columns = reference.target().topic().map(layouts::get).orElse(null);
            if (columns == null) {
                columns = types.names("SELECT * FROM main." + StorageDdl.quote(reference.target().viewName()) + " LIMIT 0");
            }
            types.tables.put(reference.table(), columns);
        }
        var columns = types.select(query.select(), null);
        var result = new HashMap<Integer, JDBCType>();
        for (int i = 0; i < columns.size(); i++) if (columns.get(i).type() != null) result.put(i + 1, columns.get(i).type());
        return Map.copyOf(result);
    }

    /** Resolve CTEs on demand: SQLite permits forward references as well as recursive ones. */
    private final class Scope {
        private final Scope parent;
        private final Map<String, WithItem<?>> definitions = new HashMap<>();
        private final Map<String, List<Output>> resolved = new HashMap<>();
        Scope(Scope parent, List<WithItem<?>> definitions) {
            this.parent = parent;
            if (definitions != null) for (var with : definitions) this.definitions.put(key(with.getUnquotedAliasName()), with);
        }
        List<Output> get(String name) throws SQLException {
            if (resolved.containsKey(name)) return resolved.get(name);
            var with = definitions.get(name);
            if (with == null) return parent == null ? List.of() : parent.get(name);
            resolved.put(name, List.of()); // A recursive reference has no independent schema.
            var columns = select((ParenthesedSelect) with.getStatement(), this);
            if (with.getWithItemList() != null && with.getWithItemList().size() == columns.size()) {
                var renamed = new ArrayList<Output>();
                for (int i = 0; i < columns.size(); i++) {
                    var expression = with.getWithItemList().get(i).getExpression();
                    String label = expression instanceof Column column ? column.getUnquotedColumnName() : expression.toString();
                    renamed.add(new Output(label, columns.get(i).type()));
                }
                columns = renamed;
            }
            resolved.put(name, columns);
            return columns;
        }
    }

    private List<Output> select(Select select, Scope outer) throws SQLException {
        var ctes = new Scope(outer, select.getWithItemsList());
        if (select instanceof ParenthesedSelect nested) return select(nested.getSelect(), ctes);
        if (select instanceof SetOperationList set) {
            var output = new ArrayList<>(select(set.getSelects().getFirst(), ctes));
            for (int branch = 1; branch < set.getSelects().size(); branch++) {
                var columns = select(set.getSelects().get(branch), ctes);
                for (int i = 0; i < output.size(); i++) {
                    Output first = output.get(i);
                    if (i >= columns.size() || first.type() != columns.get(i).type())
                        output.set(i, new Output(first.name(), null));
                }
            }
            return output;
        }
        if (select instanceof Values values) {
            var expressions = values.getExpressions();
            var row = expressions.getFirst() instanceof ExpressionList<?> list ? list : expressions;
            var columns = new ArrayList<Output>();
            for (int i = 0; i < row.size(); i++) columns.add(new Output("column" + (i + 1), null));
            return columns;
        }
        if (!(select instanceof PlainSelect plain)) return names(select.toString());
        var sources = new ArrayList<Source>();
        source(plain.getFromItem(), ctes, sources);
        if (plain.getJoins() != null) for (var join : plain.getJoins()) source(join.getFromItem(), ctes, sources);
        var joined = joined(sources, plain.getJoins());
        var output = new ArrayList<Output>();
        for (var item : plain.getSelectItems()) {
            var expression = item.getExpression();
            if (expression instanceof AllTableColumns all) {
                for (var source : qualified(all.getTable().getUnquotedName(), sources)) output.addAll(source.columns());
            } else if (expression instanceof AllColumns) {
                output.addAll(joined);
            } else {
                Output origin = expression instanceof Column column ? column(column, sources, joined) : null;
                String name = item.getAlias() != null ? item.getAlias().getUnquotedName()
                        : origin != null ? origin.name() : expression.toString();
                output.add(new Output(name, origin == null ? null : origin.type()));
            }
        }
        return output;
    }

    private void source(FromItem item, Scope ctes, List<Source> sources) throws SQLException {
        if (item == null) return;
        String name = item.getAlias() == null ? "" : item.getAlias().getUnquotedName();
        if (item instanceof Table table) {
            if (name.isEmpty()) name = table.getUnquotedName();
            var columns = tables.get(table);
            sources.add(new Source(name, columns == null ? ctes.get(key(table.getUnquotedName())) : columns));
        } else if (item instanceof ParenthesedSelect nested) sources.add(new Source(name, select(nested, ctes)));
        else if (item instanceof TableFunction table) {
            var function = table.getFunction();
            // A table-valued function's column layout is SQLite's, independent of its argument values.
            int count = function.getParameters() == null ? 0 : function.getParameters().size();
            String arguments = String.join(",", java.util.Collections.nCopies(count, "NULL"));
            if (name.isEmpty()) name = function.getName();
            sources.add(new Source(name, names("SELECT * FROM " + function.getName() + "(" + arguments + ") LIMIT 0")));
        }
        else if (item instanceof ParenthesedFromItem group) {
            var nested = new ArrayList<Source>();
            source(group.getFromItem(), ctes, nested);
            if (group.getJoins() != null) for (var join : group.getJoins()) source(join.getFromItem(), ctes, nested);
            sources.add(new Source(name, joined(nested, group.getJoins()), nested));
        } else sources.add(new Source(name, names("SELECT * FROM " + item + " LIMIT 0")));
    }
    private List<Output> names(String sql) throws SQLException {
        var columns = new ArrayList<Output>();
        try (var s = reader.prepareStatement(sql)) {
            var metadata = s.getMetaData();
            for (int i = 1; i <= metadata.getColumnCount(); i++) columns.add(new Output(metadata.getColumnName(i), null));
        }
        return columns;
    }
    private static List<Output> joined(List<Source> sources, List<Join> joins) {
        var output = new ArrayList<Output>();
        var names = new java.util.HashSet<String>();
        for (int i = 0; i < sources.size(); i++) {
            var using = new java.util.HashSet<String>();
            if (i > 0 && joins != null) {
                var join = joins.get(i - 1);
                if (join.getUsingColumns() != null) join.getUsingColumns().forEach(c -> using.add(key(c.getUnquotedColumnName())));
                if (join.isNatural()) using.addAll(names);
            }
            for (var column : sources.get(i).columns()) if (!using.contains(key(column.name()))) output.add(column);
            sources.get(i).columns().forEach(c -> names.add(key(c.name())));
        }
        return output;
    }
    private static List<Source> qualified(String name, List<Source> sources) {
        var found = new ArrayList<Source>();
        for (var source : sources) {
            if (key(name).equals(key(source.name()))) found.add(source);
            else if (!source.members().isEmpty()) found.addAll(qualified(name, source.members()));
        }
        return found;
    }
    private static Output column(Column column, List<Source> sources, List<Output> joined) {
        String qualifier = column.getTable() == null ? null : column.getTable().getUnquotedName();
        var candidates = qualifier == null ? joined.stream()
                : qualified(qualifier, sources).stream().flatMap(s -> s.columns().stream());
        Output found = null;
        for (var iterator = candidates.iterator(); iterator.hasNext();) {
            var candidate = iterator.next();
            if (key(candidate.name()).equals(key(column.getUnquotedColumnName()))) {
                if (found != null) return null;
                found = candidate;
            }
        }
        return found;
    }
    private static String key(String name) { return Ascii.toLowerCase(name); }
}
