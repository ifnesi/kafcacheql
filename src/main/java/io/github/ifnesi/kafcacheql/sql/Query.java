package io.github.ifnesi.kafcacheql.sql;

import com.google.common.base.Ascii;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry.ResolvedTable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserTreeConstants;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

/** One execution's SQL tree: validate the entire statement before resolving any table. */
public record Query(Select select, List<Reference> references) {
    public record Reference(Table table, ResolvedTable target, PlainSelect select) {}
    @FunctionalInterface public interface Resolver { ResolvedTable resolve(String name) throws SQLException; }

    public static Query parseAndResolve(String sql, Resolver resolver) throws SQLException {
        Select select;
        try {
            var statements = CCJSqlParserUtil.parseStatements(sql, p -> p.withSquareBracketQuotation(true));
            if (statements.size() != 1 || !(statements.getFirst() instanceof Select s)
                    || !(s instanceof PlainSelect || s instanceof SetOperationList || s instanceof ParenthesedSelect)) {
                throw onlySelect();
            }
            select = s;
        } catch (JSQLParserException e) {
            throw new SQLException(e.getMessage(), "42000", e);
        }
        var tables = new Tables();
        select.accept((SelectVisitor<Void>) tables, null);
        if (!tables.readOnly) throw onlySelect();
        var references = new ArrayList<Reference>();
        var resolved = new HashMap<String, ResolvedTable>();
        for (var located : tables.references) {
            Table table = located.table();
            // Only the cache's main schema is available. A quoted dotted name is one name part.
            if (table.getDatabaseName() != null || (table.getSchemaName() != null
                    && !Ascii.equalsIgnoreCase(table.getUnquotedSchemaName(), "main"))) {
                throw new SQLException("Unknown table " + table.getFullyQualifiedName(), "42S02");
            }
            String name = table.getUnquotedName();
            String key = Ascii.toLowerCase(name);
            ResolvedTable target = resolved.get(key);
            if (target == null) {
                target = resolver.resolve(name);
                resolved.put(key, target);
            }
            references.add(new Reference(table, target, located.select()));
        }
        return new Query(select, List.copyOf(references));
    }

    private static SQLException onlySelect() {
        return new SQLException("Only one SELECT or WITH ... SELECT statement is supported", "42000");
    }

    /** Reuse the library's traversal, with SQLite's scoped, case-insensitive CTE names. */
    private static final class Tables extends TablesNamesFinder<Void> {
        private record Located(Table table, PlainSelect select) {}
        private final List<Located> references = new ArrayList<>();
        private Set<String> ctes = Set.of();
        private PlainSelect current;
        private boolean readOnly = true;

        private Tables() { init(false); }

        @Override public <S> Void visit(Select select, S context) {
            return select.accept((SelectVisitor<Void>) this, context);
        }

        private void scoped(Select select, Runnable visit) {
            Set<String> outer = ctes;
            if (select.getWithItemsList() != null) {
                ctes = new HashSet<>(outer);
                for (var item : select.getWithItemsList()) ctes.add(Ascii.toLowerCase(item.getUnquotedAliasName()));
            }
            try { visit.run(); }
            finally { ctes = outer; }
        }

        @Override public <S> Void visit(PlainSelect select, S context) {
            if ((select.getIntoTables() != null && !select.getIntoTables().isEmpty())
                    || select.getIntoTempTable() != null) readOnly = false;
            PlainSelect outer = current;
            current = select;
            try { scoped(select, () -> super.visit(select, context)); }
            finally { current = outer; }
            return null;
        }

        @Override public <S> Void visit(ParenthesedSelect select, S context) {
            scoped(select, () -> super.visit(select, context));
            return null;
        }

        @Override public <S> Void visit(SetOperationList select, S context) {
            scoped(select, () -> super.visit(select, context));
            return null;
        }

        @Override public <S> Void visit(WithItem<?> item, S context) {
            if (item.getStatement() instanceof ParenthesedSelect select) {
                select.accept((SelectVisitor<Void>) this, context);
            } else readOnly = false;
            return null;
        }

        @Override public <S> Void visit(Table table, S context) {
            // 5.4 splits a single quoted identifier containing dots. The grammar node retains it intact.
            var node = table.getASTNode();
            if (node.getId() == CCJSqlParserTreeConstants.JJTFROMITEM) node = node.jjtGetChild(0);
            if (node.jjtGetFirstToken() == node.jjtGetLastToken()) {
                identifier(table, node.jjtGetFirstToken().image);
            }
            if (table.getSchemaName() != null || !ctes.contains(Ascii.toLowerCase(table.getUnquotedName()))) {
                references.add(new Located(table, current));
            }
            return null;
        }

        @Override public <S> Void visit(Column column, S context) {
            restoreQualifier(column.getTable(), column.getASTNode().jjtGetFirstToken().image);
            return null;
        }

        @Override public <S> Void visit(AllTableColumns wildcard, S context) {
            restoreQualifier(wildcard.getTable(), wildcard.getASTNode().jjtGetFirstToken().image);
            return null;
        }

        private static void restoreQualifier(Table qualifier, String token) {
            // Qualifiers preserve their delimiter list even when Table splits a quoted dotted name.
            if (qualifier != null && qualifier.getNameParts().size() > 1 && qualifier.getNamePartDelimiters().isEmpty()) {
                identifier(qualifier, token);
            }
        }

        private static void identifier(Table table, String name) {
            table.getNameParts().clear();
            table.setName(name, false);
        }

        @Override public <S> Void visit(JdbcParameter parameter, S context) {
            // Folding a constant operand must not renumber the remaining parameters in the query.
            parameter.setUseFixedIndex(true);
            return null;
        }
    }
}
