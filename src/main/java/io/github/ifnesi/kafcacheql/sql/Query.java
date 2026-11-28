package io.github.ifnesi.kafcacheql.sql;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import com.google.common.base.Ascii;
import io.github.ifnesi.kafcacheql.cache.TopicRegistry.ResolvedTable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserTreeConstants;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.parser.TokenMgrException;
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
public record Query(Select select, List<Reference> references, Set<Integer> parameters, Command command) {
    public enum Kind { SHOW, DESCRIBE, DROP, SYNC }
    public record Command(Kind kind, String table, boolean ifExists, List<String> patterns) {}
    private static final Set<String> START_WORDS = Set.of("SELECT", "WITH", "SHOW", "DESCRIBE", "DROP", "SYNC");
    private static final String GLOB = "[A-Za-z0-9._?*\\-]+";
    private static final String PATTERN = "(?:" + GLOB + "|\"" + GLOB + "\"|`" + GLOB + "`|\\[" + GLOB + "\\])";
    private static final java.util.regex.Pattern SYNC = java.util.regex.Pattern.compile(
            "(?i)\\s*SYNC(?:\\s+(" + PATTERN + "(?:\\s*,\\s*" + PATTERN + ")*))?\\s*;?\\s*");
    private static Query command(Kind kind, String table, boolean ifExists, List<String> patterns) {
        return new Query(null, List.of(), Set.of(), new Command(kind, table, ifExists, patterns));
    }
    public record Reference(Table table, ResolvedTable target, PlainSelect select) {}
    @FunctionalInterface public interface Resolver { ResolvedTable resolve(String name) throws SQLException; }

    public static Query parseAndResolve(String sql, Resolver resolver) throws SQLException {
        if (sql == null) throw new SQLException(sentence("SQL is null"), "42000");
        var sync = SYNC.matcher(sql);
        if (sync.matches()) {
            var patterns = new ArrayList<String>();
            if (sync.group(1) != null) for (String pattern : sync.group(1).split("\\s*,\\s*")) {
                char first = pattern.charAt(0);
                patterns.add(first == '"' || first == '`' || first == '[' ? pattern.substring(1, pattern.length() - 1) : pattern);
            }
            return command(Kind.SYNC, null, false, List.copyOf(patterns));
        }
        Select select;
        var first = new AtomicReference<Token>();
        try {
            var statements = CCJSqlParserUtil.parseStatements(sql, p -> {
                p.withSquareBracketQuotation(true);
                first.set(p.getToken(1)); // Peek using this parser's lexer; comments and quoting stay its job.
            });
            validateStart(first.get(), null);
            if (statements.size() != 1) throw new SQLException(sentence("Only one statement at a time is supported."), "42000");
            var statement = statements.getFirst();
            if (statement instanceof net.sf.jsqlparser.statement.show.ShowTablesStatement show) {
                if (!show.toString().equalsIgnoreCase("SHOW TABLES")) throw unsupportedStatement();
                return command(Kind.SHOW, null, false, List.of());
            }
            if (statement instanceof net.sf.jsqlparser.statement.DescribeStatement describe) {
                return command(Kind.DESCRIBE, tableName(describe.getTable()), false, List.of());
            }
            if (statement instanceof net.sf.jsqlparser.statement.drop.Drop drop) {
                if (!"TABLE".equalsIgnoreCase(drop.getType()) || drop.isMaterialized() || drop.isUsingTemporary()
                        || drop.getNames().size() != 1 || drop.getParameters() != null && !drop.getParameters().isEmpty()
                        || drop.getTypeToParameters() != null && !drop.getTypeToParameters().isEmpty()) throw unsupportedStatement();
                return command(Kind.DROP, tableName(drop.getName()), drop.isIfExists(), List.of());
            }
            if (!(statement instanceof Select s)
                    || !(s instanceof PlainSelect || s instanceof SetOperationList || s instanceof ParenthesedSelect)) {
                throw unsupportedStatement();
            }
            select = s;
        } catch (JSQLParserException | TokenMgrException e) {
            validateStart(first.get(), e);
            for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                // TokenMgrException exposes its EOF flag only in its own diagnostic message.
                if (cause instanceof TokenMgrException lexer && lexer.getMessage() != null
                        && lexer.getMessage().contains("Encountered: <EOF>")) {
                    throw new SQLException("SQL syntax error: the statement ends inside a quoted name, string or comment that isn't closed.",
                            "42000", e);
                }
                if (cause instanceof ParseException parse && parse.currentToken != null && parse.currentToken.next != null) {
                    throw syntaxError(parse.currentToken.next, e);
                }
            }
            // Other lexical failures and parser timeouts provide no structured rejected token.
            throw new SQLException(sentence("Invalid SQL syntax"), "42000", e);
        }
        var tables = new Tables();
        select.accept((SelectVisitor<Void>) tables, null);
        if (!tables.readOnly) throw unsupportedStatement();
        var references = new ArrayList<Reference>();
        var resolved = new HashMap<String, ResolvedTable>();
        for (var located : tables.references) {
            Table table = located.table();
            String name = tableName(table);
            String key = Ascii.toLowerCase(name);
            ResolvedTable target = resolved.get(key);
            if (target == null) {
                target = resolver.resolve(name);
                resolved.put(key, target);
            }
            references.add(new Reference(table, target, located.select()));
        }
        return new Query(select, List.copyOf(references), Set.copyOf(tables.parameters), null);
    }

    private static void validateStart(Token first, Throwable cause) throws SQLException {
        // Parenthesized SELECTs are already accepted; inspect their first keyword too.
        while (first != null && "(".equals(first.image)) first = first.next;
        if (first == null && cause != null) return; // A lexical failure before the first token.
        String word = first == null ? "" : first.image;
        if (word.isEmpty() || ";".equals(word)) throw new SQLException(sentence("Empty statement."), "42000", cause);
        if (Ascii.equalsIgnoreCase(word, "SYNC")) throw new SQLException(sentence(
                "SYNC expects topic names or patterns separated by commas, such as SYNC orders, shop-*."), "42000", cause);
        if (!START_WORDS.contains(Ascii.toUpperCase(word))) {
            throw new SQLException(sentence("Unknown statement \"" + word
                    + "\": only SELECT, WITH ... SELECT, SHOW TABLES, DESCRIBE, DROP TABLE and SYNC are supported."), "42000", cause);
        }
        Token second = first.next;
        if (second != null && Ascii.equalsIgnoreCase(word, "DROP") && !Ascii.equalsIgnoreCase(second.image, "TABLE"))
            throw new SQLException(sentence("Only DROP TABLE is supported."), "42000", cause);
        if (second != null && Ascii.equalsIgnoreCase(word, "SHOW") && !Ascii.equalsIgnoreCase(second.image, "TABLES"))
            throw syntaxError(second, cause);
    }

    private static SQLException syntaxError(Token token, Throwable cause) {
        String message = "SQL syntax error at line " + token.beginLine + ", column " + token.beginColumn
                + ": unexpected token '" + token.image + "'.";
        if ("-".equals(token.image) || ".".equals(token.image)) {
            message += " Quote names that contain '-' or '.', such as \"my-topic\".";
        }
        return new SQLException(sentence(message), "42000", cause);
    }

    private static String tableName(Table table) throws SQLException {
        restoreIdentifier(table);
        // Only the cache's main schema is available. A quoted dotted name is one name part.
        if (table.getDatabaseName() != null || table.getSchemaName() != null && !Ascii.equalsIgnoreCase(table.getUnquotedSchemaName(), "main"))
            throw new SQLException(sentence("Unknown table " + table.getFullyQualifiedName()), "42S02");
        return table.getUnquotedName();
    }

    private static SQLException unsupportedStatement() {
        return new SQLException(sentence("Only one SELECT, WITH ... SELECT, SHOW TABLES, DESCRIBE, DROP TABLE or SYNC statement is supported"), "42000");
    }

    private static void restoreIdentifier(Table table) {
        // 5.4 splits a single quoted identifier containing dots. The grammar node retains it intact.
        var node = table.getASTNode();
        if (node.getId() == CCJSqlParserTreeConstants.JJTFROMITEM) node = node.jjtGetChild(0);
        if (node.jjtGetFirstToken() == node.jjtGetLastToken()) identifier(table, node.jjtGetFirstToken().image);
    }

    private static void identifier(Table table, String name) {
        table.getNameParts().clear();
        table.setName(name, false);
    }

    /** Reuse the library's traversal, with SQLite's scoped, case-insensitive CTE names. */
    private static final class Tables extends TablesNamesFinder<Void> {
        private record Located(Table table, PlainSelect select) {}
        private final List<Located> references = new ArrayList<>();
        private final Set<Integer> parameters = new HashSet<>();
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
            restoreIdentifier(table);
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

        @Override public <S> Void visit(JdbcParameter parameter, S context) {
            // Folding a constant operand must not renumber the remaining parameters in the query.
            parameter.setUseFixedIndex(true);
            parameters.add(parameter.getIndex());
            return null;
        }
    }
}
