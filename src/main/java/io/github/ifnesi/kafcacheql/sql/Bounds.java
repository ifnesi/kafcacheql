package io.github.ifnesi.kafcacheql.sql;

import static io.github.ifnesi.kafcacheql.config.ConfigErrors.sentence;

import com.google.common.base.Ascii;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.JsonAggregateFunction;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.IsUnknownExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/** Fetch hints for append references; compacted syncs still fetch the whole state. */
public record Bounds(ImmutableRangeSet<Long> partitions, ImmutableRangeSet<Long> offsets,
                     ImmutableRangeSet<Long> timestamps, boolean hasWindowBound, OptionalLong tailRecords) {
    public Bounds(ImmutableRangeSet<Long> partitions, ImmutableRangeSet<Long> offsets,
                  ImmutableRangeSet<Long> timestamps, boolean hasWindowBound) {
        this(partitions, offsets, timestamps, hasWindowBound, OptionalLong.empty());
    }
    private enum Axis { PARTITION, OFFSET, TIMESTAMP }
    private enum Op { EQ, LT, LE, GT, GE }
    private static final ImmutableRangeSet<Long> ALL = ImmutableRangeSet.of(Range.all());

    /** Runs at execute time on a reader, with a fresh Query for each execution. Numeric constant operands
     * are folded into the AST so final filtering uses the same clock/random values as fetch planning.
     * Parameter keys are the original, one-based JDBC positions.
     */
    public static Map<Query.Reference, Bounds> extract(Query query, Connection sqlite, Map<Integer, ?> parameters)
            throws SQLException {
        var builders = new LinkedHashMap<Query.Reference, Builder>();
        var selects = new LinkedHashMap<PlainSelect, List<Builder>>();
        for (var reference : query.references()) {
            var builder = new Builder(reference);
            builders.put(reference, builder);
            if (reference.target().topic().isPresent() && reference.select() != null) {
                selects.computeIfAbsent(reference.select(), ignored -> new ArrayList<>()).add(builder);
            }
        }
        var extraction = new Extraction(sqlite, parameters);
        for (var entry : selects.entrySet()) {
            if (extraction.conditions(entry.getKey().getWhere(), entry.getValue()) && query.references().size() == 1) {
                Builder builder = entry.getValue().getFirst();
                builder.tailRecords = tail(query, builder, sqlite);
            }
        }
        var result = new LinkedHashMap<Query.Reference, Bounds>();
        builders.forEach((reference, builder) -> result.put(reference, builder.build()));
        return result;
    }

    private static final class Builder {
        final Query.Reference reference;
        final RangeSet<Long> partitions = TreeRangeSet.create(ALL);
        final RangeSet<Long> offsets = TreeRangeSet.create(ALL);
        final RangeSet<Long> timestamps = TreeRangeSet.create(ALL);
        boolean windowBound;
        OptionalLong tailRecords = OptionalLong.empty();

        Builder(Query.Reference reference) { this.reference = reference; }

        void restrict(Axis axis, RangeSet<Long> allowed) {
            RangeSet<Long> current = switch (axis) {
                case PARTITION -> partitions;
                case OFFSET -> offsets;
                case TIMESTAMP -> timestamps;
            };
            current.removeAll(allowed.complement());
            windowBound |= axis != Axis.PARTITION;
        }

        Bounds build() {
            return new Bounds(ImmutableRangeSet.copyOf(partitions), ImmutableRangeSet.copyOf(offsets),
                    ImmutableRangeSet.copyOf(timestamps), windowBound, tailRecords);
        }
    }

    private record Target(Builder builder, Axis axis) {}
    private record Scalar(Object value, Expression expression) {}

    private record Extraction(Connection sqlite, Map<Integer, ?> parameters) {
        boolean conditions(Expression expression, List<Builder> sources) throws SQLException {
            expression = unwrap(expression);
            if (expression == null) return true;
            if (expression instanceof AndExpression and) {
                return conditions(and.getLeftExpression(), sources) & conditions(and.getRightExpression(), sources);
            } else if (expression instanceof Between between && !between.isNot() && !between.isUsingSymmetric()) {
                Target target = target(between.getLeftExpression(), sources);
                if (target == null || target.axis() == Axis.PARTITION) return false;
                var values = evaluate(List.of(between.getBetweenExpressionStart(), between.getBetweenExpressionEnd()));
                if (values == null) return false;
                target.builder().restrict(target.axis(), range(Op.GE, values.get(0).value()));
                target.builder().restrict(target.axis(), range(Op.LE, values.get(1).value()));
                between.setBetweenExpressionStart(values.get(0).expression());
                between.setBetweenExpressionEnd(values.get(1).expression());
            } else if (expression instanceof InExpression in && !in.isNot()
                    && in.getRightExpression() instanceof ExpressionList<?> operands) {
                Target target = target(in.getLeftExpression(), sources);
                if (target == null || target.axis() == Axis.TIMESTAMP) return false;
                var values = evaluate(operands);
                if (values == null) return false;
                var allowed = TreeRangeSet.<Long>create();
                var folded = new ArrayList<Expression>();
                for (Scalar value : values) {
                    allowed.addAll(range(Op.EQ, value.value()));
                    folded.add(value.expression());
                }
                target.builder().restrict(target.axis(), allowed);
                in.setRightExpression(new ParenthesedExpressionList<>(folded));
                return target.axis() == Axis.PARTITION;
            } else if (expression instanceof BinaryExpression comparison) {
                Op op = switch (comparison) {
                    case EqualsTo ignored -> Op.EQ;
                    case MinorThan ignored -> Op.LT;
                    case MinorThanEquals ignored -> Op.LE;
                    case GreaterThan ignored -> Op.GT;
                    case GreaterThanEquals ignored -> Op.GE;
                    default -> null;
                };
                if (op == null) return false;
                Target target = target(comparison.getLeftExpression(), sources);
                boolean reversed = target == null;
                if (reversed) {
                    target = target(comparison.getRightExpression(), sources);
                    op = switch (op) { case EQ -> Op.EQ; case LT -> Op.GT; case LE -> Op.GE; case GT -> Op.LT; case GE -> Op.LE; };
                }
                if (target == null || (target.axis() == Axis.PARTITION && op != Op.EQ)) return false;
                var values = evaluate(List.of(reversed ? comparison.getLeftExpression() : comparison.getRightExpression()));
                if (values == null) return false;
                Scalar value = values.getFirst();
                target.builder().restrict(target.axis(), range(op, value.value()));
                if (reversed) comparison.setLeftExpression(value.expression()); else comparison.setRightExpression(value.expression());
                return target.axis() == Axis.PARTITION;
            }
            return false;
        }

        private List<Scalar> evaluate(List<? extends Expression> operands) throws SQLException {
            var constant = new Constant();
            for (Expression operand : operands) operand.accept(constant, null);
            if (!constant.valid || operands.isEmpty()) return null;
            String sql = operands.stream().map(Object::toString).collect(Collectors.joining(",", "SELECT ", ""));
            try (var statement = sqlite.prepareStatement(sql)) {
                for (int index : constant.parameters) {
                    if (!parameters.containsKey(index)) throw new SQLException(sentence("Parameter " + index + " is not bound"), "07001");
                    statement.setObject(index, parameters.get(index));
                }
                try (var rows = statement.executeQuery()) {
                    rows.next();
                    var values = new ArrayList<Scalar>(operands.size());
                    for (int i = 0; i < operands.size(); i++) {
                        Object value = rows.getObject(i + 1);
                        Expression folded = switch (value) {
                            case null -> new NullValue();
                            case Integer n -> new LongValue(n.longValue());
                            case Long n -> new LongValue(n);
                            case Double n when Double.isFinite(n) -> new DoubleValue(n);
                            default -> operands.get(i);
                        };
                        values.add(new Scalar(value, folded));
                    }
                    return values;
                }
            }
        }
    }

    private static Expression unwrap(Expression expression) {
        while (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) expression = list.getFirst();
        return expression;
    }

    private static Target target(Expression expression, List<Builder> sources) {
        if (!(unwrap(expression) instanceof Column column)) return null;
        Axis axis = switch (Ascii.toLowerCase(column.getUnquotedColumnName())) {
            case "_partition" -> Axis.PARTITION;
            case "_offset" -> Axis.OFFSET;
            case "_timestamp" -> Axis.TIMESTAMP;
            default -> null;
        };
        if (axis == null) return null;
        Builder found = null;
        for (Builder source : sources) {
            Query.Reference reference = source.reference;
            Table qualifier = column.getTable();
            boolean matches;
            if (qualifier == null) matches = singleSource(reference.select()) == reference.table();
            else if (reference.table().getAlias() != null) {
                matches = qualifier.getSchemaName() == null && Ascii.equalsIgnoreCase(qualifier.getUnquotedName(),
                        reference.table().getAlias().getUnquotedName());
            } else {
                matches = qualifier.getDatabaseName() == null
                        && (qualifier.getSchemaName() == null || Ascii.equalsIgnoreCase(qualifier.getUnquotedSchemaName(), "main"))
                        && Ascii.equalsIgnoreCase(qualifier.getUnquotedName(), reference.table().getUnquotedName());
            }
            if (matches) {
                if (found != null) return null;
                found = source;
            }
        }
        return found == null ? null : new Target(found, axis);
    }

    private static FromItem singleSource(PlainSelect select) {
        if (select.getJoins() != null && !select.getJoins().isEmpty()) return null;
        FromItem source = select.getFromItem();
        while (source instanceof ParenthesedFromItem group) {
            if (group.getJoins() != null && !group.getJoins().isEmpty()) return null;
            source = group.getFromItem();
        }
        return source;
    }

    /** Only the outer, row-preserving single-topic SELECT can replace its default window with a tail. */
    private static OptionalLong tail(Query query, Builder source, Connection sqlite) throws SQLException {
        PlainSelect select = source.reference.select();
        if (query.select() != select || singleSource(select) != source.reference.table()
                || select.getWithItemsList() != null || select.getDistinct() != null || select.getGroupBy() != null
                || select.getHaving() != null || select.getQualify() != null || select.getWindowDefinitions() != null
                || select.getOffset() != null || select.getFetch() != null || select.getLimitBy() != null) return OptionalLong.empty();
        var limit = select.getLimit();
        if (limit == null || limit.getOffset() != null || !(limit.getRowCount() instanceof LongValue n)
                || n.getBigIntegerValue().signum() < 0 || n.getBigIntegerValue().bitLength() > 63) return OptionalLong.empty();
        var ordering = select.getOrderByElements();
        if (ordering == null || ordering.size() != 1 || ordering.getFirst().isAsc()) return OptionalLong.empty();
        var order = ordering.getFirst().getExpression();
        Target target = target(order, List.of(source));
        if (target == null || target.axis() != Axis.OFFSET) return OptionalLong.empty();
        // SQLite resolves an unqualified ORDER BY against output aliases before source columns.
        if (((Column) unwrap(order)).getTable() == null && select.getSelectItems().stream().anyMatch(item ->
                item.getAlias() != null && Ascii.equalsIgnoreCase(item.getAlias().getUnquotedName(), "_offset"))) return OptionalLong.empty();
        var projection = new Projection();
        for (var item : select.getSelectItems()) item.getExpression().accept(projection, null);
        if (!projection.rowLocal) return OptionalLong.empty();
        if (!projection.functions.isEmpty()) {
            // SQLite knows its aggregate overloads, including application-registered functions.
            try (var p = sqlite.prepareStatement("SELECT 1 FROM pragma_function_list WHERE name=? COLLATE NOCASE "
                    + "AND type<>'s' AND (narg=? OR narg=-1) LIMIT 1")) {
                for (Function function : projection.functions) {
                    p.setString(1, new Column(function.getName()).getUnquotedColumnName());
                    p.setInt(2, function.getParameters() == null ? 0 : function.getParameters().size());
                    try (var rows = p.executeQuery()) { if (rows.next()) return OptionalLong.empty(); }
                }
            }
        }
        return OptionalLong.of(n.getValue());
    }

    private static final class Projection extends ExpressionVisitorAdapter<Void> {
        boolean rowLocal = true;
        final List<Function> functions = new ArrayList<>();
        @Override public <S> Void visit(Function function, S context) {
            functions.add(function); return super.visit(function, context);
        }
        @Override public <S> Void visit(AnalyticExpression expression, S context) { rowLocal = false; return null; }
        @Override public <S> Void visit(JsonAggregateFunction expression, S context) { rowLocal = false; return null; }
        @Override public <S> Void visit(Select expression, S context) { rowLocal = false; return null; }
        @Override public <S> Void visit(ParenthesedSelect expression, S context) { rowLocal = false; return null; }
    }

    private static final class Constant extends ExpressionVisitorAdapter<Void> {
        boolean valid = true;
        final Set<Integer> parameters = new LinkedHashSet<>();
        // Comparisons/logical operators can group differently in SQLite (notably BETWEEN ... = TRUE).
        // Fold only scalar arithmetic/functions and literals; leave other operands entirely as written.
        @Override protected <S> Void visitExpressions(Expression expression, S context, Collection<Expression> children) {
            if (!(expression instanceof Addition || expression instanceof Subtraction || expression instanceof Multiplication
                    || expression instanceof Division || expression instanceof Modulo || expression instanceof Function
                    || expression instanceof ExpressionList<?>)) {
                valid = false;
                return null;
            }
            return super.visitExpressions(expression, context, children);
        }
        @Override protected <S> Void applyExpression(Expression expression, S context) {
            if (!(expression instanceof LongValue || expression instanceof DoubleValue
                    || expression instanceof StringValue || expression instanceof NullValue)) valid = false;
            return null;
        }
        @Override public <S> Void visit(NotExpression expression, S context) { valid = false; return null; }
        @Override public <S> Void visit(IsBooleanExpression expression, S context) { valid = false; return null; }
        @Override public <S> Void visit(IsNullExpression expression, S context) { valid = false; return null; }
        @Override public <S> Void visit(IsUnknownExpression expression, S context) { valid = false; return null; }
        @Override public <S> Void visit(JsonExpression expression, S context) { valid = false; return null; }
        @Override public <S> Void visit(CollateExpression expression, S context) { valid = false; return null; }
        @Override public <S> Void visit(Column column, S context) { valid = false; return null; }
        @Override public <S> Void visit(Select select, S context) { valid = false; return null; }
        @Override public <S> Void visit(ParenthesedSelect select, S context) { valid = false; return null; }
        @Override public <S> Void visit(JdbcNamedParameter parameter, S context) { valid = false; return null; }
        @Override public <S> Void visit(NumericBind parameter, S context) { valid = false; return null; }
        @Override public <S> Void visit(UserVariable parameter, S context) { valid = false; return null; }
        @Override public <S> Void visit(JdbcParameter parameter, S context) {
            parameters.add(parameter.getIndex());
            return null;
        }
    }

    /** SQLite's integer metadata columns can be compared with REAL constants. Round only the cut point,
     * never an expression. Non-numeric values keep an unrestricted hint rather than assume SQL coercion.
     */
    private static RangeSet<Long> range(Op op, Object value) {
        if (value == null) return ImmutableRangeSet.of();
        if (!(value instanceof Number number) || (number instanceof Double d && !Double.isFinite(d))) return ALL;
        BigDecimal n = number instanceof Double d ? new BigDecimal(d) : BigDecimal.valueOf(number.longValue());
        if (op == Op.EQ && n.stripTrailingZeros().scale() > 0) return ImmutableRangeSet.of();
        n = n.setScale(0, op == Op.GE || op == Op.LT ? RoundingMode.CEILING : RoundingMode.FLOOR);
        if (n.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0) return op == Op.GE || op == Op.GT ? ALL : ImmutableRangeSet.of();
        if (n.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) return op == Op.LE || op == Op.LT ? ALL : ImmutableRangeSet.of();
        long endpoint = n.longValueExact();
        return ImmutableRangeSet.of(switch (op) {
            case EQ -> Range.singleton(endpoint);
            case LT -> Range.lessThan(endpoint);
            case LE -> Range.atMost(endpoint);
            case GT -> Range.greaterThan(endpoint);
            case GE -> Range.atLeast(endpoint);
        });
    }
}
