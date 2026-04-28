/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Column;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Eq;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.FilterExpr;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.FilterExprValue;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Gt;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.GtEq;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.InList;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Like;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LiteralBool;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LiteralDouble;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LiteralInt;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LiteralLong;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LiteralString;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LiteralTimestampMillis;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Lt;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.LtEq;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.NotEq;
import org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.NotLike;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * parquet-rs filter pushdown that translates ESQL filter expressions into the
 * FlatBuffers {@code FilterExpr} IPC payload (schema in
 * {@code native/schema/filter_expr.fbs}) consumed by {@link ParquetRsBridge#openReader}.
 * <p>
 * parquet-rs applies RowFilter at the row level during scan, so pushed filters
 * use {@link Pushability#YES}.
 */
public class ParquetRsFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(ParquetRsFilterPushdownSupport.class);

    static final Predicate<DataType> TYPE_SUPPORTED = dt -> dt == DataType.INTEGER
        || dt == DataType.LONG
        || dt == DataType.DOUBLE
        || dt == DataType.KEYWORD
        || dt == DataType.BOOLEAN
        || dt == DataType.DATETIME;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<Expression> pushed = new ArrayList<>();
        List<Expression> remainder = new ArrayList<>();

        for (Expression filter : filters) {
            if (canConvert(filter)) {
                pushed.add(filter);
            } else {
                remainder.add(filter);
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        // The actual FlatBuffer is built on-demand in ParquetRsFormatReader.read(): byte[] is
        // GC-managed, so we don't need a defined owner across the optimizer / per-query / per-file
        // lifecycle (unlike the JNI handle pattern this replaced).
        logger.debug("parquet-rs filter pushdown: accepted {} of {} expressions", pushed.size(), filters.size());
        return new PushdownResult(new ParquetRsPushedFilter(pushed), pushed, remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        if (canConvert(expr)) {
            return Pushability.YES;
        }
        return Pushability.NO;
    }

    static boolean canConvert(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            if (PushdownPredicates.isComparison(bc, TYPE_SUPPORTED) == false) {
                return false;
            }
            if (bc.right() instanceof Literal lit && lit.value() == null) {
                return false;
            }
            if (bc.left() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return bc instanceof Equals || bc instanceof NotEquals;
            }
            return true;
        }
        if (expr instanceof In inExpr) {
            return PushdownPredicates.isIn(inExpr, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNull isNull) {
            return PushdownPredicates.isIsNull(isNull, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNotNull isNotNull) {
            return PushdownPredicates.isIsNotNull(isNotNull, TYPE_SUPPORTED);
        }
        if (expr instanceof Range range) {
            if (range.value() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return false;
            }
            return PushdownPredicates.isRange(range, TYPE_SUPPORTED);
        }
        if (expr instanceof And and) {
            // Both sides must be convertible: PushFiltersToSource removes the FilterExec when the
            // remainder is empty, so the source becomes solely responsible for the predicate.
            // Allowing a partially-convertible And here would force the And translation branch to
            // either drop the non-convertible side (returning too many rows) or fail at execution.
            // Top-level conjuncts are pre-split by Predicates.splitAnd, so this only restricts ANDs
            // nested under Or/Not, where partial pushdown is unsound.
            return canConvert(and.left()) && canConvert(and.right());
        }
        if (expr instanceof Or or) {
            return canConvert(or.left()) && canConvert(or.right());
        }
        if (expr instanceof Not not) {
            return canConvert(not.field());
        }
        if (expr instanceof StartsWith sw) {
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD)
                && sw.prefix() instanceof Literal lit
                && lit.value() != null;
        }
        if (expr instanceof WildcardLike wl) {
            return wl.field() instanceof NamedExpression ne && (ne.dataType() == DataType.KEYWORD || ne.dataType() == DataType.TEXT);
        }
        return false;
    }

    /**
     * Encodes a list of pushed-down ESQL filter expressions (logically AND-ed together) as a
     * {@code FilterExpr} FlatBuffer matching {@code native/schema/filter_expr.fbs}. Returns
     * {@code null} when the list is empty, signaling "no filter" to {@link ParquetRsBridge#openReader}.
     * <p>
     * Every expression passed in must already have been accepted by {@link #canConvert}.
     */
    static byte[] translateToFlatBuffer(List<Expression> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }
        FlatBufferBuilder fbb = new FlatBufferBuilder(256);
        int rootEnv = encodeAllAnded(fbb, expressions);
        fbb.finish(rootEnv);
        return fbb.sizedByteArray();
    }

    /** AND-chain all top-level expressions into a single {@code FilterExpr} envelope offset. */
    private static int encodeAllAnded(FlatBufferBuilder fbb, List<Expression> expressions) {
        int combined = encodeExprRequired(fbb, expressions.get(0));
        for (int i = 1; i < expressions.size(); i++) {
            int next = encodeExprRequired(fbb, expressions.get(i));
            int andTable = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.And.createAnd(fbb, combined, next);
            combined = wrap(fbb, FilterExprValue.And, andTable);
        }
        return combined;
    }

    /**
     * Wraps {@link #encodeExpr} with a contract assertion. A 0 return signals a drift between
     * {@link #canConvert} and {@code encodeExpr} — pushing on regardless would silently drop a
     * filter the optimizer promised the source would apply, producing wrong query results.
     * Failing loudly surfaces the bug in tests instead.
     */
    private static int encodeExprRequired(FlatBufferBuilder fbb, Expression expr) {
        int env = encodeExpr(fbb, expr);
        if (env == 0) {
            throw new IllegalStateException("encodeExpr returned 0 for expression accepted by canConvert: [" + expr + "]");
        }
        return env;
    }

    /** Encode one expression and return the offset of its enclosing {@code FilterExpr} envelope. */
    private static int encodeExpr(FlatBufferBuilder fbb, Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right() instanceof Literal lit) {
            Object value = lit.value();
            if (value == null) {
                return 0;
            }
            int colEnv = encodeColumn(fbb, ne.name());
            int litEnv = encodeLiteral(fbb, ne.dataType(), value);
            if (litEnv == 0) {
                return 0;
            }
            return encodeComparison(fbb, bc, colEnv, litEnv);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return encodeIn(fbb, ne, inExpr.list());
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            int colEnv = encodeColumn(fbb, ne.name());
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.IsNull.createIsNull(fbb, colEnv);
            return wrap(fbb, FilterExprValue.IsNull, t);
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            int colEnv = encodeColumn(fbb, ne.name());
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.IsNotNull.createIsNotNull(fbb, colEnv);
            return wrap(fbb, FilterExprValue.IsNotNull, t);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return encodeRange(fbb, ne, range);
        }
        if (expr instanceof And and) {
            int l = encodeExprRequired(fbb, and.left());
            int r = encodeExprRequired(fbb, and.right());
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.And.createAnd(fbb, l, r);
            return wrap(fbb, FilterExprValue.And, t);
        }
        if (expr instanceof Or or) {
            int l = encodeExprRequired(fbb, or.left());
            int r = encodeExprRequired(fbb, or.right());
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Or.createOr(fbb, l, r);
            return wrap(fbb, FilterExprValue.Or, t);
        }
        if (expr instanceof Not not) {
            int inner = encodeExprRequired(fbb, not.field());
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.Not.createNot(fbb, inner);
            return wrap(fbb, FilterExprValue.Not, t);
        }
        if (expr instanceof WildcardLike wl && wl.field() instanceof NamedExpression ne) {
            int colEnv = encodeColumn(fbb, ne.name());
            int patOff = fbb.createString(esqlWildcardToSqlLike(wl.pattern().pattern()));
            int t = Like.createLike(fbb, colEnv, patOff);
            return wrap(fbb, FilterExprValue.Like, t);
        }
        if (expr instanceof StartsWith sw
            && sw.singleValueField() instanceof NamedExpression ne
            && sw.prefix() instanceof Literal prefixLit) {
            if (prefixLit.value() == null) {
                return 0;
            }
            BytesRef prefix = (BytesRef) prefixLit.value();
            BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(prefix);
            int colEnv = encodeColumn(fbb, ne.name());
            int prefixOff = fbb.createString(prefix.utf8ToString());
            int upperOff = upper != null ? fbb.createString(upper.utf8ToString()) : 0;
            org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.StartsWith.startStartsWith(fbb);
            org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.StartsWith.addExpr(fbb, colEnv);
            org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.StartsWith.addPrefix(fbb, prefixOff);
            if (upperOff != 0) {
                org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.StartsWith.addUpperBound(fbb, upperOff);
            }
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.StartsWith.endStartsWith(fbb);
            return wrap(fbb, FilterExprValue.StartsWith, t);
        }
        return 0;
    }

    private static int encodeColumn(FlatBufferBuilder fbb, String name) {
        int nameOff = fbb.createString(name);
        int t = Column.createColumn(fbb, nameOff);
        return wrap(fbb, FilterExprValue.Column, t);
    }

    private static int encodeLiteral(FlatBufferBuilder fbb, DataType dataType, Object value) {
        return switch (dataType) {
            case INTEGER -> wrap(fbb, FilterExprValue.LiteralInt, LiteralInt.createLiteralInt(fbb, ((Number) value).intValue()));
            case LONG -> wrap(fbb, FilterExprValue.LiteralLong, LiteralLong.createLiteralLong(fbb, ((Number) value).longValue()));
            case DATETIME -> wrap(
                fbb,
                FilterExprValue.LiteralTimestampMillis,
                LiteralTimestampMillis.createLiteralTimestampMillis(fbb, ((Number) value).longValue())
            );
            case DOUBLE -> wrap(fbb, FilterExprValue.LiteralDouble, LiteralDouble.createLiteralDouble(fbb, ((Number) value).doubleValue()));
            case BOOLEAN -> wrap(fbb, FilterExprValue.LiteralBool, LiteralBool.createLiteralBool(fbb, (Boolean) value));
            case KEYWORD -> {
                String s = (value instanceof BytesRef br) ? br.utf8ToString() : value.toString();
                int sOff = fbb.createString(s);
                yield wrap(fbb, FilterExprValue.LiteralString, LiteralString.createLiteralString(fbb, sOff));
            }
            default -> 0;
        };
    }

    private static int encodeComparison(FlatBufferBuilder fbb, EsqlBinaryComparison bc, int leftEnv, int rightEnv) {
        return switch (bc) {
            case Equals ignored -> wrap(fbb, FilterExprValue.Eq, Eq.createEq(fbb, leftEnv, rightEnv));
            case NotEquals ignored -> wrap(fbb, FilterExprValue.NotEq, NotEq.createNotEq(fbb, leftEnv, rightEnv));
            case GreaterThan ignored -> wrap(fbb, FilterExprValue.Gt, Gt.createGt(fbb, leftEnv, rightEnv));
            case GreaterThanOrEqual ignored -> wrap(fbb, FilterExprValue.GtEq, GtEq.createGtEq(fbb, leftEnv, rightEnv));
            case LessThan ignored -> wrap(fbb, FilterExprValue.Lt, Lt.createLt(fbb, leftEnv, rightEnv));
            case LessThanOrEqual ignored -> wrap(fbb, FilterExprValue.LtEq, LtEq.createLtEq(fbb, leftEnv, rightEnv));
            default -> 0;
        };
    }

    private static int encodeIn(FlatBufferBuilder fbb, NamedExpression ne, List<Expression> items) {
        List<Integer> itemEnvs = new ArrayList<>();
        for (Expression item : items) {
            if (item instanceof Literal lit && lit.value() != null) {
                int env = encodeLiteral(fbb, ne.dataType(), lit.value());
                if (env != 0) {
                    itemEnvs.add(env);
                }
            }
        }
        if (itemEnvs.isEmpty()) {
            return 0;
        }
        int colEnv = encodeColumn(fbb, ne.name());
        int[] arr = new int[itemEnvs.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = itemEnvs.get(i);
        }
        int itemsVec = InList.createItemsVector(fbb, arr);
        int t = InList.createInList(fbb, colEnv, itemsVec);
        return wrap(fbb, FilterExprValue.InList, t);
    }

    private static int encodeRange(FlatBufferBuilder fbb, NamedExpression ne, Range range) {
        if ((range.lower() instanceof Literal lowerLit)
            && (range.upper() instanceof Literal upperLit)
            && lowerLit.value() == null
            && upperLit.value() == null) {

            int lower = encodeBound(fbb, ne, lowerLit.value(), range.includeLower(), true);
            if (lower == 0) {
                return 0;
            }
            int upper = encodeBound(fbb, ne, upperLit.value(), range.includeUpper(), false);
            if (upper == 0) {
                return 0;
            }
            int t = org.elasticsearch.xpack.esql.datasource.parquet.parquetrs.fbs.And.createAnd(fbb, lower, upper);
            return wrap(fbb, FilterExprValue.And, t);
        }
        return 0;
    }

    /** Builds one half of a range: {@code col >[=] lit} when {@code isLower}, otherwise {@code col <[=] lit}. */
    private static int encodeBound(FlatBufferBuilder fbb, NamedExpression ne, Object value, boolean inclusive, boolean isLower) {
        int litEnv = encodeLiteral(fbb, ne.dataType(), value);
        if (litEnv == 0) {
            return 0;
        }
        int colEnv = encodeColumn(fbb, ne.name());
        if (isLower) {
            return inclusive
                ? wrap(fbb, FilterExprValue.GtEq, GtEq.createGtEq(fbb, colEnv, litEnv))
                : wrap(fbb, FilterExprValue.Gt, Gt.createGt(fbb, colEnv, litEnv));
        }
        return inclusive
            ? wrap(fbb, FilterExprValue.LtEq, LtEq.createLtEq(fbb, colEnv, litEnv))
            : wrap(fbb, FilterExprValue.Lt, Lt.createLt(fbb, colEnv, litEnv));
    }

    /** Wrap a payload table in the {@code FilterExpr} union envelope and return the envelope offset. */
    private static int wrap(FlatBufferBuilder fbb, byte variant, int payloadOffset) {
        return FilterExpr.createFilterExpr(fbb, variant, payloadOffset);
    }

    /**
     * Translates an ESQL {@code LIKE} pattern into a SQL {@code LIKE} pattern accepted
     * by parquet-rs, mapping wildcards and re-escaping the SQL special characters that
     * ESQL treats as literals:
     * <ul>
     *   <li>{@code *} -> {@code %} (any sequence)</li>
     *   <li>{@code ?} -> {@code _} (any single char)</li>
     *   <li>{@code %} -> {@code \%}, {@code _} -> {@code \_} (literal in ESQL, wildcard in SQL)</li>
     *   <li>{@code \X} (any escaped char) -> {@code \X} verbatim, so the next char is left as-is</li>
     * </ul>
     *
     * <p>Trailing-backslash edge case: an ESQL pattern ending in a single unmatched {@code \}
     * is emitted as {@code \\} (escape an escape) rather than dropping it. This keeps the SQL
     * pattern syntactically valid — a lone trailing {@code \} would otherwise be an incomplete
     * escape sequence in SQL {@code LIKE} — and treats the input symmetrically with how the
     * loop preserves any other escaped sequence.
     */
    static String esqlWildcardToSqlLike(String esqlPattern) {
        StringBuilder sb = new StringBuilder(esqlPattern.length());
        boolean escaped = false;
        for (int i = 0; i < esqlPattern.length(); i++) {
            char c = esqlPattern.charAt(i);
            if (escaped) {
                sb.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                sb.append(c);
                continue;
            }
            switch (c) {
                case '*' -> sb.append('%');
                case '?' -> sb.append('_');
                case '%' -> sb.append("\\%");
                case '_' -> sb.append("\\_");
                default -> sb.append(c);
            }
        }
        if (escaped) {
            // See class-level note on trailing-backslash handling: emit "\\" so the SQL pattern
            // never ends with an incomplete escape sequence.
            sb.append('\\');
        }
        return sb.toString();
    }
}
