/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.SymbolExpression;
import com.hazelcast.sql.impl.expression.datetime.CurrentDateFunction;
import com.hazelcast.sql.impl.expression.datetime.CurrentTimestampFunction;
import com.hazelcast.sql.impl.expression.datetime.DatePartFunction;
import com.hazelcast.sql.impl.expression.datetime.DatePartUnit;
import com.hazelcast.sql.impl.expression.datetime.DatePartUnitConstantExpression;
import com.hazelcast.sql.impl.expression.datetime.LocalTimeFunction;
import com.hazelcast.sql.impl.expression.datetime.LocalTimestampFunction;
import com.hazelcast.sql.impl.expression.math.AbsFunction;
import com.hazelcast.sql.impl.expression.math.Atan2Function;
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.expression.math.DoubleFunction;
import com.hazelcast.sql.impl.expression.math.FloorCeilFunction;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.math.PowerFunction;
import com.hazelcast.sql.impl.expression.math.RandFunction;
import com.hazelcast.sql.impl.expression.math.RemainderFunction;
import com.hazelcast.sql.impl.expression.math.RoundTruncateFunction;
import com.hazelcast.sql.impl.expression.math.SignFunction;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.CaseExpression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.hazelcast.sql.impl.expression.string.AsciiFunction;
import com.hazelcast.sql.impl.expression.string.CharLengthFunction;
import com.hazelcast.sql.impl.expression.string.ConcatFunction;
import com.hazelcast.sql.impl.expression.string.InitcapFunction;
import com.hazelcast.sql.impl.expression.string.LikeFunction;
import com.hazelcast.sql.impl.expression.string.LowerFunction;
import com.hazelcast.sql.impl.expression.string.PositionFunction;
import com.hazelcast.sql.impl.expression.string.ReplaceFunction;
import com.hazelcast.sql.impl.expression.string.SubstringFunction;
import com.hazelcast.sql.impl.expression.string.TrimFunction;
import com.hazelcast.sql.impl.expression.string.UpperFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.util.Calendar;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.CHARACTER_LENGTH;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.CHAR_LENGTH;
import static com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable.LENGTH;

/**
 * Utility methods for REX to Hazelcast expression conversion.
 */
public final class RexToExpression {

    private static final int MILLISECONDS_PER_SECOND = 1_000;
    private static final int NANOSECONDS_PER_MILLISECOND = 1_000_000;

    private RexToExpression() {
        // No-op.
    }

    /**
     * Converts the given REX literal to runtime {@link ConstantExpression
     * constant expression}.
     *
     * @param literal the literal to convert.
     * @return the resulting constant expression.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    public static Expression<?> convertLiteral(RexLiteral literal) {
        SqlTypeName type = literal.getType().getSqlTypeName();

        switch (type) {
            case BOOLEAN:
                return convertBooleanLiteral(literal, type);

            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return convertNumericLiteral(literal, type);

            case CHAR:
            case VARCHAR:
                return convertStringLiteral(literal, type);

            case NULL:
                return ConstantExpression.create(null, QueryDataType.NULL);

            case ANY:
                // currently, the only possible literal of ANY type is NULL
                assert literal.getValueAs(Object.class) == null;
                return ConstantExpression.create(null, QueryDataType.OBJECT);

            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTemporalLiteral(literal, type);

            case INTERVAL_YEAR:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
                return convertIntervalYearMonthLiteral(literal, type);

            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return convertIntervalDaySecondLiteral(literal, type);

            case SYMBOL:
                return convertSymbolLiteral(literal);

            default:
                throw QueryException.error("Unsupported literal: " + literal);
        }
    }

    /**
     * Converts a {@link RexCall} to {@link Expression}.
     *
     * @param call the call to convert.
     * @return the resulting expression.
     * @throws QueryException if the given {@link RexCall} can't be
     *                        converted.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength", "checkstyle:ReturnCount",
        "checkstyle:NPathComplexity"})
    public static Expression<?> convertCall(RexCall call, Expression<?>[] operands) {
        SqlOperator operator = call.getOperator();
        QueryDataType resultType = SqlToQueryType.map(call.getType().getSqlTypeName());

        switch (operator.getKind()) {
            case DEFAULT:
                return ConstantExpression.create(null, resultType);

            case CAST:
                if (operands[0].getType().equals(resultType)) {
                    // It might happen that two types Calcite considers different
                    // are mapped to the same Hazelcast type. For instance, to
                    // preserve the row signature, Calcite may insert synthetic
                    // casts from a non-nullable type to the same nullable type.
                    // Technically, such casts are not 100% valid SQL construct
                    // since casts can't change the nullability of the operand
                    // being casted, but they are valid on the RexNode level.
                    //
                    // Consider nullableBooleanColumn OR TRUE, the type of this
                    // expression is nullable BOOLEAN. When Calcite simplifies it
                    // to TRUE, the type changes to non-nullable BOOLEAN since
                    // literals are non-nullable (except NULL literal itself).
                    // That changes the row signature, to preserve it Calcite
                    // synthetically casts TRUE to a nullable BOOLEAN.
                    //
                    // Currently, all Hazelcast types are nullable, therefore
                    // there is no distinction between non-nullable types and
                    // nullable ones after the conversion.
                    return operands[0];
                }
                return CastExpression.create(operands[0], resultType);

            case AND:
                return AndPredicate.create(operands);

            case OR:
                return OrPredicate.create(operands);

            case NOT:
                return NotPredicate.create(operands[0]);

            case PLUS:
                return PlusFunction.create(operands[0], operands[1], resultType);

            case MINUS:
                return MinusFunction.create(operands[0], operands[1], resultType);

            case TIMES:
                return MultiplyFunction.create(operands[0], operands[1], resultType);

            case DIVIDE:
                return DivideFunction.create(operands[0], operands[1], resultType);

            case MINUS_PREFIX:
                return UnaryMinusFunction.create(operands[0], resultType);

            case PLUS_PREFIX:
                return operands[0];

            case FLOOR:
                return FloorCeilFunction.create(operands[0], resultType, false);

            case CEIL:
                return FloorCeilFunction.create(operands[0], resultType, true);

            case EQUALS:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.EQUALS);

            case NOT_EQUALS:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.NOT_EQUALS);

            case GREATER_THAN:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.GREATER_THAN);

            case GREATER_THAN_OR_EQUAL:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.GREATER_THAN_OR_EQUAL);

            case LESS_THAN:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.LESS_THAN);

            case LESS_THAN_OR_EQUAL:
                return ComparisonPredicate.create(operands[0], operands[1], ComparisonMode.LESS_THAN_OR_EQUAL);

            case IS_TRUE:
                return IsTruePredicate.create(operands[0]);

            case IS_NOT_TRUE:
                return IsNotTruePredicate.create(operands[0]);

            case IS_FALSE:
                return IsFalsePredicate.create(operands[0]);

            case IS_NOT_FALSE:
                return IsNotFalsePredicate.create(operands[0]);

            case IS_NULL:
                return IsNullPredicate.create(operands[0]);

            case IS_NOT_NULL:
                return IsNotNullPredicate.create(operands[0]);

            case MOD:
                return RemainderFunction.create(operands[0], operands[1]);

            case CASE:
                return CaseExpression.create(operands, resultType);

            case LIKE:
                Expression<?> escape = operands.length == 2 ? null : operands[2];

                return LikeFunction.create(operands[0], operands[1], escape);

            case POSITION:
                Expression<?> position = operands.length == 2 ? null : operands[2];
                return PositionFunction.create(operands[0], operands[1], position);

            case TRIM:
                assert operands.length == 3;
                assert operands[0] instanceof SymbolExpression;

                SqlTrimFunction.Flag trimFlag = ((SymbolExpression) operands[0]).getSymbol();

                return TrimFunction.create(
                    operands[2],
                    operands[1],
                    trimFlag.getLeft() == 1,
                    trimFlag.getRight() == 1
                );

            case OTHER:
                if (operator == HazelcastSqlOperatorTable.CONCAT) {
                    assert operands.length == 2;

                    return ConcatFunction.create(operands[0], operands[1]);
                }

                break;

            case EXTRACT:
                DatePartUnit unit = ((DatePartUnitConstantExpression) operands[0]).getUnit();
                return DatePartFunction.create(operands[1], unit);

            case TIMESTAMP_ADD:
                // TODO
                return null;

            case OTHER_FUNCTION:
                SqlFunction function = (SqlFunction) operator;

                // Math.

                if (function == HazelcastSqlOperatorTable.COS) {
                    return DoubleFunction.create(operands[0], DoubleFunction.COS);
                } else if (function == HazelcastSqlOperatorTable.SIN) {
                    return DoubleFunction.create(operands[0], DoubleFunction.SIN);
                } else if (function == HazelcastSqlOperatorTable.TAN) {
                    return DoubleFunction.create(operands[0], DoubleFunction.TAN);
                } else if (function == HazelcastSqlOperatorTable.COT) {
                    return DoubleFunction.create(operands[0], DoubleFunction.COT);
                } else if (function == HazelcastSqlOperatorTable.ACOS) {
                    return DoubleFunction.create(operands[0], DoubleFunction.ACOS);
                } else if (function == HazelcastSqlOperatorTable.ASIN) {
                    return DoubleFunction.create(operands[0], DoubleFunction.ASIN);
                } else if (function == HazelcastSqlOperatorTable.ATAN) {
                    return DoubleFunction.create(operands[0], DoubleFunction.ATAN);
                } else if (function == SqlStdOperatorTable.SQRT) {
                    throw new UnsupportedOperationException("SQRT is not supported yet");
                } else if (function == HazelcastSqlOperatorTable.EXP) {
                    return DoubleFunction.create(operands[0], DoubleFunction.EXP);
                } else if (function == HazelcastSqlOperatorTable.LN) {
                    return DoubleFunction.create(operands[0], DoubleFunction.LN);
                } else if (function == HazelcastSqlOperatorTable.LOG10) {
                    return DoubleFunction.create(operands[0], DoubleFunction.LOG10);
                } else if (function == HazelcastSqlOperatorTable.RAND) {
                    return RandFunction.create(operands.length == 0 ? null : operands[0]);
                } else if (function == HazelcastSqlOperatorTable.ABS) {
                    return AbsFunction.create(operands[0], resultType);
                } else if (function == SqlStdOperatorTable.PI) {
                    return ConstantExpression.create(Math.PI, resultType);
                } else if (function == HazelcastSqlOperatorTable.SIGN) {
                    return SignFunction.create(operands[0], resultType);
                } else if (function == SqlStdOperatorTable.ATAN2) {
                    return Atan2Function.create(operands[0], operands[1]);
                } else if (function == SqlStdOperatorTable.POWER) {
                    return PowerFunction.create(operands[0], operands[1]);
                } else if (function == HazelcastSqlOperatorTable.DEGREES) {
                    return DoubleFunction.create(operands[0], DoubleFunction.DEGREES);
                } else if (function == HazelcastSqlOperatorTable.RADIANS) {
                    return DoubleFunction.create(operands[0], DoubleFunction.RADIANS);
                } else if (function == HazelcastSqlOperatorTable.ROUND) {
                    return RoundTruncateFunction.create(
                        operands[0],
                        operands.length == 1 ? null : operands[1],
                        resultType,
                        false
                    );
                } else if (function == HazelcastSqlOperatorTable.TRUNCATE) {
                    return RoundTruncateFunction.create(
                        operands[0],
                        operands.length == 1 ? null : operands[1],
                        resultType,
                        true
                    );
                }

                // Strings.

                if (function == CHAR_LENGTH || function == CHARACTER_LENGTH || function == LENGTH) {
                    return CharLengthFunction.create(operands[0]);
                } else if (function == HazelcastSqlOperatorTable.UPPER) {
                    return UpperFunction.create(operands[0]);
                } else if (function == HazelcastSqlOperatorTable.LOWER) {
                    return LowerFunction.create(operands[0]);
                } else if (function == HazelcastSqlOperatorTable.INITCAP) {
                    return InitcapFunction.create(operands[0]);
                } else if (function == HazelcastSqlOperatorTable.ASCII) {
                    return AsciiFunction.create(operands[0]);
                } else if (function == HazelcastSqlOperatorTable.SUBSTRING) {
                    Expression<?> input = operands[0];
                    Expression<?> start = operands[1];
                    Expression<?> length = operands.length > 2 ? operands[2] : null;

                    return SubstringFunction.create(input, start, length);
                } else if (function == HazelcastSqlOperatorTable.LTRIM) {
                    return TrimFunction.create(operands[0], null, true, false);
                } else if (function == HazelcastSqlOperatorTable.RTRIM) {
                    return TrimFunction.create(operands[0], null, false, true);
                } else if (function == HazelcastSqlOperatorTable.BTRIM) {
                    return TrimFunction.create(operands[0], null, true, true);
                } else if (function == SqlStdOperatorTable.REPLACE) {
                    return ReplaceFunction.create(operands[0], operands[1], operands[2]);
                }

                // Dates.

                if (function == SqlStdOperatorTable.CURRENT_DATE) {
                    return new CurrentDateFunction();
                } else if (function == SqlStdOperatorTable.CURRENT_TIMESTAMP) {
                    return CurrentTimestampFunction.create(operands.length == 0 ? null : operands[0]);
                } else if (function == SqlStdOperatorTable.LOCALTIMESTAMP) {
                    return LocalTimestampFunction.create(operands.length == 0 ? null : operands[0]);
                } else if (function == SqlStdOperatorTable.LOCALTIME) {
                    return LocalTimeFunction.create(operands.length == 0 ? null : operands[0]);
                }

                break;

            default:
                break;
        }

        throw QueryException.error("Unsupported operator: " + operator);
    }

    private static Expression<?> convertBooleanLiteral(RexLiteral literal, SqlTypeName type) {
        assert type == SqlTypeName.BOOLEAN;
        Boolean value = literal.getValueAs(Boolean.class);
        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertNumericLiteral(RexLiteral literal, SqlTypeName type) {
        Object value;
        switch (type) {
            case TINYINT:
                value = literal.getValueAs(Byte.class);
                break;

            case SMALLINT:
                value = literal.getValueAs(Short.class);
                break;

            case INTEGER:
                value = literal.getValueAs(Integer.class);
                break;

            case BIGINT:
                // XXX: Calcite returns unscaled value of the internally stored
                // BigDecimal if a long value is requested on the literal.
                BigDecimal decimalValue = literal.getValueAs(BigDecimal.class);
                value = decimalValue == null ? null : decimalValue.longValue();
                break;

            case DECIMAL:
                value = literal.getValueAs(BigDecimal.class);
                break;

            case REAL:
                value = literal.getValueAs(Float.class);
                break;

            case DOUBLE:
                value = literal.getValueAs(Double.class);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertStringLiteral(RexLiteral literal, SqlTypeName type) {
        Object value;
        switch (type) {
            case CHAR:
            case VARCHAR:
                value = literal.getValueAs(String.class);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertTemporalLiteral(RexLiteral literal, SqlTypeName type) {
        Calendar calendar = literal.getValueAs(Calendar.class);
        CalendarConverter converter = CalendarConverter.INSTANCE;

        Object value;
        switch (type) {
            case DATE:
                value = converter.asDate(calendar);
                break;

            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                value = converter.asTime(calendar);
                break;

            case TIMESTAMP:
                value = converter.asTimestamp(calendar);
                break;

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                value = converter.asTimestampWithTimezone(calendar);
                break;

            default:
                throw new IllegalArgumentException("Unsupported literal type: " + type);
        }

        return ConstantExpression.create(value, SqlToQueryType.map(type));
    }

    private static Expression<?> convertIntervalYearMonthLiteral(RexLiteral literal, SqlTypeName type) {
        int months = literal.getValueAs(Integer.class);
        return ConstantExpression.create(new SqlYearMonthInterval(months), SqlToQueryType.map(type));
    }

    private static Expression<?> convertIntervalDaySecondLiteral(RexLiteral literal, SqlTypeName type) {
        long value = literal.getValueAs(Long.class);

        long seconds = value / MILLISECONDS_PER_SECOND;
        int nanoseconds = (int) (value % MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND;

        return ConstantExpression.create(new SqlDaySecondInterval(seconds, nanoseconds), SqlToQueryType.map(type));
    }

    private static Expression<?> convertSymbolLiteral(RexLiteral literal) {
        Object value = literal.getValue();

        if (value instanceof TimeUnitRange) {
            TimeUnitRange range = (TimeUnitRange) value;

            DatePartUnit unit;
            switch (range) {
                case YEAR:
                    unit = DatePartUnit.YEAR;
                    break;

                case QUARTER:
                    unit = DatePartUnit.QUARTER;
                    break;

                case MONTH:
                    unit = DatePartUnit.MONTH;
                    break;

                case WEEK:
                    unit = DatePartUnit.WEEK;
                    break;

                case DOY:
                    unit = DatePartUnit.DAYOFYEAR;

                    break;

                case DOW:
                    unit = DatePartUnit.DAYOFWEEK;
                    break;

                case DAY:
                    unit = DatePartUnit.DAYOFMONTH;
                    break;

                case HOUR:
                    unit = DatePartUnit.HOUR;
                    break;

                case MINUTE:
                    unit = DatePartUnit.MINUTE;
                    break;

                case SECOND:
                    unit = DatePartUnit.SECOND;
                    break;

                default:
                    throw QueryException.error("Unsupported literal symbol: " + literal);
            }

            return new DatePartUnitConstantExpression(unit);
        }

        return SymbolExpression.create(literal.getValue());
    }

}
