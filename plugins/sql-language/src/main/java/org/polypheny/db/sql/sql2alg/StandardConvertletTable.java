/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.sql2alg;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCallBinding;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexRangeRef;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlIntervalLiteral;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlJdbcFunctionCall;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlNumericLiteral;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.language.fun.SqlBetweenOperator;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.sql.language.fun.SqlDatetimeSubtractionOperator;
import org.polypheny.db.sql.language.fun.SqlExtractFunction;
import org.polypheny.db.sql.language.fun.SqlItemOperator;
import org.polypheny.db.sql.language.fun.SqlLiteralChainOperator;
import org.polypheny.db.sql.language.fun.SqlMapValueConstructor;
import org.polypheny.db.sql.language.fun.SqlMultisetQueryConstructor;
import org.polypheny.db.sql.language.fun.SqlMultisetValueConstructor;
import org.polypheny.db.sql.language.fun.SqlOverlapsOperator;
import org.polypheny.db.sql.language.fun.SqlRowOperator;
import org.polypheny.db.sql.language.fun.SqlSequenceValueOperator;
import org.polypheny.db.sql.language.fun.SqlTrimFunction;
import org.polypheny.db.sql.language.util.SqlTypeUtil;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter.Blackboard;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.InitializerContext;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.temporal.DateTimeUtils;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * Standard implementation of {@link SqlRexConvertletTable}.
 */
public class StandardConvertletTable extends ReflectiveConvertletTable {

    /**
     * Singleton instance.
     */
    public static final StandardConvertletTable INSTANCE = new StandardConvertletTable();


    private StandardConvertletTable() {
        super();

        // Register aliases (operators which have a different name but identical behavior to other operators).
        addAlias( OperatorRegistry.get( OperatorName.CHARACTER_LENGTH ), OperatorRegistry.get( OperatorName.CHAR_LENGTH ) );
        addAlias( OperatorRegistry.get( OperatorName.IS_UNKNOWN ), OperatorRegistry.get( OperatorName.IS_NULL ) );
        addAlias( OperatorRegistry.get( OperatorName.IS_NOT_UNKNOWN ), OperatorRegistry.get( OperatorName.IS_NOT_NULL ) );
        addAlias( OperatorRegistry.get( OperatorName.PERCENT_REMAINDER ), OperatorRegistry.get( OperatorName.MOD ) );

        // Register convertlets for specific objects.
        registerOp( OperatorRegistry.get( OperatorName.CAST ), this::convertCast );
        registerOp( OperatorRegistry.get( OperatorName.IS_DISTINCT_FROM ), ( cx, call ) -> convertIsDistinctFrom( cx, call, false ) );
        registerOp( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ), ( cx, call ) -> convertIsDistinctFrom( cx, call, true ) );

        registerOp( OperatorRegistry.get( OperatorName.PLUS ), this::convertPlus );

        registerOp(
                OperatorRegistry.get( OperatorName.MINUS ),
                ( cx, call ) -> {
                    final RexCall e = (RexCall) StandardConvertletTable.this.convertCall( cx, call, (SqlOperator) call.getOperator() );
                    return switch ( e.getOperands().get( 0 ).getType().getPolyType() ) {
                        case DATE, TIME, TIMESTAMP -> convertDatetimeMinus( cx, OperatorRegistry.get( OperatorName.MINUS_DATE, SqlDatetimeSubtractionOperator.class ), call );
                        default -> e;
                    };
                } );

        registerOp( OracleSqlOperatorTable.LTRIM, new TrimConvertlet( SqlTrimFunction.Flag.LEADING ) );
        registerOp( OracleSqlOperatorTable.RTRIM, new TrimConvertlet( SqlTrimFunction.Flag.TRAILING ) );

        registerOp( OracleSqlOperatorTable.GREATEST, new GreatestConvertlet() );
        registerOp( OracleSqlOperatorTable.LEAST, new GreatestConvertlet() );

        registerOp(
                OracleSqlOperatorTable.NVL,
                ( cx, call ) -> {
                    final RexBuilder rexBuilder = cx.getRexBuilder();
                    final RexNode operand0 = cx.convertExpression( call.getSqlOperandList().get( 0 ) );
                    final RexNode operand1 = cx.convertExpression( call.getSqlOperandList().get( 1 ) );
                    final AlgDataType type = cx.getValidator().getValidatedNodeType( call );
                    return rexBuilder.makeCall(
                            type,
                            OperatorRegistry.get( OperatorName.CASE ),
                            ImmutableList.of(
                                    rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), operand0 ),
                                    rexBuilder.makeCast( type, operand0 ),
                                    rexBuilder.makeCast( type, operand1 ) ) );
                } );

        registerOp(
                OracleSqlOperatorTable.DECODE,
                ( cx, call ) -> {
                    final RexBuilder rexBuilder = cx.getRexBuilder();
                    final List<RexNode> operands = convertExpressionList( cx, call.getSqlOperandList(), PolyOperandTypeChecker.Consistency.NONE );
                    final AlgDataType type = cx.getValidator().getValidatedNodeType( call );
                    final List<RexNode> exprs = new ArrayList<>();
                    for ( int i = 1; i < operands.size() - 1; i += 2 ) {
                        exprs.add( AlgOptUtil.isDistinctFrom( rexBuilder, operands.get( 0 ), operands.get( i ), true ) );
                        exprs.add( operands.get( i + 1 ) );
                    }
                    if ( operands.size() % 2 == 0 ) {
                        exprs.add( Util.last( operands ) );
                    } else {
                        exprs.add( rexBuilder.makeNullLiteral( type ) );
                    }
                    return rexBuilder.makeCall( type, OperatorRegistry.get( OperatorName.CASE ), exprs );
                } );

        // Expand "x NOT LIKE y" into "NOT (x LIKE y)"
        registerOp(
                OperatorRegistry.get( OperatorName.NOT_LIKE ),
                ( cx, call ) -> cx.convertExpression(
                        (SqlNode) OperatorRegistry.get( OperatorName.NOT ).createCall(
                                ParserPos.ZERO,
                                OperatorRegistry.get( OperatorName.LIKE ).createCall( ParserPos.ZERO, call.getOperandList() ) ) ) );

        // Expand "x NOT SIMILAR y" into "NOT (x SIMILAR y)"
        registerOp(
                OperatorRegistry.get( OperatorName.NOT_SIMILAR_TO ),
                ( cx, call ) -> cx.convertExpression(
                        (SqlNode) OperatorRegistry.get( OperatorName.NOT ).createCall(
                                ParserPos.ZERO,
                                OperatorRegistry.get( OperatorName.SIMILAR_TO ).createCall( ParserPos.ZERO, call.getOperandList() ) ) ) );

        // Unary "+" has no effect, so expand "+ x" into "x".
        registerOp(
                OperatorRegistry.get( OperatorName.UNARY_PLUS ),
                ( cx, call ) -> cx.convertExpression( call.operand( 0 ) ) );

        // "DOT"
        registerOp(
                OperatorRegistry.get( OperatorName.DOT ),
                ( cx, call ) -> cx.getRexBuilder().makeFieldAccess(
                        cx.convertExpression( call.operand( 0 ) ),
                        call.operand( 1 ).toString(), false ) );
        // "AS" has no effect, so expand "x AS id" into "x".
        registerOp( OperatorRegistry.get( OperatorName.AS ), ( cx, call ) -> cx.convertExpression( call.operand( 0 ) ) );
        // "SQRT(x)" is equivalent to "POWER(x, .5)"
        registerOp(
                OperatorRegistry.get( OperatorName.SQRT ),
                ( cx, call ) -> cx.convertExpression(
                        (SqlNode) OperatorRegistry.get( OperatorName.POWER ).createCall(
                                ParserPos.ZERO,
                                call.operand( 0 ),
                                SqlLiteral.createExactNumeric( "0.5", ParserPos.ZERO ) ) ) );

        // Convert json_value('{"foo":"bar"}', 'lax $.foo', returning varchar(2000))
        // to cast(json_value('{"foo":"bar"}', 'lax $.foo') as varchar(2000))
        registerOp(
                (SqlOperator) OperatorRegistry.get( OperatorName.JSON_VALUE ),
                ( cx, call ) -> {
                    SqlNode expanded =
                            (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall(
                                    ParserPos.ZERO,
                                    OperatorRegistry.get( OperatorName.JSON_VALUE_ANY ).createCall(
                                            ParserPos.ZERO,
                                            call.operand( 0 ),
                                            call.operand( 1 ),
                                            call.operand( 2 ),
                                            call.operand( 3 ),
                                            call.operand( 4 ),
                                            null ),
                                    call.operand( 5 ) );
                    return cx.convertExpression( expanded );
                } );

        // REVIEW jvs 24-Apr-2006: This only seems to be working from within a windowed agg.  I have added an optimizer rule org.polypheny.db.alg.rules.AggregateReduceFunctionsRule which handles other cases post-translation.  The reason I did that was to defer the
        // implementation decision; e.g. we may want to push it down to a foreign server directly rather than decomposed; decomposition is easier than recognition.

        // Convert "avg(<expr>)" to "cast(sum(<expr>) / count(<expr>) as <type>)". We don't need to handle the empty set specially, because the SUM is already supposed to come out as NULL in cases where the COUNT is zero, so the null check should take place first and prevent
        // division by zero. We need the cast because SUM and COUNT may use different types, say BIGINT.
        //
        // Similarly STDDEV_POP and STDDEV_SAMP, VAR_POP and VAR_SAMP.
        registerOp( OperatorRegistry.get( OperatorName.AVG ), new AvgVarianceConvertlet( Kind.AVG ) );
        registerOp( OperatorRegistry.get( OperatorName.STDDEV_POP ), new AvgVarianceConvertlet( Kind.STDDEV_POP ) );
        registerOp( OperatorRegistry.get( OperatorName.STDDEV_SAMP ), new AvgVarianceConvertlet( Kind.STDDEV_SAMP ) );
        registerOp( OperatorRegistry.get( OperatorName.STDDEV ), new AvgVarianceConvertlet( Kind.STDDEV_SAMP ) );
        registerOp( OperatorRegistry.get( OperatorName.VAR_POP ), new AvgVarianceConvertlet( Kind.VAR_POP ) );
        registerOp( OperatorRegistry.get( OperatorName.VAR_SAMP ), new AvgVarianceConvertlet( Kind.VAR_SAMP ) );
        registerOp( OperatorRegistry.get( OperatorName.VARIANCE ), new AvgVarianceConvertlet( Kind.VAR_SAMP ) );
        registerOp( OperatorRegistry.get( OperatorName.COVAR_POP ), new RegrCovarianceConvertlet( Kind.COVAR_POP ) );
        registerOp( OperatorRegistry.get( OperatorName.COVAR_SAMP ), new RegrCovarianceConvertlet( Kind.COVAR_SAMP ) );
        registerOp( OperatorRegistry.get( OperatorName.REGR_SXX ), new RegrCovarianceConvertlet( Kind.REGR_SXX ) );
        registerOp( OperatorRegistry.get( OperatorName.REGR_SYY ), new RegrCovarianceConvertlet( Kind.REGR_SYY ) );

        final SqlRexConvertlet floorCeilConvertlet = new FloorCeilConvertlet();
        registerOp( OperatorRegistry.get( OperatorName.FLOOR ), floorCeilConvertlet );
        registerOp( OperatorRegistry.get( OperatorName.CEIL ), floorCeilConvertlet );

        registerOp( OperatorRegistry.get( OperatorName.TIMESTAMP_ADD ), new TimestampAddConvertlet() );
        registerOp( OperatorRegistry.get( OperatorName.TIMESTAMP_DIFF ), new TimestampDiffConvertlet() );

        // Convert "element(<expr>)" to "$element_slice(<expr>)", if the expression is a multiset of scalars.
        if ( false ) {
            registerOp(
                    OperatorRegistry.get( OperatorName.ELEMENT ),
                    ( cx, call ) -> {
                        assert call.operandCount() == 1;
                        final SqlNode operand = call.operand( 0 );
                        final AlgDataType type = cx.getValidator().getValidatedNodeType( operand );
                        if ( !type.getComponentType().isStruct() ) {
                            return cx.convertExpression( (SqlNode) OperatorRegistry.get( OperatorName.ELEMENT_SLICE ).createCall( ParserPos.ZERO, operand ) );
                        }

                        // fallback on default behavior
                        return StandardConvertletTable.this.convertCall( cx, call );
                    } );
        }

        // Convert "$element_slice(<expr>)" to "element(<expr>).field#0"
        if ( false ) {
            registerOp(
                    OperatorRegistry.get( OperatorName.ELEMENT_SLICE ),
                    ( cx, call ) -> {
                        assert call.operandCount() == 1;
                        final SqlNode operand = call.operand( 0 );
                        final RexNode expr = cx.convertExpression( (SqlNode) OperatorRegistry.get( OperatorName.ELEMENT ).createCall( ParserPos.ZERO, operand ) );
                        return cx.getRexBuilder().makeFieldAccess( expr, 0 );
                    } );
        }
    }


    private RexNode or( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.OR ), a0, a1 );
    }


    private RexNode eq( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), a0, a1 );
    }


    private RexNode ge( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), a0, a1 );
    }


    private RexNode le( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), a0, a1 );
    }


    private RexNode and( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), a0, a1 );
    }


    private static RexNode divideInt( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE_INTEGER ), a0, a1 );
    }


    private RexNode plus( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), a0, a1 );
    }


    private RexNode minus( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), a0, a1 );
    }


    private static RexNode multiply( RexBuilder rexBuilder, RexNode a0, RexNode a1 ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), a0, a1 );
    }


    private RexNode case_( RexBuilder rexBuilder, RexNode... args ) {
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.CASE ), args );
    }

    // SqlNode helpers


    private SqlCall plus( ParserPos pos, SqlNode a0, SqlNode a1 ) {
        return (SqlCall) OperatorRegistry.get( OperatorName.PLUS ).createCall( pos, a0, a1 );
    }


    /**
     * Converts a CASE expression.
     */
    public RexNode convertCase( SqlRexContext cx, SqlCase call ) {
        SqlNodeList whenList = call.getWhenOperands();
        SqlNodeList thenList = call.getThenOperands();
        assert whenList.size() == thenList.size();

        RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> exprList = new ArrayList<>();
        for ( int i = 0; i < whenList.size(); i++ ) {
            if ( CoreUtil.isNullLiteral( whenList.get( i ), false ) ) {
                exprList.add( rexBuilder.constantNull() );
            } else {
                exprList.add( cx.convertExpression( whenList.getSqlList().get( i ) ) );
            }
            if ( CoreUtil.isNullLiteral( thenList.get( i ), false ) ) {
                exprList.add( rexBuilder.constantNull() );
            } else {
                exprList.add( cx.convertExpression( thenList.getSqlList().get( i ) ) );
            }
        }
        if ( CoreUtil.isNullLiteral( call.getElseOperand(), false ) ) {
            exprList.add( rexBuilder.constantNull() );
        } else {
            exprList.add( cx.convertExpression( call.getElseOperand() ) );
        }

        AlgDataType type = rexBuilder.deriveReturnType( call.getOperator(), exprList );
        for ( int i : elseArgs( exprList.size() ) ) {
            exprList.set( i, rexBuilder.ensureType( type, exprList.get( i ), false ) );
        }
        return rexBuilder.makeCall( type, OperatorRegistry.get( OperatorName.CASE ), exprList );
    }


    public RexNode convertMultiset( SqlRexContext cx, SqlMultisetValueConstructor op, SqlCall call ) {
        final AlgDataType originalType = cx.getValidator().getValidatedNodeType( call );
        RexRangeRef rr = cx.getSubQueryExpr( call );
        assert rr != null;
        AlgDataType msType = rr.getType().getFields().get( 0 ).getType();
        RexNode expr = cx.getRexBuilder().makeInputRef( msType, rr.getOffset() );
        assert msType.getComponentType().isStruct();
        if ( !originalType.getComponentType().isStruct() ) {
            // If the type is not a struct, the multiset operator will have wrapped the type as a record. Add a call to the $SLICE operator to compensate. For example,
            // if '<ms>' has type 'RECORD (INTEGER x) MULTISET', then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
            // This will be removed as the expression is translated.
            expr = cx.getRexBuilder().makeCall( originalType, OperatorRegistry.get( OperatorName.SLICE ), ImmutableList.of( expr ) );
        }
        return expr;
    }


    public RexNode convertArray( SqlRexContext cx, SqlArrayValueConstructor op, SqlCall call ) {
        return convertCall( cx, call );
    }


    public RexNode convertMap( SqlRexContext cx, SqlMapValueConstructor op, SqlCall call ) {
        return convertCall( cx, call );
    }


    public RexNode convertMultisetQuery( SqlRexContext cx, SqlMultisetQueryConstructor op, SqlCall call ) {
        final AlgDataType originalType = cx.getValidator().getValidatedNodeType( call );
        RexRangeRef rr = cx.getSubQueryExpr( call );
        assert rr != null;
        AlgDataType msType = rr.getType().getFields().get( 0 ).getType();
        RexNode expr = cx.getRexBuilder().makeInputRef( msType, rr.getOffset() );
        assert msType.getComponentType().isStruct();
        if ( !originalType.getComponentType().isStruct() ) {
            // If the type is not a struct, the multiset operator will have wrapped the type as a record. Add a call to the $SLICE operator to compensate. For example,
            // if '<ms>' has type 'RECORD (INTEGER x) MULTISET', then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
            // This will be removed as the expression is translated.
            expr = cx.getRexBuilder().makeCall( OperatorRegistry.get( OperatorName.SLICE ), expr );
        }
        return expr;
    }


    public RexNode convertJdbc( SqlRexContext cx, SqlJdbcFunctionCall op, SqlCall call ) {
        // Yuck!! The function definition contains arguments!
        // TODO: adopt a more conventional definition/instance structure
        final SqlCall convertedCall = op.getLookupCall();
        return cx.convertExpression( convertedCall );
    }


    protected RexNode convertCast( SqlRexContext cx, final SqlCall call ) {
        AlgDataTypeFactory typeFactory = cx.getTypeFactory();
        assert call.getKind() == Kind.CAST;
        final SqlNode left = call.operand( 0 );
        final SqlNode right = call.operand( 1 );
        if ( right instanceof SqlIntervalQualifier ) {
            final SqlIntervalQualifier intervalQualifier = (SqlIntervalQualifier) right;
            if ( left instanceof SqlIntervalLiteral ) {
                RexLiteral sourceInterval = (RexLiteral) cx.convertExpression( left );
                Long sourceValue = sourceInterval.value.asInterval().millis;
                RexLiteral castedInterval = cx.getRexBuilder().makeIntervalLiteral( sourceValue, intervalQualifier );
                return castToValidatedType( cx, call, castedInterval );
            } else if ( left instanceof SqlNumericLiteral ) {
                RexLiteral sourceInterval = (RexLiteral) cx.convertExpression( left );
                long sourceValue = sourceInterval.getValue().asNumber().longValue();
                final Long multiplier = intervalQualifier.getUnit().multiplier.longValue();
                sourceValue = sourceValue * multiplier;
                RexLiteral castedInterval = cx.getRexBuilder().makeIntervalLiteral( sourceValue, intervalQualifier );
                return castToValidatedType( cx, call, castedInterval );
            }
            return castToValidatedType( cx, call, cx.convertExpression( left ) );
        }
        SqlDataTypeSpec dataType = (SqlDataTypeSpec) right;
        AlgDataType type = dataType.deriveType( typeFactory );
        if ( CoreUtil.isNullLiteral( left, false ) ) {
            final SqlValidatorImpl validator = (SqlValidatorImpl) cx.getValidator();
            validator.setValidatedNodeType( left, type );
            return cx.convertExpression( left );
        }
        RexNode arg = cx.convertExpression( left );
        if ( type == null ) {
            type = cx.getValidator().getValidatedNodeType( dataType.getTypeName() );
        }
        if ( arg.getType().isNullable() ) {
            type = typeFactory.createTypeWithNullability( type, true );
        }
        if ( null != dataType.getCollectionsTypeName() ) {
            final AlgDataType argComponentType = arg.getType().getComponentType();
            final AlgDataType componentType = type.getComponentType();
            if ( argComponentType.isStruct() && !componentType.isStruct() ) {
                AlgDataType tt =
                        typeFactory.builder()
                                .add( null, argComponentType.getFields().get( 0 ).getName(), null, componentType )
                                .build();
                tt = typeFactory.createTypeWithNullability( tt, componentType.isNullable() );
                boolean isn = type.isNullable();
                type = typeFactory.createMultisetType( tt, -1 );
                type = typeFactory.createTypeWithNullability( type, isn );
            }
        }
        return cx.getRexBuilder().makeCast( type, arg );
    }


    protected RexNode convertFloorCeil( SqlRexContext cx, SqlCall call ) {
        final boolean floor = call.getKind() == Kind.FLOOR;
        // Rewrite floor, ceil of interval
        if ( call.operandCount() == 1 && call.operand( 0 ) instanceof SqlIntervalLiteral ) {
            final SqlIntervalLiteral literal = call.operand( 0 );
            SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue) literal.getValue();
            BigDecimal val = interval.getIntervalQualifier().getStartUnit().multiplier;
            RexNode rexInterval = cx.convertExpression( literal );

            final RexBuilder rexBuilder = cx.getRexBuilder();
            RexNode zero = rexBuilder.makeExactLiteral( BigDecimal.valueOf( 0 ) );
            RexNode cond = ge( rexBuilder, rexInterval, zero );

            RexNode pad = rexBuilder.makeExactLiteral( val.subtract( BigDecimal.ONE ) );
            RexNode cast = rexBuilder.makeReinterpretCast( rexInterval.getType(), pad, rexBuilder.makeLiteral( false ) );
            RexNode sum = floor
                    ? minus( rexBuilder, rexInterval, cast )
                    : plus( rexBuilder, rexInterval, cast );

            RexNode kase = floor
                    ? case_( rexBuilder, rexInterval, cond, sum )
                    : case_( rexBuilder, sum, cond, rexInterval );

            RexNode factor = rexBuilder.makeExactLiteral( val );
            RexNode div = divideInt( rexBuilder, kase, factor );
            return multiply( rexBuilder, div, factor );
        }

        // normal floor, ceil function
        return convertFunction( cx, (SqlFunction) call.getOperator(), call );
    }


    /**
     * Converts a call to the {@code EXTRACT} function.
     * <p>
     * Called automatically via reflection.
     */
    public RexNode convertExtract( SqlRexContext cx, SqlExtractFunction op, SqlCall call ) {
        return convertFunction( cx, (SqlFunction) call.getOperator(), call );
    }


    private RexNode mod( RexBuilder rexBuilder, AlgDataType resType, RexNode res, BigDecimal val ) {
        if ( val.equals( BigDecimal.ONE ) ) {
            return res;
        }
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MOD ), res, rexBuilder.makeExactLiteral( val, resType ) );
    }


    private static RexNode divide( RexBuilder rexBuilder, RexNode res, BigDecimal val ) {
        if ( val.equals( BigDecimal.ONE ) ) {
            return res;
        }
        // If val is between 0 and 1, rather than divide by val, multiply by its reciprocal. For example, rather than divide by 0.001 multiply by 1000.
        if ( val.compareTo( BigDecimal.ONE ) < 0 && val.signum() == 1 ) {
            try {
                final BigDecimal reciprocal = BigDecimal.ONE.divide( val, RoundingMode.UNNECESSARY );
                return multiply( rexBuilder, res, rexBuilder.makeExactLiteral( reciprocal ) );
            } catch ( ArithmeticException e ) {
                // ignore - reciprocal is not an integer
            }
        }
        return divideInt( rexBuilder, res, rexBuilder.makeExactLiteral( val ) );
    }


    public RexNode convertDatetimeMinus( SqlRexContext cx, SqlDatetimeSubtractionOperator op, SqlCall call ) {
        // Rewrite datetime minus
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<SqlNode> operands = call.getSqlOperandList();
        final List<RexNode> exprs = convertExpressionList( cx, operands, PolyOperandTypeChecker.Consistency.NONE );

        final AlgDataType resType = cx.getValidator().getValidatedNodeType( call );
        return rexBuilder.makeCall( resType, op, exprs.subList( 0, 2 ) );
    }


    public RexNode convertFunction( SqlRexContext cx, SqlFunction fun, SqlCall call ) {
        final List<SqlNode> operands = call.getSqlOperandList();
        final List<RexNode> exprs = convertExpressionList( cx, operands, PolyOperandTypeChecker.Consistency.NONE );
        if ( fun.getFunctionCategory() == FunctionCategory.USER_DEFINED_CONSTRUCTOR ) {
            return makeConstructorCall( cx, fun, exprs );
        }
        AlgDataType returnType = cx.getValidator().getValidatedNodeTypeIfKnown( call );
        if ( returnType == null ) {
            returnType = cx.getRexBuilder().deriveReturnType( fun, exprs );
        }
        return cx.getRexBuilder().makeCall( returnType, fun, exprs );
    }


    public RexNode convertSequenceValue( SqlRexContext cx, SqlSequenceValueOperator fun, SqlCall call ) {
        final List<SqlNode> operands = call.getSqlOperandList();
        assert operands.size() == 1;
        assert operands.get( 0 ) instanceof SqlIdentifier;
        final SqlIdentifier id = (SqlIdentifier) operands.get( 0 );
        final String key = Util.listToString( id.names );
        AlgDataType returnType = cx.getValidator().getValidatedNodeType( call );
        return cx.getRexBuilder().makeCall(
                returnType,
                fun,
                ImmutableList.of( cx.getRexBuilder().makeLiteral( key ) ) );
    }


    public RexNode convertAggregateFunction( SqlRexContext cx, SqlAggFunction fun, SqlCall call ) {
        final List<SqlNode> operands = call.getSqlOperandList();
        final List<RexNode> exprs;
        if ( call.isCountStar() ) {
            exprs = ImmutableList.of();
        } else {
            exprs = convertExpressionList( cx, operands, PolyOperandTypeChecker.Consistency.NONE );
        }
        AlgDataType returnType = cx.getValidator().getValidatedNodeTypeIfKnown( call );
        final int groupCount = cx.getGroupCount();
        if ( returnType == null ) {
            RexCallBinding binding =
                    new RexCallBinding( cx.getTypeFactory(), fun, exprs, ImmutableList.of() ) {
                        @Override
                        public int getGroupCount() {
                            return groupCount;
                        }
                    };
            returnType = fun.inferReturnType( binding );
        }
        return cx.getRexBuilder().makeCall( returnType, fun, exprs );
    }


    private static RexNode makeConstructorCall( SqlRexContext cx, SqlFunction constructor, List<RexNode> exprs ) {
        final RexBuilder rexBuilder = cx.getRexBuilder();
        AlgDataType type = rexBuilder.deriveReturnType( constructor, exprs );

        int n = type.getFieldCount();
        ImmutableList.Builder<RexNode> initializationExprs = ImmutableList.builder();
        final InitializerContext initializerContext = new InitializerContext() {
            @Override
            public RexBuilder getRexBuilder() {
                return rexBuilder;
            }


            @Override
            public RexNode convertExpression( Node e ) {
                throw new UnsupportedOperationException();
            }
        };
        for ( int i = 0; i < n; ++i ) {
            initializationExprs.add(
                    cx.getInitializerExpressionFactory().newAttributeInitializer(
                            type,
                            constructor,
                            i,
                            exprs,
                            initializerContext ) );
        }

        List<RexNode> defaultCasts =
                RexUtil.generateCastExpressions(
                        rexBuilder,
                        type,
                        initializationExprs.build() );

        return rexBuilder.makeNewInvocation( type, defaultCasts );
    }


    /**
     * Converts a call to an operator into a {@link RexCall} to the same operator.
     * <p>
     * Called automatically via reflection.
     *
     * @param cx Context
     * @param call Call
     * @return Rex call
     */
    public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
        return convertCall( cx, call, (SqlOperator) call.getOperator() );
    }


    /**
     * Converts a {@link SqlCall} to a {@link RexCall} with a perhaps different operator.
     */
    private RexNode convertCall( SqlRexContext cx, SqlCall call, SqlOperator op ) {
        final List<SqlNode> operands = call.getSqlOperandList();
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final PolyOperandTypeChecker.Consistency consistency =
                op.getOperandTypeChecker() == null
                        ? PolyOperandTypeChecker.Consistency.NONE
                        : op.getOperandTypeChecker().getConsistency();
        if ( op.getOperatorName() == OperatorName.CROSS_MODEL_ITEM ) {
            RexNode target = cx.convertExpression( call.operand( 0 ) );
            return rexBuilder.makeCall(
                    rexBuilder.getTypeFactory().createPolyType( PolyType.VARCHAR, 255 ),
                    OperatorRegistry.get( OperatorName.CROSS_MODEL_ITEM ),
                    List.of( target, rexBuilder.makeLiteral( ((SqlIdentifier) call.operand( 1 )).names.get( 0 ) ) ) );
        }

        final List<RexNode> exprs = convertExpressionList( cx, operands, consistency );
        AlgDataType type;
        if ( call.getOperator() instanceof SqlItemOperator ) {
            //taking the type from the sql validation instead. This one will be correct, because it looks at chained itemOperators as well. E.g. a[1][1:1][1] should return the array type
            type = call.getOperator().deriveType( cx.getValidator(), ((Blackboard) cx).scope, call );
        } else {
            type = rexBuilder.deriveReturnType( op, exprs );
        }
        if ( type.getPolyType() == PolyType.ARRAY ) {
            return rexBuilder.makeArray( type, makePolyValues( cx, operands ) );
        }

        return rexBuilder.makeCall( type, op, RexUtil.flatten( exprs, op ) );
    }


    private List<PolyValue> makePolyValues( SqlRexContext cx, List<SqlNode> nodes ) {
        final List<PolyValue> exprs = new ArrayList<>();
        for ( SqlNode node : nodes ) {
            exprs.add( toPolyValue( node ) );
        }
        /*if ( exprs.size() > 1 ) {
            final AlgDataType type = consistentType( cx, consistency, RexUtil.types( exprs ) );
            if ( type != null ) {
                final List<RexNode> oldExprs = Lists.newArrayList( exprs );
                exprs.clear();
                for ( RexNode expr : oldExprs ) {
                    exprs.add( cx.getRexBuilder().ensureType( type, expr, true ) );
                }
            }
        }*/
        return exprs;
    }


    private PolyValue toPolyValue( SqlNode node ) {
        return switch ( node.getKind() ) {
            case LITERAL -> ((SqlLiteral) node).getPolyValue();
            case CAST -> PolyValue.convert( toPolyValue( ((SqlCall) node).operand( 0 ) ), ((DataTypeSpec) ((SqlCall) node).operand( 1 )).getType() );
            case ARRAY_VALUE_CONSTRUCTOR -> PolyList.of( ((SqlCall) node).getSqlOperandList().stream().map( this::toPolyValue ).toList() );
            default -> null;
        };
    }


    private List<Integer> elseArgs( int count ) {
        // If list is odd, e.g. [0, 1, 2, 3, 4] we get [1, 3, 4]
        // If list is even, e.g. [0, 1, 2, 3, 4, 5] we get [2, 4, 5]
        final List<Integer> list = new ArrayList<>();
        for ( int i = count % 2; ; ) {
            list.add( i );
            i += 2;
            if ( i >= count ) {
                list.add( i - 1 );
                break;
            }
        }
        return list;
    }


    private static List<RexNode> convertExpressionList( SqlRexContext cx, List<SqlNode> nodes, PolyOperandTypeChecker.Consistency consistency ) {
        final List<RexNode> exprs = new ArrayList<>();
        for ( SqlNode node : nodes ) {
            exprs.add( cx.convertExpression( node ) );
        }
        if ( exprs.size() > 1 ) {
            final AlgDataType type = consistentType( cx, consistency, RexUtil.types( exprs ) );
            if ( type != null ) {
                final List<RexNode> oldExprs = Lists.newArrayList( exprs );
                exprs.clear();
                for ( RexNode expr : oldExprs ) {
                    exprs.add( cx.getRexBuilder().ensureType( type, expr, true ) );
                }
            }
        }
        return exprs;
    }


    private static AlgDataType consistentType( SqlRexContext cx, PolyOperandTypeChecker.Consistency consistency, List<AlgDataType> types ) {
        switch ( consistency ) {
            case COMPARE:
                if ( PolyTypeUtil.areSameFamily( types ) ) {
                    // All arguments are of same family. No need for explicit casts.
                    return null;
                }
                final List<AlgDataType> nonCharacterTypes = new ArrayList<>();
                for ( AlgDataType type : types ) {
                    if ( type.getFamily() != PolyTypeFamily.CHARACTER ) {
                        nonCharacterTypes.add( type );
                    }
                }
                if ( !nonCharacterTypes.isEmpty() ) {
                    final int typeCount = types.size();
                    types = nonCharacterTypes;
                    if ( nonCharacterTypes.size() < typeCount ) {
                        final AlgDataTypeFamily family = nonCharacterTypes.get( 0 ).getFamily();
                        if ( family instanceof PolyTypeFamily ) {
                            // The character arguments might be larger than the numeric argument. Give ourselves some headroom.
                            switch ( (PolyTypeFamily) family ) {
                                case INTEGER:
                                case NUMERIC:
                                    nonCharacterTypes.add( cx.getTypeFactory().createPolyType( PolyType.BIGINT ) );
                            }
                        }
                    }
                }
                // fall through
            case LEAST_RESTRICTIVE:
                return cx.getTypeFactory().leastRestrictive( types );
            default:
                return null;
        }
    }


    private RexNode convertPlus( SqlRexContext cx, SqlCall call ) {
        final RexNode rex = convertCall( cx, call );
        switch ( rex.getType().getPolyType() ) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                // Use special "+" operator for datetime + interval.
                // Re-order operands, if necessary, so that interval is second.
                final RexBuilder rexBuilder = cx.getRexBuilder();
                List<RexNode> operands = ((RexCall) rex).getOperands();
                if ( operands.size() == 2 ) {
                    final PolyType polyType = operands.get( 0 ).getType().getPolyType();
                    if ( Objects.requireNonNull( polyType ) == PolyType.INTERVAL ) {
                        operands = ImmutableList.of( operands.get( 1 ), operands.get( 0 ) );
                    }
                }
                return rexBuilder.makeCall( rex.getType(), OperatorRegistry.get( OperatorName.DATETIME_PLUS ), operands );
            default:
                return rex;
        }
    }


    private RexNode convertIsDistinctFrom( SqlRexContext cx, SqlCall call, boolean neg ) {
        RexNode op0 = cx.convertExpression( call.operand( 0 ) );
        RexNode op1 = cx.convertExpression( call.operand( 1 ) );
        return AlgOptUtil.isDistinctFrom( cx.getRexBuilder(), op0, op1, neg );
    }


    /**
     * Converts a BETWEEN expression.
     * <p>
     * Called automatically via reflection.
     */
    public RexNode convertBetween( SqlRexContext cx, SqlBetweenOperator op, SqlCall call ) {
        final List<RexNode> list =
                convertExpressionList(
                        cx,
                        call.getSqlOperandList(),
                        op.getOperandTypeChecker().getConsistency() );
        final RexNode x = list.get( SqlBetweenOperator.VALUE_OPERAND );
        final RexNode y = list.get( SqlBetweenOperator.LOWER_OPERAND );
        final RexNode z = list.get( SqlBetweenOperator.UPPER_OPERAND );

        final RexBuilder rexBuilder = cx.getRexBuilder();
        RexNode ge1 = ge( rexBuilder, x, y );
        RexNode le1 = le( rexBuilder, x, z );
        RexNode and1 = and( rexBuilder, ge1, le1 );

        RexNode res;
        final SqlBetweenOperator.Flag symmetric = op.flag;
        switch ( symmetric ) {
            case ASYMMETRIC:
                res = and1;
                break;
            case SYMMETRIC:
                RexNode ge2 = ge( rexBuilder, x, z );
                RexNode le2 = le( rexBuilder, x, y );
                RexNode and2 = and( rexBuilder, ge2, le2 );
                res = or( rexBuilder, and1, and2 );
                break;
            default:
                throw Util.unexpected( symmetric );
        }
        final SqlBetweenOperator betweenOp = (SqlBetweenOperator) call.getOperator();
        if ( betweenOp.isNegated() ) {
            res = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT ), res );
        }
        return res;
    }


    /**
     * Converts a LiteralChain expression: that is, concatenates the operands immediately, to produce a single literal string.
     * <p>
     * Called automatically via reflection.
     */
    public RexNode convertLiteralChain( SqlRexContext cx, SqlLiteralChainOperator op, SqlCall call ) {
        Util.discard( cx );

        SqlLiteral sum = SqlLiteralChainOperator.concatenateOperands( call );
        return cx.convertLiteral( sum );
    }


    /**
     * Converts a ROW.
     * <p>
     * Called automatically via reflection.
     */
    public RexNode convertRow( SqlRexContext cx, SqlRowOperator op, SqlCall call ) {
        if ( cx.getValidator().getValidatedNodeType( call ).getPolyType() != PolyType.COLUMN_LIST ) {
            return convertCall( cx, call );
        }
        final RexBuilder rexBuilder = cx.getRexBuilder();
        final List<RexNode> columns = new ArrayList<>();
        for ( SqlNode operand : call.getSqlOperandList() ) {
            columns.add( rexBuilder.makeLiteral( ((SqlIdentifier) operand).getSimple() ) );
        }
        final AlgDataType type = rexBuilder.deriveReturnType( OperatorRegistry.get( OperatorName.COLUMN_LIST ), columns );
        return rexBuilder.makeCall( type, OperatorRegistry.get( OperatorName.COLUMN_LIST ), columns );
    }


    /**
     * Converts a call to OVERLAPS.
     * <p>
     * Called automatically via reflection.
     */
    public RexNode convertOverlaps( SqlRexContext cx, SqlOverlapsOperator op, SqlCall call ) {
        // for intervals [t0, t1] overlaps [t2, t3], we can find if the
        // intervals overlaps by: ~(t1 < t2 or t3 < t0)
        assert call.getOperandList().size() == 2;

        final Pair<RexNode, RexNode> left = convertOverlapsOperand( cx, call.getPos(), call.operand( 0 ) );
        final RexNode r0 = left.left;
        final RexNode r1 = left.right;
        final Pair<RexNode, RexNode> right = convertOverlapsOperand( cx, call.getPos(), call.operand( 1 ) );
        final RexNode r2 = right.left;
        final RexNode r3 = right.right;

        // Sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
        final RexBuilder rexBuilder = cx.getRexBuilder();
        RexNode leftSwap = le( rexBuilder, r0, r1 );
        final RexNode s0 = case_( rexBuilder, leftSwap, r0, r1 );
        final RexNode e0 = case_( rexBuilder, leftSwap, r1, r0 );
        RexNode rightSwap = le( rexBuilder, r2, r3 );
        final RexNode s1 = case_( rexBuilder, rightSwap, r2, r3 );
        final RexNode e1 = case_( rexBuilder, rightSwap, r3, r2 );
        // (e0 >= s1) AND (e1 >= s0)
        switch ( op.kind ) {
            case OVERLAPS:
                return and(
                        rexBuilder,
                        ge( rexBuilder, e0, s1 ),
                        ge( rexBuilder, e1, s0 ) );
            case CONTAINS:
                return and(
                        rexBuilder,
                        le( rexBuilder, s0, s1 ),
                        ge( rexBuilder, e0, e1 ) );
            case PERIOD_EQUALS:
                return and(
                        rexBuilder,
                        eq( rexBuilder, s0, s1 ),
                        eq( rexBuilder, e0, e1 ) );
            case PRECEDES:
                return le( rexBuilder, e0, s1 );
            case IMMEDIATELY_PRECEDES:
                return eq( rexBuilder, e0, s1 );
            case SUCCEEDS:
                return ge( rexBuilder, s0, e1 );
            case IMMEDIATELY_SUCCEEDS:
                return eq( rexBuilder, s0, e1 );
            default:
                throw new AssertionError( op );
        }
    }


    private Pair<RexNode, RexNode> convertOverlapsOperand( SqlRexContext cx, ParserPos pos, SqlNode operand ) {
        final SqlNode a0;
        final SqlNode a1;
        switch ( operand.getKind() ) {
            case ROW:
                a0 = ((SqlCall) operand).operand( 0 );
                final SqlNode a10 = ((SqlCall) operand).operand( 1 );
                final AlgDataType t1 = cx.getValidator().getValidatedNodeType( a10 );
                if ( PolyTypeUtil.isInterval( t1 ) ) {
                    // make t1 = t0 + t1 when t1 is an interval.
                    a1 = plus( pos, a0, a10 );
                } else {
                    a1 = a10;
                }
                break;
            default:
                a0 = operand;
                a1 = operand;
        }

        final RexNode r0 = cx.convertExpression( a0 );
        final RexNode r1 = cx.convertExpression( a1 );
        return Pair.of( r0, r1 );
    }


    /**
     * Casts a RexNode value to the validated type of a SqlCall. If the value was already of the validated type, then the value is returned without an additional cast.
     */
    public RexNode castToValidatedType( SqlRexContext cx, SqlCall call, RexNode value ) {
        return castToValidatedType( call, value, cx.getValidator(), cx.getRexBuilder() );
    }


    /**
     * Casts a RexNode value to the validated type of a SqlCall. If the value was already of the validated type, then the value is returned without an additional cast.
     */
    public static RexNode castToValidatedType( SqlNode node, RexNode e, SqlValidator validator, RexBuilder rexBuilder ) {
        final AlgDataType type = validator.getValidatedNodeType( node );
        if ( e.getType() == type ) {
            return e;
        }
        return rexBuilder.makeCast( type, e );
    }


    /**
     * Convertlet that handles {@code COVAR_POP}, {@code COVAR_SAMP}, {@code REGR_SXX}, {@code REGR_SYY} windowed aggregate functions.
     */
    private static class RegrCovarianceConvertlet implements SqlRexConvertlet {

        private final Kind kind;


        RegrCovarianceConvertlet( Kind kind ) {
            this.kind = kind;
        }


        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            assert call.operandCount() == 2;
            final SqlNode arg1 = call.operand( 0 );
            final SqlNode arg2 = call.operand( 1 );
            final SqlNode expr;
            final AlgDataType type = cx.getValidator().getValidatedNodeType( call );
            switch ( kind ) {
                case COVAR_POP:
                    expr = expandCovariance( arg1, arg2, null, type, cx, true );
                    break;
                case COVAR_SAMP:
                    expr = expandCovariance( arg1, arg2, null, type, cx, false );
                    break;
                case REGR_SXX:
                    expr = expandRegrSzz( arg2, arg1, type, cx, true );
                    break;
                case REGR_SYY:
                    expr = expandRegrSzz( arg1, arg2, type, cx, true );
                    break;
                default:
                    throw Util.unexpected( kind );
            }
            RexNode rex = cx.convertExpression( expr );
            return cx.getRexBuilder().ensureType( type, rex, true );
        }


        private SqlNode expandRegrSzz( final SqlNode arg1, final SqlNode arg2, final AlgDataType avgType, final SqlRexContext cx, boolean variance ) {
            final ParserPos pos = ParserPos.ZERO;
            final Node count = OperatorRegistry.get( OperatorName.REGR_COUNT ).createCall( pos, arg1, arg2 );
            final SqlNode varPop = expandCovariance( arg1, variance ? arg1 : arg2, arg2, avgType, cx, true );
            final RexNode varPopRex = cx.convertExpression( varPop );
            final SqlNode varPopCast;
            varPopCast = getCastedSqlNode( varPop, avgType, pos, varPopRex );
            return (SqlNode) OperatorRegistry.get( OperatorName.MULTIPLY ).createCall( pos, varPopCast, count );
        }


        private SqlNode expandCovariance( final SqlNode arg0Input, final SqlNode arg1Input, final SqlNode dependent, final AlgDataType varType, final SqlRexContext cx, boolean biased ) {
            // covar_pop(x1, x2) ==>
            //     (sum(x1 * x2) - sum(x2) * sum(x1) / count(x1, x2))
            //     / count(x1, x2)
            //
            // covar_samp(x1, x2) ==>
            //     (sum(x1 * x2) - sum(x1) * sum(x2) / count(x1, x2))
            //     / (count(x1, x2) - 1)
            final ParserPos pos = ParserPos.ZERO;
            final SqlLiteral nullLiteral = SqlLiteral.createNull( ParserPos.ZERO );

            final RexNode arg0Rex = cx.convertExpression( arg0Input );
            final RexNode arg1Rex = cx.convertExpression( arg1Input );

            final SqlNode arg0 = getCastedSqlNode( arg0Input, varType, pos, arg0Rex );
            final SqlNode arg1 = getCastedSqlNode( arg1Input, varType, pos, arg1Rex );
            final SqlNode argSquared = (SqlNode) OperatorRegistry.get( OperatorName.MULTIPLY ).createCall( pos, arg0, arg1 );
            final SqlNode sumArgSquared;
            final SqlNode sum0;
            final SqlNode sum1;
            final SqlNode count;
            if ( dependent == null ) {
                sumArgSquared = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, argSquared );
                sum0 = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, arg0, arg1 );
                sum1 = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, arg1, arg0 );
                count = (SqlNode) OperatorRegistry.get( OperatorName.REGR_COUNT ).createCall( pos, arg0, arg1 );
            } else {
                sumArgSquared = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, argSquared, dependent );
                sum0 = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, arg0, Objects.equals( dependent, arg0Input ) ? arg1 : dependent );
                sum1 = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, arg1, Objects.equals( dependent, arg1Input ) ? arg0 : dependent );
                count = (SqlNode) OperatorRegistry.get( OperatorName.REGR_COUNT ).createCall( pos, arg0, Objects.equals( dependent, arg0Input ) ? arg1 : dependent );
            }

            final SqlNode sumSquared = (SqlNode) OperatorRegistry.get( OperatorName.MULTIPLY ).createCall( pos, sum0, sum1 );
            final SqlNode countCasted = getCastedSqlNode( count, varType, pos, cx.convertExpression( count ) );

            final SqlNode avgSumSquared = (SqlNode) OperatorRegistry.get( OperatorName.DIVIDE ).createCall( pos, sumSquared, countCasted );
            final SqlNode diff = (SqlNode) OperatorRegistry.get( OperatorName.MINUS ).createCall( pos, sumArgSquared, avgSumSquared );
            SqlNode denominator;
            if ( biased ) {
                denominator = countCasted;
            } else {
                final SqlNumericLiteral one = SqlLiteral.createExactNumeric( "1", pos );
                denominator = new SqlCase(
                        ParserPos.ZERO,
                        countCasted,
                        SqlNodeList.of( (SqlNode) OperatorRegistry.get( OperatorName.EQUALS ).createCall( pos, countCasted, one ) ),
                        SqlNodeList.of( getCastedSqlNode( nullLiteral, varType, pos, null ) ),
                        (SqlNode) OperatorRegistry.get( OperatorName.MINUS ).createCall( pos, countCasted, one ) );
            }

            return (SqlNode) OperatorRegistry.get( OperatorName.DIVIDE ).createCall( pos, diff, denominator );
        }


        private SqlNode getCastedSqlNode( SqlNode argInput, AlgDataType varType, ParserPos pos, RexNode argRex ) {
            SqlNode arg;
            if ( argRex != null && !argRex.getType().equals( varType ) ) {
                arg = (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall( pos, argInput, (Node) SqlTypeUtil.convertTypeToSpec( varType ) );
            } else {
                arg = argInput;
            }
            return arg;
        }

    }


    /**
     * Convertlet that handles {@code AVG} and {@code VARIANCE} windowed aggregate functions.
     */
    private static class AvgVarianceConvertlet implements SqlRexConvertlet {

        private final Kind kind;


        AvgVarianceConvertlet( Kind kind ) {
            this.kind = kind;
        }


        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            assert call.operandCount() == 1;
            final SqlNode arg = call.operand( 0 );
            final SqlNode expr;
            final AlgDataType type =
                    cx.getValidator().getValidatedNodeType( call );
            switch ( kind ) {
                case AVG:
                    expr = expandAvg( arg, type, cx );
                    break;
                case STDDEV_POP:
                    expr = expandVariance( arg, type, cx, true, true );
                    break;
                case STDDEV_SAMP:
                    expr = expandVariance( arg, type, cx, false, true );
                    break;
                case VAR_POP:
                    expr = expandVariance( arg, type, cx, true, false );
                    break;
                case VAR_SAMP:
                    expr = expandVariance( arg, type, cx, false, false );
                    break;
                default:
                    throw Util.unexpected( kind );
            }
            RexNode rex = cx.convertExpression( expr );
            return cx.getRexBuilder().ensureType( type, rex, true );
        }


        private SqlNode expandAvg( final SqlNode arg, final AlgDataType avgType, final SqlRexContext cx ) {
            final ParserPos pos = ParserPos.ZERO;
            final SqlNode sum = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, arg );
            final RexNode sumRex = cx.convertExpression( sum );
            final SqlNode sumCast;
            sumCast = getCastedSqlNode( sum, avgType, pos, sumRex );
            final SqlNode count = (SqlNode) OperatorRegistry.get( OperatorName.COUNT ).createCall( pos, arg );
            return (SqlNode) OperatorRegistry.get( OperatorName.DIVIDE ).createCall( pos, sumCast, count );
        }


        private SqlNode expandVariance( final SqlNode argInput, final AlgDataType varType, final SqlRexContext cx, boolean biased, boolean sqrt ) {
            // stddev_pop(x) ==>
            //   power(
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / count(x),
            //     .5)
            //
            // stddev_samp(x) ==>
            //   power(
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / (count(x) - 1),
            //     .5)
            //
            // var_pop(x) ==>
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / count(x)
            //
            // var_samp(x) ==>
            //     (sum(x * x) - sum(x) * sum(x) / count(x))
            //     / (count(x) - 1)
            final ParserPos pos = ParserPos.ZERO;

            final SqlNode arg = getCastedSqlNode( argInput, varType, pos, cx.convertExpression( argInput ) );

            final SqlNode argSquared = (SqlNode) OperatorRegistry.get( OperatorName.MULTIPLY ).createCall( pos, arg, arg );
            final SqlNode argSquaredCasted = getCastedSqlNode( argSquared, varType, pos, cx.convertExpression( argSquared ) );
            final SqlNode sumArgSquared = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, argSquaredCasted );
            final SqlNode sumArgSquaredCasted = getCastedSqlNode( sumArgSquared, varType, pos, cx.convertExpression( sumArgSquared ) );
            final SqlNode sum = (SqlNode) OperatorRegistry.get( OperatorName.SUM ).createCall( pos, arg );
            final SqlNode sumCasted = getCastedSqlNode( sum, varType, pos, cx.convertExpression( sum ) );
            final SqlNode sumSquared = (SqlNode) OperatorRegistry.get( OperatorName.MULTIPLY ).createCall( pos, sumCasted, sumCasted );
            final SqlNode sumSquaredCasted = getCastedSqlNode( sumSquared, varType, pos, cx.convertExpression( sumSquared ) );
            final SqlNode count = (SqlNode) OperatorRegistry.get( OperatorName.COUNT ).createCall( pos, arg );
            final SqlNode countCasted = getCastedSqlNode( count, varType, pos, cx.convertExpression( count ) );
            final SqlNode avgSumSquared = (SqlNode) OperatorRegistry.get( OperatorName.DIVIDE ).createCall( pos, sumSquaredCasted, countCasted );
            final SqlNode avgSumSquaredCasted = getCastedSqlNode( avgSumSquared, varType, pos, cx.convertExpression( avgSumSquared ) );
            final SqlNode diff = (SqlNode) OperatorRegistry.get( OperatorName.MINUS ).createCall( pos, sumArgSquaredCasted, avgSumSquaredCasted );
            final SqlNode diffCasted = getCastedSqlNode( diff, varType, pos, cx.convertExpression( diff ) );
            final SqlNode denominator;
            if ( biased ) {
                denominator = countCasted;
            } else {
                final SqlNumericLiteral one = SqlLiteral.createExactNumeric( "1", pos );
                final SqlLiteral nullLiteral = SqlLiteral.createNull( ParserPos.ZERO );
                denominator = new SqlCase(
                        ParserPos.ZERO,
                        count,
                        SqlNodeList.of( (SqlNode) OperatorRegistry.get( OperatorName.EQUALS ).createCall( pos, count, one ) ),
                        SqlNodeList.of( getCastedSqlNode( nullLiteral, varType, pos, null ) ),
                        (SqlNode) OperatorRegistry.get( OperatorName.MINUS ).createCall( pos, count, one ) );
            }
            final SqlNode div = (SqlNode) OperatorRegistry.get( OperatorName.DIVIDE ).createCall( pos, diffCasted, denominator );
            final SqlNode divCasted = getCastedSqlNode( div, varType, pos, cx.convertExpression( div ) );

            SqlNode result = div;
            if ( sqrt ) {
                final SqlNumericLiteral half = SqlLiteral.createExactNumeric( "0.5", pos );
                result = (SqlNode) OperatorRegistry.get( OperatorName.POWER ).createCall( pos, divCasted, half );
            }
            return result;
        }


        private SqlNode getCastedSqlNode( SqlNode argInput, AlgDataType varType, ParserPos pos, RexNode argRex ) {
            SqlNode arg;
            if ( argRex != null && !argRex.getType().equals( varType ) ) {
                arg = (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall( pos, argInput, (Node) SqlTypeUtil.convertTypeToSpec( varType ) );
            } else {
                arg = argInput;
            }
            return arg;
        }

    }


    /**
     * Convertlet that converts {@code LTRIM} and {@code RTRIM} to {@code TRIM}.
     */
    private static class TrimConvertlet implements SqlRexConvertlet {

        private final SqlTrimFunction.Flag flag;


        TrimConvertlet( SqlTrimFunction.Flag flag ) {
            this.flag = flag;
        }


        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final RexNode operand = cx.convertExpression( call.getSqlOperandList().get( 0 ) );
            return rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.TRIM ),
                    rexBuilder.makeFlag( flag ),
                    rexBuilder.makeLiteral( " " ),
                    operand );
        }

    }


    /**
     * Convertlet that converts {@code GREATEST} and {@code LEAST}.
     */
    private static class GreatestConvertlet implements SqlRexConvertlet {

        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            // Translate
            //   GREATEST(a, b, c, d)
            // to
            //   CASE
            //   WHEN a IS NULL OR b IS NULL OR c IS NULL OR d IS NULL
            //   THEN NULL
            //   WHEN a > b AND a > c AND a > d
            //   THEN a
            //   WHEN b > c AND b > d
            //   THEN b
            //   WHEN c > d
            //   THEN c
            //   ELSE d
            //   END
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final AlgDataType type = cx.getValidator().getValidatedNodeType( call );
            final BinaryOperator op;
            switch ( call.getKind() ) {
                case GREATEST:
                    op = OperatorRegistry.getBinary( OperatorName.GREATER_THAN );
                    break;
                case LEAST:
                    op = OperatorRegistry.getBinary( OperatorName.LESS_THAN );
                    break;
                default:
                    throw new AssertionError();
            }
            final List<RexNode> exprs = convertExpressionList( cx, call.getSqlOperandList(), PolyOperandTypeChecker.Consistency.NONE );
            final List<RexNode> list = new ArrayList<>();
            final List<RexNode> orList = new ArrayList<>();
            for ( RexNode expr : exprs ) {
                orList.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), expr ) );
            }
            list.add( RexUtil.composeDisjunction( rexBuilder, orList ) );
            list.add( rexBuilder.makeNullLiteral( type ) );
            for ( int i = 0; i < exprs.size() - 1; i++ ) {
                RexNode expr = exprs.get( i );
                final List<RexNode> andList = new ArrayList<>();
                for ( int j = i + 1; j < exprs.size(); j++ ) {
                    final RexNode expr2 = exprs.get( j );
                    andList.add( rexBuilder.makeCall( op, expr, expr2 ) );
                }
                list.add( RexUtil.composeConjunction( rexBuilder, andList ) );
                list.add( expr );
            }
            list.add( exprs.get( exprs.size() - 1 ) );
            return rexBuilder.makeCall( type, OperatorRegistry.get( OperatorName.CASE ), list );
        }

    }


    /**
     * Convertlet that handles {@code FLOOR} and {@code CEIL} functions.
     */
    private class FloorCeilConvertlet implements SqlRexConvertlet {

        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            return convertFloorCeil( cx, call );
        }

    }


    /**
     * Convertlet that handles the {@code TIMESTAMPADD} function.
     */
    private static class TimestampAddConvertlet implements SqlRexConvertlet {

        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            // TIMESTAMPADD(unit, count, timestamp)
            //  => timestamp + count * INTERVAL '1' UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlLiteral unitLiteral = call.operand( 0 );
            final TimeUnit unit = unitLiteral.symbolValue( TimeUnit.class );
            RexNode interval2Add;
            SqlIntervalQualifier qualifier = new SqlIntervalQualifier( unit, null, unitLiteral.getPos() );
            RexNode op1 = cx.convertExpression( call.operand( 1 ) );
            interval2Add = switch ( unit ) {
                case MICROSECOND, NANOSECOND -> divide(
                        rexBuilder,
                        multiply(
                                rexBuilder,
                                rexBuilder.makeIntervalLiteral( 1L, qualifier ),
                                op1 ),
                        BigDecimal.ONE.divide( unit.multiplier, RoundingMode.UNNECESSARY ) );
                default -> multiply(
                        rexBuilder,
                        rexBuilder.makeIntervalLiteral( 1L, qualifier ),
                        op1 );
            };

            return rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.DATETIME_PLUS ),
                    cx.convertExpression( call.operand( 2 ) ),
                    toInterval( rexBuilder, interval2Add, qualifier ) );
        }


        private RexNode toInterval( RexBuilder rexBuilder, RexNode interval2Add, SqlIntervalQualifier qualifier ) {
            return rexBuilder.makeCast(
                    rexBuilder.getTypeFactory().createIntervalType( qualifier ),
                    interval2Add );
        }



    }


    /**
     * Convertlet that handles the {@code TIMESTAMPDIFF} function.
     */
    private static class TimestampDiffConvertlet implements SqlRexConvertlet {

        @Override
        public RexNode convertCall( SqlRexContext cx, SqlCall call ) {
            // TIMESTAMPDIFF(unit, t1, t2)
            //    => (t2 - t1) UNIT
            final RexBuilder rexBuilder = cx.getRexBuilder();
            final SqlLiteral unitLiteral = call.operand( 0 );
            TimeUnit unit = unitLiteral.symbolValue( TimeUnit.class );
            BigDecimal multiplier = BigDecimal.ONE;
            BigDecimal divider = BigDecimal.ONE;
            PolyType polyType =
                    unit == TimeUnit.NANOSECOND
                            ? PolyType.BIGINT
                            : PolyType.INTEGER;
            switch ( unit ) {
                case MICROSECOND:
                case MILLISECOND:
                case NANOSECOND:
                case WEEK:
                    multiplier = BigDecimal.valueOf( DateTimeUtils.MILLIS_PER_SECOND );
                    divider = unit.multiplier;
                    unit = TimeUnit.SECOND;
                    break;
                case QUARTER:
                    divider = unit.multiplier;
                    unit = TimeUnit.MONTH;
                    break;
            }
            final SqlIntervalQualifier qualifier = new SqlIntervalQualifier( unit, null, ParserPos.ZERO );
            final RexNode op2 = cx.convertExpression( call.operand( 2 ) );
            final RexNode op1 = cx.convertExpression( call.operand( 1 ) );
            final AlgDataType intervalType =
                    cx.getTypeFactory().createTypeWithNullability(
                            cx.getTypeFactory().createIntervalType( qualifier ),
                            op1.getType().isNullable() || op2.getType().isNullable() );
            final RexCall rexCall = (RexCall) rexBuilder.makeCall(
                    intervalType,
                    OperatorRegistry.get( OperatorName.MINUS_DATE ),
                    ImmutableList.of( op2, op1 ) );
            final AlgDataType intType =
                    cx.getTypeFactory().createTypeWithNullability(
                            cx.getTypeFactory().createPolyType( polyType ),
                            PolyTypeUtil.containsNullable( rexCall.getType() ) );
            RexNode e = rexBuilder.makeCast( intType, rexCall );
            return rexBuilder.multiplyDivide( e, multiplier, divider );
        }

    }

}

