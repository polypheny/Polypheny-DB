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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.rules;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * ReduceDecimalsRule is a rule which reduces decimal operations (such as casts or arithmetic) into operations involving more
 * primitive types (such as longs and doubles). The rule allows Polypheny-DB implementations to deal with decimals in a
 * consistent manner, while saving the effort of implementing them.
 *
 * The rule can be applied to a
 * {@link LogicalCalc} with a program for which
 * {@link RexUtil#requiresDecimalExpansion} returns true. The rule relies on a
 * {@link RexShuttle} to walk over relational expressions and replace them.
 *
 * While decimals are generally not implemented by the Polypheny-DB runtime, the rule is optionally applied, in order to
 * support the situation in which we would like to push down decimal operations to an external database.
 */
public class ReduceDecimalsRule extends AlgOptRule {

    public static final ReduceDecimalsRule INSTANCE = new ReduceDecimalsRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ReduceDecimalsRule.
     */
    public ReduceDecimalsRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalCalc.class, any() ), algBuilderFactory, null );
    }


    // implement RelOptRule
    @Override
    public Convention getOutConvention() {
        return Convention.NONE;
    }


    // implement RelOptRule
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalCalc calc = call.alg( 0 );

        // Expand decimals in every expression in this program. If no expression changes, don't apply the rule.
        final RexProgram program = calc.getProgram();
        if ( !RexUtil.requiresDecimalExpansion( program, true ) ) {
            return;
        }

        final RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        final RexShuttle shuttle = new DecimalShuttle( rexBuilder );
        RexProgramBuilder programBuilder =
                RexProgramBuilder.create(
                        rexBuilder,
                        calc.getInput().getTupleType(),
                        program.getExprList(),
                        program.getProjectList(),
                        program.getCondition(),
                        program.getOutputRowType(),
                        shuttle,
                        true );

        final RexProgram newProgram = programBuilder.getProgram();
        LogicalCalc newCalc = LogicalCalc.create( calc.getInput(), newProgram );
        call.transformTo( newCalc );
    }


    /**
     * A shuttle which converts decimal expressions to expressions based on longs.
     */
    public class DecimalShuttle extends RexShuttle {

        private final Map<Pair<RexNode, String>, RexNode> irreducible;
        private final Map<Pair<RexNode, String>, RexNode> results;
        private final ExpanderMap expanderMap;


        public DecimalShuttle( RexBuilder rexBuilder ) {
            irreducible = new HashMap<>();
            results = new HashMap<>();
            expanderMap = new ExpanderMap( rexBuilder );
        }


        /**
         * Rewrites a call in place, from bottom up, as follows:
         *
         * <ol>
         * <li>visit operands</li>
         * <li>visit call node</li>
         * <ol>
         * <li>rewrite call</li>
         * <li>visit the rewritten call</li>
         * </ol>
         * </ol>
         */
        @Override
        public RexNode visitCall( RexCall call ) {
            RexNode savedResult = lookup( call );
            if ( savedResult != null ) {
                return savedResult;
            }

            // permanently updates a call in place
            List<RexNode> newOperands = apply( call.getOperands() );
            if ( true ) {
                // FIXME: Operands are now immutable. Create a new call with new operands?
                throw new AssertionError();
            }

            RexNode newCall = call;
            RexNode rewrite = rewriteCall( call );
            if ( rewrite != call ) {
                newCall = rewrite.accept( this );
            }

            register( call, newCall );
            return newCall;
        }


        /**
         * Registers node so it will not be computed again
         */
        private void register( RexNode node, RexNode reducedNode ) {
            Pair<RexNode, String> key = RexUtil.makeKey( node );
            if ( node == reducedNode ) {
                irreducible.put( key, reducedNode );
            } else {
                results.put( key, reducedNode );
            }
        }


        /**
         * Lookup registered node
         */
        private RexNode lookup( RexNode node ) {
            Pair<RexNode, String> key = RexUtil.makeKey( node );
            if ( irreducible.get( key ) != null ) {
                return node;
            }
            return results.get( key );
        }


        /**
         * Rewrites a call, if required, or returns the original call
         */
        private RexNode rewriteCall( RexCall call ) {
            Operator operator = call.getOperator();
            if ( !operator.requiresDecimalExpansion() ) {
                return call;
            }

            RexExpander expander = getExpander( call );
            if ( expander.canExpand( call ) ) {
                return expander.expand( call );
            }
            return call;
        }


        /**
         * Returns a {@link RexExpander} for a call
         */
        private RexExpander getExpander( RexCall call ) {
            return expanderMap.getExpander( call );
        }

    }


    /**
     * Maps a RexCall to a RexExpander
     */
    private class ExpanderMap {

        private final Map<Operator, RexExpander> map;
        private RexExpander defaultExpander;


        private ExpanderMap( RexBuilder rexBuilder ) {
            map = new HashMap<>();
            registerExpanders( rexBuilder );
        }


        private void registerExpanders( RexBuilder rexBuilder ) {
            RexExpander cast = new CastExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.CAST ), cast );

            RexExpander passThrough = new PassThroughExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.UNARY_MINUS ), passThrough );
            map.put( OperatorRegistry.get( OperatorName.ABS ), passThrough );

            map.put( OperatorRegistry.get( OperatorName.IS_NULL ), passThrough );
            map.put( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), passThrough );

            RexExpander arithmetic = new BinaryArithmeticExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.DIVIDE ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.MULTIPLY ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.PLUS ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.MINUS ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.MOD ), arithmetic );

            map.put( OperatorRegistry.get( OperatorName.EQUALS ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.GREATER_THAN ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.LESS_THAN ), arithmetic );
            map.put( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), arithmetic );

            RexExpander floor = new FloorExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.FLOOR ), floor );
            RexExpander ceil = new CeilExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.CEIL ), ceil );

            RexExpander reinterpret = new ReinterpretExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.REINTERPRET ), reinterpret );

            RexExpander caseExpander = new CaseExpander( rexBuilder );
            map.put( OperatorRegistry.get( OperatorName.CASE ), caseExpander );

            defaultExpander = new CastArgAsDoubleExpander( rexBuilder );
        }


        public RexExpander getExpander( RexCall call ) {
            RexExpander expander = map.get( call.getOperator() );
            return (expander != null) ? expander : defaultExpander;
        }

    }


    /**
     * Rewrites a decimal expression for a specific set of SqlOperator's. In general, most expressions are rewritten in such
     * a way that SqlOperator's do not have to deal with decimals. Decimals are represented by their unscaled integer
     * representations, similar to {@link BigDecimal#unscaledValue()} (i.e. 10^scale). Once decimals are decoded, SqlOperators
     * can then operate on the integer representations. The value can later be recoded as a decimal.
     * <p>
     * For example, suppose one casts 2.0 as a decima(10,4). The value is decoded (20), multiplied by a scale factor (1000),
     * for a result of (20000) which is encoded as a decimal(10,4), in this case 2.0000
     * <p>
     * To avoid the lengthy coding of RexNode expressions, this base class provides succinct methods for building expressions
     * used in rewrites.
     */
    public abstract static class RexExpander {

        /**
         * Factory for constructing new relational expressions
         */
        RexBuilder builder;

        /**
         * Type for the internal representation of decimals. This type is a non-nullable type and requires extra work to make it nullable.
         */
        AlgDataType int8;

        /**
         * Type for doubles. This type is a non-nullable type and requires extra work to make it nullable.
         */
        AlgDataType real8;


        /**
         * Constructs a RexExpander
         */
        public RexExpander( RexBuilder builder ) {
            this.builder = builder;
            int8 = builder.getTypeFactory().createPolyType( PolyType.BIGINT );
            real8 = builder.getTypeFactory().createPolyType( PolyType.DOUBLE );
        }


        /**
         * This defaults to the utility method, {@link RexUtil#requiresDecimalExpansion(RexNode, boolean)} which checks
         * general guidelines on whether a rewrite should be considered at all. In general, it is helpful to update the
         * utility method since that method is often used to filter the somewhat expensive rewrite process.
         * <p>
         * However, this method provides another place for implementations of RexExpander to make a more detailed analysis
         * before deciding on whether to perform a rewrite.
         */
        public boolean canExpand( RexCall call ) {
            return RexUtil.requiresDecimalExpansion( call, false );
        }


        /**
         * Rewrites an expression containing decimals. Normally, this method always performs a rewrite, but implementations
         * may choose to return the original expression if no change was required.
         */
        public abstract RexNode expand( RexCall call );


        /**
         * Makes an exact numeric literal to be used for scaling
         *
         * @param scale a scale from one to max precision - 1
         * @return 10^scale as an exact numeric value
         */
        protected RexNode makeScaleFactor( int scale ) {
            assert scale > 0;
            assert scale < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision();
            return makeExactLiteral( powerOfTen( scale ) );
        }


        /**
         * Makes an approximate literal to be used for scaling
         *
         * @param scale a scale from -99 to 99
         * @return 10^scale as an approximate value
         */
        protected RexNode makeApproxScaleFactor( int scale ) {
            assert (-100 < scale) && (scale < 100) : "could not make approximate scale factor";
            if ( scale >= 0 ) {
                return makeApproxLiteral( BigDecimal.TEN.pow( scale ) );
            } else {
                BigDecimal tenth = BigDecimal.valueOf( 1, 1 );
                return makeApproxLiteral( tenth.pow( -scale ) );
            }
        }


        /**
         * Makes an exact numeric value to be used for rounding.
         *
         * @param scale a scale from 1 to max precision - 1
         * @return 10^scale / 2 as an exact numeric value
         */
        protected RexNode makeRoundFactor( int scale ) {
            assert scale > 0;
            assert scale < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision();
            return makeExactLiteral( powerOfTen( scale ) / 2 );
        }


        /**
         * Calculates a power of ten, as a long value
         */
        protected long powerOfTen( int scale ) {
            assert scale >= 0;
            assert scale < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision();
            return BigInteger.TEN.pow( scale ).longValue();
        }


        /**
         * Makes an exact, non-nullable literal of Bigint type
         */
        protected RexNode makeExactLiteral( long l ) {
            BigDecimal bd = BigDecimal.valueOf( l );
            return builder.makeExactLiteral( bd, int8 );
        }


        /**
         * Makes an approximate literal of double precision
         */
        protected RexNode makeApproxLiteral( BigDecimal bd ) {
            return builder.makeApproxLiteral( bd );
        }


        /**
         * Scales up a decimal value and returns the scaled value as an exact number.
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision - 1
         * @return value * 10^scale as an exact numeric value
         */
        protected RexNode scaleUp( RexNode value, int scale ) {
            assert scale >= 0;
            assert scale < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision();
            if ( scale == 0 ) {
                return value;
            }
            return builder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), value, makeScaleFactor( scale ) );
        }


        /**
         * Scales down a decimal value, and returns the scaled value as an exact numeric. with the rounding convention
         * {@link BigDecimal#ROUND_HALF_UP BigDecimal.ROUND_HALF_UP}. (Values midway between two points are rounded away from zero.)
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision
         * @return value/10^scale, rounded away from zero and returned as an exact numeric value
         */
        protected RexNode scaleDown( RexNode value, int scale ) {
            final int maxPrecision = builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision();
            assert scale >= 0 && scale <= maxPrecision;
            if ( scale == 0 ) {
                return value;
            }
            if ( scale == maxPrecision ) {
                long half = BigInteger.TEN.pow( scale - 1 ).longValue() * 5;
                return makeCase(
                        builder.makeCall(
                                OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ),
                                value,
                                makeExactLiteral( half ) ),
                        makeExactLiteral( 1 ),
                        builder.makeCall(
                                OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ),
                                value,
                                makeExactLiteral( -half ) ),
                        makeExactLiteral( -1 ),
                        makeExactLiteral( 0 ) );
            }
            RexNode roundFactor = makeRoundFactor( scale );
            RexNode roundValue =
                    makeCase(
                            builder.makeCall(
                                    OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                    value,
                                    makeExactLiteral( 0 ) ),
                            makePlus( value, roundFactor ),
                            makeMinus( value, roundFactor ) );
            return makeDivide(
                    roundValue,
                    makeScaleFactor( scale ) );
        }


        /**
         * Scales down a decimal value and returns the scaled value as a an double precision approximate value.
         * Scaling is implemented with double precision arithmetic.
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision
         * @return value/10^scale as a double precision value
         */
        protected RexNode scaleDownDouble( RexNode value, int scale ) {
            assert scale >= 0;
            assert scale <= builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision();
            RexNode cast = ensureType( real8, value );
            if ( scale == 0 ) {
                return cast;
            }
            return makeDivide( cast, makeApproxScaleFactor( scale ) );
        }


        /**
         * Ensures a value is of a required scale. If it is not, then the value is multiplied by a scale factor. Scaling up
         * an exact value is limited to max precision - 1, because we cannot represent the result of larger scales internally.
         * Scaling up a floating point value is more flexible since the value may be very small despite having a scale of
         * zero and the scaling may still produce a reasonable result.
         *
         * @param value integer representation of decimal, or a floating point number
         * @param scale current scale, 0 for floating point numbers
         * @param required required scale, must be at least the current scale; the scale difference may not be greater than max precision - 1 for exact numerics
         * @return value * 10^scale, returned as an exact or approximate value corresponding to the input value
         */
        protected RexNode ensureScale( RexNode value, int scale, int required ) {
            final AlgDataTypeSystem typeSystem = builder.getTypeFactory().getTypeSystem();
            final int maxPrecision = typeSystem.getMaxNumericPrecision();
            assert scale <= maxPrecision && required <= maxPrecision;
            assert required >= scale;
            if ( scale == required ) {
                return value;
            }
            int scaleDiff = required - scale;
            if ( PolyTypeUtil.isApproximateNumeric( value.getType() ) ) {
                return makeMultiply( value, makeApproxScaleFactor( scaleDiff ) );
            }

            // TODO: make a validator exception for this
            if ( scaleDiff >= maxPrecision ) {
                throw Util.needToImplement( "Source type with scale " + scale + " cannot be converted to target type with scale " + required + " because the smallest value of the source type is too large to be encoded by the target type" );
            }
            return scaleUp( value, scaleDiff );
        }


        /**
         * Retrieves a decimal node's integer representation
         *
         * @param decimalNode the decimal value as an opaque type
         * @return an integer representation of the decimal value
         */
        protected RexNode decodeValue( RexNode decimalNode ) {
            assert PolyTypeUtil.isDecimal( decimalNode.getType() );
            return builder.decodeIntervalOrDecimal( decimalNode );
        }


        /**
         * Retrieves the primitive value of a numeric node. If the node is a decimal, then it must first be decoded. Otherwise, the original node may be returned.
         *
         * @param node a numeric node, possibly a decimal
         * @return the primitive value of the numeric node
         */
        protected RexNode accessValue( RexNode node ) {
            assert PolyTypeUtil.isNumeric( node.getType() );
            if ( PolyTypeUtil.isDecimal( node.getType() ) ) {
                return decodeValue( node );
            }
            return node;
        }


        /**
         * Casts a decimal's integer representation to a decimal node. If the expression is not the expected integer type,
         * then it is cast first.
         * <p>
         * This method does not request an overflow check.
         *
         * @param value integer representation of decimal
         * @param decimalType type integer will be reinterpreted as
         * @return the integer representation reinterpreted as a decimal type
         */
        protected RexNode encodeValue( RexNode value, AlgDataType decimalType ) {
            return encodeValue( value, decimalType, false );
        }


        /**
         * Casts a decimal's integer representation to a decimal node. If the expression is not the expected integer type,
         * then it is cast first.
         * <p>
         * An overflow check may be requested to ensure the internal value does not exceed the maximum value of the decimal type.
         *
         * @param value integer representation of decimal
         * @param decimalType type integer will be reinterpreted as
         * @param checkOverflow indicates whether an overflow check is required when reinterpreting this particular value as the decimal type. A check usually not required for arithmetic, but is often required for rounding and explicit casts.
         * @return the integer reinterpreted as an opaque decimal type
         */
        protected RexNode encodeValue( RexNode value, AlgDataType decimalType, boolean checkOverflow ) {
            return builder.encodeIntervalOrDecimal( value, decimalType, checkOverflow );
        }


        /**
         * Ensures expression is interpreted as a specified type. The returned expression may be wrapped with a cast.
         * <p>
         * This method corrects the nullability of the specified type to match the nullability of the expression.
         *
         * @param type desired type
         * @param node expression
         * @return a cast expression or the original expression
         */
        protected RexNode ensureType( AlgDataType type, RexNode node ) {
            return ensureType( type, node, true );
        }


        /**
         * Ensures expression is interpreted as a specified type. The returned expression may be wrapped with a cast.
         *
         * @param type desired type
         * @param node expression
         * @param matchNullability whether to correct nullability of specified type to match the expression; this usually should be true, except for explicit casts which can override default nullability
         * @return a casted expression or the original expression
         */
        protected RexNode ensureType( AlgDataType type, RexNode node, boolean matchNullability ) {
            return builder.ensureType( type, node, matchNullability );
        }


        protected RexNode makeCase( RexNode condition, RexNode thenClause, RexNode elseClause ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.CASE ),
                    condition,
                    thenClause,
                    elseClause );
        }


        protected RexNode makeCase( RexNode whenA, RexNode thenA, RexNode whenB, RexNode thenB, RexNode elseClause ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.CASE ),
                    whenA,
                    thenA,
                    whenB,
                    thenB,
                    elseClause );
        }


        protected RexNode makePlus( RexNode a, RexNode b ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.PLUS ),
                    a,
                    b );
        }


        protected RexNode makeMinus( RexNode a, RexNode b ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.MINUS ),
                    a,
                    b );
        }


        protected RexNode makeDivide( RexNode a, RexNode b ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.DIVIDE_INTEGER ),
                    a,
                    b );
        }


        protected RexNode makeMultiply( RexNode a, RexNode b ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.MULTIPLY ),
                    a,
                    b );
        }


        protected RexNode makeIsPositive( RexNode a ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.GREATER_THAN ),
                    a,
                    makeExactLiteral( 0 ) );
        }


        protected RexNode makeIsNegative( RexNode a ) {
            return builder.makeCall(
                    OperatorRegistry.get( OperatorName.LESS_THAN ),
                    a,
                    makeExactLiteral( 0 ) );
        }

    }


    /**
     * Expands a decimal cast expression
     */
    private static class CastExpander extends RexExpander {

        private CastExpander( RexBuilder builder ) {
            super( builder );
        }


        // implement RexExpander
        @Override
        public RexNode expand( RexCall call ) {
            List<RexNode> operands = call.operands;
            assert call.isA( Kind.CAST );
            assert operands.size() == 1;
            assert !RexLiteral.isNullLiteral( operands.get( 0 ) );

            RexNode operand = operands.get( 0 );
            AlgDataType fromType = operand.getType();
            AlgDataType toType = call.getType();
            assert PolyTypeUtil.isDecimal( fromType ) || PolyTypeUtil.isDecimal( toType );

            if ( PolyTypeUtil.isIntType( toType ) ) {
                // decimal to int
                return ensureType(
                        toType,
                        scaleDown( decodeValue( operand ), fromType.getScale() ),
                        false );
            } else if ( PolyTypeUtil.isApproximateNumeric( toType ) ) {
                // decimal to floating point
                return ensureType(
                        toType,
                        scaleDownDouble( decodeValue( operand ), fromType.getScale() ),
                        false );
            } else if ( PolyTypeUtil.isApproximateNumeric( fromType ) ) {
                // real to decimal
                return encodeValue(
                        ensureScale( operand, 0, toType.getScale() ),
                        toType,
                        true );
            }

            if ( !PolyTypeUtil.isExactNumeric( fromType ) || !PolyTypeUtil.isExactNumeric( toType ) ) {
                throw Util.needToImplement( "Cast from '" + fromType.toString() + "' to '" + toType.toString() + "'" );
            }
            int fromScale = fromType.getScale();
            int toScale = toType.getScale();
            int fromDigits = fromType.getPrecision() - fromScale;
            int toDigits = toType.getPrecision() - toScale;

            // NOTE: precision 19 overflows when its underlying bigint representation overflows
            boolean checkOverflow = (toType.getPrecision() < 19) && (toDigits < fromDigits);

            if ( PolyTypeUtil.isIntType( fromType ) ) {
                // int to decimal
                return encodeValue(
                        ensureScale( operand, 0, toType.getScale() ),
                        toType,
                        checkOverflow );
            } else if ( PolyTypeUtil.isDecimal( fromType ) && PolyTypeUtil.isDecimal( toType ) ) {
                // decimal to decimal
                RexNode value = decodeValue( operand );
                RexNode scaled;
                if ( fromScale <= toScale ) {
                    scaled = ensureScale( value, fromScale, toScale );
                } else {
                    if ( toDigits == fromDigits ) {
                        // rounding away from zero may cause an overflow
                        // for example: cast(9.99 as decimal(2,1))
                        checkOverflow = true;
                    }
                    scaled = scaleDown( value, fromScale - toScale );
                }

                return encodeValue( scaled, toType, checkOverflow );
            } else {
                throw Util.needToImplement( "Reduce decimal cast from " + fromType + " to " + toType );
            }
        }

    }


    /**
     * Expands a decimal arithmetic expression
     */
    private static class BinaryArithmeticExpander extends RexExpander {

        AlgDataType typeA;
        AlgDataType typeB;
        int scaleA;
        int scaleB;


        private BinaryArithmeticExpander( RexBuilder builder ) {
            super( builder );
        }


        // implement RexExpander
        @Override
        public RexNode expand( RexCall call ) {
            List<RexNode> operands = call.operands;
            assert operands.size() == 2;
            AlgDataType typeA = operands.get( 0 ).getType();
            AlgDataType typeB = operands.get( 1 ).getType();
            assert PolyTypeUtil.isNumeric( typeA ) && PolyTypeUtil.isNumeric( typeB );

            if ( PolyTypeUtil.isApproximateNumeric( typeA ) || PolyTypeUtil.isApproximateNumeric( typeB ) ) {
                List<RexNode> newOperands;
                if ( PolyTypeUtil.isApproximateNumeric( typeA ) ) {
                    newOperands = ImmutableList.of( operands.get( 0 ), ensureType( real8, operands.get( 1 ) ) );
                } else {
                    newOperands = ImmutableList.of( ensureType( real8, operands.get( 0 ) ), operands.get( 1 ) );
                }
                return builder.makeCall( call.getOperator(), newOperands );
            }

            analyzeOperands( operands );
            if ( call.isA( Kind.PLUS ) ) {
                return expandPlusMinus( call, operands );
            } else if ( call.isA( Kind.MINUS ) ) {
                return expandPlusMinus( call, operands );
            } else if ( call.isA( Kind.DIVIDE ) ) {
                return expandDivide( call, operands );
            } else if ( call.isA( Kind.TIMES ) ) {
                return expandTimes( call, operands );
            } else if ( call.isA( Kind.COMPARISON ) ) {
                return expandComparison( call, operands );
            } else if ( call.getOperator().getOperatorName() == OperatorName.MOD ) {
                return expandMod( call, operands );
            } else {
                throw new AssertionError( "ReduceDecimalsRule could not expand " + call.getOperator() );
            }
        }


        /**
         * Convenience method for reading characteristics of operands (such as scale, precision, whole digits) into an ArithmeticExpander. The operands are restricted by the following contraints:
         *
         * <ul>
         * <li>there are exactly two operands</li>
         * <li>both are exact numeric types</li>
         * </ul>
         */
        private void analyzeOperands( List<RexNode> operands ) {
            assert operands.size() == 2;
            typeA = operands.get( 0 ).getType();
            typeB = operands.get( 1 ).getType();
            assert PolyTypeUtil.isExactNumeric( typeA ) && PolyTypeUtil.isExactNumeric( typeB );

            scaleA = typeA.getScale();
            scaleB = typeB.getScale();
        }


        private RexNode expandPlusMinus( RexCall call, List<RexNode> operands ) {
            AlgDataType outType = call.getType();
            int outScale = outType.getScale();
            return encodeValue(
                    builder.makeCall(
                            call.getOperator(),
                            ensureScale(
                                    accessValue( operands.get( 0 ) ),
                                    scaleA,
                                    outScale ),
                            ensureScale(
                                    accessValue( operands.get( 1 ) ),
                                    scaleB,
                                    outScale ) ),
                    outType );
        }


        private RexNode expandDivide( RexCall call, List<RexNode> operands ) {
            AlgDataType outType = call.getType();
            RexNode dividend =
                    builder.makeCall(
                            call.getOperator(),
                            ensureType(
                                    real8,
                                    accessValue( operands.get( 0 ) ) ),
                            ensureType(
                                    real8,
                                    accessValue( operands.get( 1 ) ) ) );
            int scaleDifference = outType.getScale() - scaleA + scaleB;
            RexNode rescale =
                    builder.makeCall(
                            OperatorRegistry.get( OperatorName.MULTIPLY ),
                            dividend,
                            makeApproxScaleFactor( scaleDifference ) );
            return encodeValue( rescale, outType );
        }


        private RexNode expandTimes( RexCall call, List<RexNode> operands ) {
            // Multiplying the internal values of the two arguments leads to a number with scale = scaleA + scaleB. If the result type has a lower scale, then the number should be scaled down.
            int divisor = scaleA + scaleB - call.getType().getScale();

            if ( builder.getTypeFactory().useDoubleMultiplication( typeA, typeB ) ) {
                // Approximate implementation:
                // cast (a as double) * cast (b as double)
                //     / 10^divisor
                RexNode division =
                        makeDivide(
                                makeMultiply(
                                        ensureType( real8, accessValue( operands.get( 0 ) ) ),
                                        ensureType( real8, accessValue( operands.get( 1 ) ) ) ),
                                makeApproxLiteral( BigDecimal.TEN.pow( divisor ) ) );
                return encodeValue( division, call.getType(), true );
            } else {
                // Exact implementation: scaleDown(a * b)
                return encodeValue(
                        scaleDown(
                                builder.makeCall(
                                        call.getOperator(),
                                        accessValue( operands.get( 0 ) ),
                                        accessValue( operands.get( 1 ) ) ),
                                divisor ),
                        call.getType() );
            }
        }


        private RexNode expandComparison( RexCall call, List<RexNode> operands ) {
            int commonScale = Math.max( scaleA, scaleB );
            return builder.makeCall(
                    call.getOperator(),
                    ensureScale(
                            accessValue( operands.get( 0 ) ),
                            scaleA,
                            commonScale ),
                    ensureScale(
                            accessValue( operands.get( 1 ) ),
                            scaleB,
                            commonScale ) );
        }


        private RexNode expandMod( RexCall call, List<RexNode> operands ) {
            assert PolyTypeUtil.isExactNumeric( typeA );
            assert PolyTypeUtil.isExactNumeric( typeB );
            if ( scaleA != 0 || scaleB != 0 ) {
                throw RESOURCE.argumentMustHaveScaleZero( call.getOperator().getName() ).ex();
            }
            RexNode result =
                    builder.makeCall(
                            call.getOperator(),
                            accessValue( operands.get( 0 ) ),
                            accessValue( operands.get( 1 ) ) );
            AlgDataType retType = call.getType();
            if ( PolyTypeUtil.isDecimal( retType ) ) {
                return encodeValue( result, retType );
            }
            return ensureType( call.getType(), result );
        }

    }


    /**
     * Expander that rewrites floor(decimal) expressions:
     *
     * <blockquote><pre>
     * if (value &lt; 0)
     *     (value - 0.99...) / (10^scale)
     * else
     *     value / (10 ^ scale)
     * </pre></blockquote>
     */
    private static class FloorExpander extends RexExpander {

        FloorExpander( RexBuilder rexBuilder ) {
            super( rexBuilder );
        }


        @Override
        public RexNode expand( RexCall call ) {
            assert call.getOperator().getOperatorName() == OperatorName.FLOOR;
            RexNode decValue = call.operands.get( 0 );
            int scale = decValue.getType().getScale();
            RexNode value = decodeValue( decValue );
            final AlgDataTypeSystem typeSystem = builder.getTypeFactory().getTypeSystem();

            RexNode rewrite;
            if ( scale == 0 ) {
                rewrite = decValue;
            } else if ( scale == typeSystem.getMaxNumericPrecision() ) {
                rewrite =
                        makeCase(
                                makeIsNegative( value ),
                                makeExactLiteral( -1 ),
                                makeExactLiteral( 0 ) );
            } else {
                RexNode round = makeExactLiteral( 1 - powerOfTen( scale ) );
                RexNode scaleFactor = makeScaleFactor( scale );
                rewrite =
                        makeCase(
                                makeIsNegative( value ),
                                makeDivide(
                                        makePlus( value, round ),
                                        scaleFactor ),
                                makeDivide( value, scaleFactor ) );
            }
            return encodeValue( rewrite, call.getType() );
        }

    }


    /**
     * Expander that rewrites ceiling(decimal) expressions:
     *
     * <blockquote><pre>
     * if (value &gt; 0)
     *     (value + 0.99...) / (10 ^ scale)
     * else
     *     value / (10 ^ scale)
     * </pre></blockquote>
     */
    private static class CeilExpander extends RexExpander {

        CeilExpander( RexBuilder rexBuilder ) {
            super( rexBuilder );
        }


        @Override
        public RexNode expand( RexCall call ) {
            assert call.getOperator().getOperatorName() == OperatorName.CEIL;
            RexNode decValue = call.operands.get( 0 );
            int scale = decValue.getType().getScale();
            RexNode value = decodeValue( decValue );
            final AlgDataTypeSystem typeSystem = builder.getTypeFactory().getTypeSystem();

            RexNode rewrite;
            if ( scale == 0 ) {
                rewrite = decValue;
            } else if ( scale == typeSystem.getMaxNumericPrecision() ) {
                rewrite =
                        makeCase(
                                makeIsPositive( value ),
                                makeExactLiteral( 1 ),
                                makeExactLiteral( 0 ) );
            } else {
                RexNode round = makeExactLiteral( powerOfTen( scale ) - 1 );
                RexNode scaleFactor = makeScaleFactor( scale );
                rewrite =
                        makeCase(
                                makeIsPositive( value ),
                                makeDivide(
                                        makePlus( value, round ),
                                        scaleFactor ),
                                makeDivide( value, scaleFactor ) );
            }
            return encodeValue( rewrite, call.getType() );
        }

    }


    /**
     * Expander that rewrites case expressions, in place. Starting from:
     *
     * <blockquote><pre>(when $cond then $val)+ else $default</pre></blockquote>
     *
     * This expander casts all values to the return type. If the target type is a decimal, then the values are then decoded. The result of expansion is that the case operator no longer deals with decimals args.
     * (The return value is encoded if necessary.)
     * <p>
     * Note: a decimal type is returned iff arguments have decimals.
     */
    private static class CaseExpander extends RexExpander {

        CaseExpander( RexBuilder rexBuilder ) {
            super( rexBuilder );
        }


        @Override
        public RexNode expand( RexCall call ) {
            AlgDataType retType = call.getType();
            int argCount = call.operands.size();
            ImmutableList.Builder<RexNode> opBuilder = ImmutableList.builder();

            for ( int i = 0; i < argCount; i++ ) {
                // skip case conditions
                if ( ((i % 2) == 0) && (i != (argCount - 1)) ) {
                    opBuilder.add( call.operands.get( i ) );
                    continue;
                }
                RexNode expr = ensureType( retType, call.operands.get( i ), false );
                if ( PolyTypeUtil.isDecimal( retType ) ) {
                    expr = decodeValue( expr );
                }
                opBuilder.add( expr );
            }

            RexNode newCall = builder.makeCall( retType, call.getOperator(), opBuilder.build() );
            if ( PolyTypeUtil.isDecimal( retType ) ) {
                newCall = encodeValue( newCall, retType );
            }
            return newCall;
        }

    }


    /**
     * An expander that substitutes decimals with their integer representations. If the output is decimal, the output is reinterpreted from the integer representation into a decimal.
     */
    private static class PassThroughExpander extends RexExpander {

        PassThroughExpander( RexBuilder builder ) {
            super( builder );
        }


        @Override
        public boolean canExpand( RexCall call ) {
            return RexUtil.requiresDecimalExpansion( call, false );
        }


        @Override
        public RexNode expand( RexCall call ) {
            ImmutableList.Builder<RexNode> opBuilder = ImmutableList.builder();
            for ( RexNode operand : call.operands ) {
                if ( PolyTypeUtil.isNumeric( operand.getType() ) ) {
                    opBuilder.add( accessValue( operand ) );
                } else {
                    opBuilder.add( operand );
                }
            }

            RexNode newCall = builder.makeCall( call.getType(), call.getOperator(), opBuilder.build() );
            if ( PolyTypeUtil.isDecimal( call.getType() ) ) {
                return encodeValue( newCall, call.getType() );
            } else {
                return newCall;
            }
        }

    }


    /**
     * An expander which casts decimal arguments as doubles
     */
    private class CastArgAsDoubleExpander extends CastArgAsTypeExpander {

        CastArgAsDoubleExpander( RexBuilder builder ) {
            super( builder );
        }


        @Override
        public AlgDataType getArgType( RexCall call, int ordinal ) {
            AlgDataType type = real8;
            if ( call.operands.get( ordinal ).getType().isNullable() ) {
                type = builder.getTypeFactory().createTypeWithNullability( type, true );
            }
            return type;
        }

    }


    /**
     * An expander which casts decimal arguments as another type
     */
    private abstract static class CastArgAsTypeExpander extends RexExpander {

        private CastArgAsTypeExpander( RexBuilder builder ) {
            super( builder );
        }


        public abstract AlgDataType getArgType( RexCall call, int ordinal );


        @Override
        public RexNode expand( RexCall call ) {
            ImmutableList.Builder<RexNode> opBuilder = ImmutableList.builder();

            for ( Ord<RexNode> operand : Ord.zip( call.operands ) ) {
                AlgDataType targetType = getArgType( call, operand.i );
                if ( PolyTypeUtil.isDecimal( operand.e.getType() ) ) {
                    opBuilder.add( ensureType( targetType, operand.e, true ) );
                } else {
                    opBuilder.add( operand.e );
                }
            }

            RexNode ret = builder.makeCall( call.getType(), call.getOperator(), opBuilder.build() );
            ret = ensureType( call.getType(), ret, true );
            return ret;
        }

    }


    /**
     * This expander simplifies reinterpret calls. Consider (1.0+1)*1. The inner operation encodes a decimal (Reinterpret(...)) which the outer operation immediately decodes: (Reinterpret(Reinterpret(...))).
     * Arithmetic overflow is handled by underlying integer operations, so we don't have to consider it. Simply remove the nested Reinterpret.
     */
    private static class ReinterpretExpander extends RexExpander {

        ReinterpretExpander( RexBuilder builder ) {
            super( builder );
        }


        @Override
        public boolean canExpand( RexCall call ) {
            return call.isA( Kind.REINTERPRET ) && call.operands.get( 0 ).isA( Kind.REINTERPRET );
        }


        @Override
        public RexNode expand( RexCall call ) {
            List<RexNode> operands = call.operands;
            RexCall subCall = (RexCall) operands.get( 0 );
            RexNode innerValue = subCall.operands.get( 0 );
            if ( canSimplify( call, subCall, innerValue ) ) {
                return innerValue;
            }
            return call;
        }


        /**
         * Detect, in a generic, but strict way, whether it is possible to simplify a reinterpret cast. The rules are as follows:
         *
         * <ol>
         * <li>If value is not the same basic type as outer, then we cannot simplify</li>
         * <li>If the value is nullable but the inner or outer are not, then we cannot simplify.</li>
         * <li>If inner is nullable but outer is not, we cannot simplify.</li>
         * <li>If an overflow check is required from either inner or outer, we cannot simplify.</li>
         * <li>Otherwise, given the same type, and sufficient nullability constraints, we can simplify.</li>
         * </ol>
         *
         * @param outer outer call to reinterpret
         * @param inner inner call to reinterpret
         * @param value inner value
         * @return whether the two reinterpret casts can be removed
         */
        private boolean canSimplify( RexCall outer, RexCall inner, RexNode value ) {
            AlgDataType outerType = outer.getType();
            AlgDataType innerType = inner.getType();
            AlgDataType valueType = value.getType();
            boolean outerCheck = RexUtil.canReinterpretOverflow( outer );
            boolean innerCheck = RexUtil.canReinterpretOverflow( inner );

            if ( (outerType.getPolyType() != valueType.getPolyType())
                    || (outerType.getPrecision() != valueType.getPrecision())
                    || (outerType.getScale() != valueType.getScale()) ) {
                return false;
            }
            if ( valueType.isNullable() && (!innerType.isNullable() || !outerType.isNullable()) ) {
                return false;
            }
            if ( innerType.isNullable() && !outerType.isNullable() ) {
                return false;
            }

            // One would think that we could go from Nullable -> Not Nullable since we are substituting a general type with a more specific type. However, the optimizer doesn't like it.
            if ( valueType.isNullable() != outerType.isNullable() ) {
                return false;
            }
            return !innerCheck && !outerCheck;
        }

    }

}

