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


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.CompositeList;
import org.polypheny.db.util.Util;


/**
 * Planner rule that reduces aggregate functions in {@link org.polypheny.db.algebra.core.Aggregate}s to simpler forms.
 * <p>
 * Rewrites:
 * <ul>
 * <li>AVG(x) &rarr; SUM(x) / COUNT(x)</li>
 * <li>STDDEV_POP(x) &rarr; SQRT( (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x)) / COUNT(x))</li>
 * <li>STDDEV_SAMP(x) &rarr; SQRT( (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x)) / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)</li>
 * <li>VAR_POP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x)) / COUNT(x)</li>
 * <li>VAR_SAMP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x)) / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END</li>
 * <li>COVAR_POP(x, y) &rarr; (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)</li>
 * <li>COVAR_SAMP(x, y) &rarr; (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / CASE REGR_COUNT(x, y) WHEN 1 THEN NULL ELSE REGR_COUNT(x, y) - 1 END</li>
 * <li>REGR_SXX(x, y) &rarr; REGR_COUNT(x, y) * VAR_POP(y)</li>
 * <li>REGR_SYY(x, y) &rarr; REGR_COUNT(x, y) * VAR_POP(x)</li>
 * </ul>
 *
 * Since many of these rewrites introduce multiple occurrences of simpler forms like {@code COUNT(x)}, the rule gathers common sub-expressions as it goes.
 */
public class AggregateReduceFunctionsRule extends AlgOptRule {

    /**
     * The singleton.
     */
    public static final AggregateReduceFunctionsRule INSTANCE = new AggregateReduceFunctionsRule( operand( LogicalRelAggregate.class, any() ), AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates an AggregateReduceFunctionsRule.
     */
    public AggregateReduceFunctionsRule( AlgOptRuleOperand operand, AlgBuilderFactory algBuilderFactory ) {
        super( operand, algBuilderFactory, null );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        if ( !super.matches( call ) ) {
            return false;
        }
        Aggregate oldAggRel = (Aggregate) call.algs[0];
        return containsAvgStddevVarCall( oldAggRel.getAggCallList() );
    }


    @Override
    public void onMatch( AlgOptRuleCall ruleCall ) {
        Aggregate oldAggAlg = (Aggregate) ruleCall.algs[0];
        reduceAggs( ruleCall, oldAggAlg );
    }


    /**
     * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
     *
     * @param aggCallList List of aggregate calls
     */
    private boolean containsAvgStddevVarCall( List<AggregateCall> aggCallList ) {
        for ( AggregateCall call : aggCallList ) {
            if ( isReducible( call.getAggregation().getKind() ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns whether the aggregate call is a reducible function
     */
    private boolean isReducible( final Kind kind ) {
        if ( Kind.AVG_AGG_FUNCTIONS.contains( kind ) || Kind.COVAR_AVG_AGG_FUNCTIONS.contains( kind ) ) {
            return true;
        }
        return Objects.requireNonNull( kind ) == Kind.SUM;
    }


    /**
     * Reduces all calls to AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP in the aggregates list to.
     * <p>
     * It handles newly generated common subexpressions since this was done at the sql2alg stage.
     */
    private void reduceAggs( AlgOptRuleCall ruleCall, Aggregate oldAggAlg ) {
        RexBuilder rexBuilder = oldAggAlg.getCluster().getRexBuilder();

        List<AggregateCall> oldCalls = oldAggAlg.getAggCallList();
        final int groupCount = oldAggAlg.getGroupCount();
        final int indicatorCount = oldAggAlg.getIndicatorCount();

        final List<AggregateCall> newCalls = new ArrayList<>();
        final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

        final List<RexNode> projList = new ArrayList<>();

        // pass through group key (+ indicators if present)
        for ( int i = 0; i < groupCount + indicatorCount; ++i ) {
            projList.add( rexBuilder.makeInputRef( getFieldType( oldAggAlg, i ), i ) );
        }

        // List of input expressions. If a particular aggregate needs more, it will add an expression to the end, and we will create an extra project.
        final AlgBuilder algBuilder = ruleCall.builder();
        algBuilder.push( oldAggAlg.getInput() );
        final List<RexNode> inputExprs = new ArrayList<>( algBuilder.fields() );

        // create new agg function calls and rest of project list together
        for ( AggregateCall oldCall : oldCalls ) {
            projList.add( reduceAgg( oldAggAlg, oldCall, newCalls, aggCallMapping, inputExprs ) );
        }

        final int extraArgCount = inputExprs.size() - algBuilder.peek().getTupleType().getFieldCount();
        if ( extraArgCount > 0 ) {
            algBuilder.project(
                    inputExprs,
                    CompositeList.of(
                            algBuilder.peek().getTupleType().getFieldNames(),
                            Collections.nCopies( extraArgCount, null ) ) );
        }
        newAggregateAlg( algBuilder, oldAggAlg, newCalls );

        newCalcAlg( algBuilder, oldAggAlg.getTupleType(), projList );
        ruleCall.transformTo( algBuilder.build() );
    }


    private RexNode reduceAgg( Aggregate oldAggAlg, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs ) {
        final Kind kind = oldCall.getAggregation().getKind();
        if ( isReducible( kind ) ) {
            final Integer y;
            final Integer x;
            switch ( kind ) {
                case SUM:
                    // replace original SUM(x) with case COUNT(x) when 0 then null else SUM0(x) end
                    return reduceSum( oldAggAlg, oldCall, newCalls, aggCallMapping );
                case AVG:
                    // replace original AVG(x) with SUM(x) / COUNT(x)
                    return reduceAvg( oldAggAlg, oldCall, newCalls, aggCallMapping, inputExprs );
                case COVAR_POP:
                    // replace original COVAR_POP(x, y) with
                    //     (SUM(x * y) - SUM(y) * SUM(y) / COUNT(x))
                    //     / COUNT(x))
                    return reduceCovariance( oldAggAlg, oldCall, true, newCalls, aggCallMapping, inputExprs );
                case COVAR_SAMP:
                    // replace original COVAR_SAMP(x, y) with
                    //   SQRT(
                    //     (SUM(x * y) - SUM(x) * SUM(y) / COUNT(x))
                    //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
                    return reduceCovariance( oldAggAlg, oldCall, false, newCalls, aggCallMapping, inputExprs );
                case REGR_SXX:
                    // replace original REGR_SXX(x, y) with REGR_COUNT(x, y) * VAR_POP(y)
                    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
                    x = oldCall.getArgList().get( 0 );
                    y = oldCall.getArgList().get( 1 );
                    //noinspection SuspiciousNameCombination
                    return reduceRegrSzz( oldAggAlg, oldCall, newCalls, aggCallMapping, inputExprs, y, y, x );
                case REGR_SYY:
                    // replace original REGR_SYY(x, y) with REGR_COUNT(x, y) * VAR_POP(x)
                    assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
                    x = oldCall.getArgList().get( 0 );
                    y = oldCall.getArgList().get( 1 );
                    //noinspection SuspiciousNameCombination
                    return reduceRegrSzz( oldAggAlg, oldCall, newCalls, aggCallMapping, inputExprs, x, x, y );
                case STDDEV_POP:
                    // replace original STDDEV_POP(x) with
                    //   SQRT(
                    //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
                    //     / COUNT(x))
                    return reduceStddev( oldAggAlg, oldCall, true, true, newCalls, aggCallMapping, inputExprs );
                case STDDEV_SAMP:
                    // replace original STDDEV_POP(x) with
                    //   SQRT(
                    //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
                    //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
                    return reduceStddev( oldAggAlg, oldCall, false, true, newCalls, aggCallMapping, inputExprs );
                case VAR_POP:
                    // replace original VAR_POP(x) with
                    //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
                    //     / COUNT(x)
                    return reduceStddev( oldAggAlg, oldCall, true, false, newCalls, aggCallMapping, inputExprs );
                case VAR_SAMP:
                    // replace original VAR_POP(x) with
                    //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
                    //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
                    return reduceStddev( oldAggAlg, oldCall, false, false, newCalls, aggCallMapping, inputExprs );
                default:
                    throw Util.unexpected( kind );
            }
        } else {
            // anything else:  preserve original call
            RexBuilder rexBuilder = oldAggAlg.getCluster().getRexBuilder();
            final int nGroups = oldAggAlg.getGroupCount();
            List<AlgDataType> oldArgTypes = PolyTypeUtil.projectTypes( oldAggAlg.getInput().getTupleType(), oldCall.getArgList() );
            return rexBuilder.addAggCall( oldCall, nGroups, oldAggAlg.indicator, newCalls, aggCallMapping, oldArgTypes );
        }
    }


    private AggregateCall createAggregateCallWithBinding( AlgDataTypeFactory typeFactory, AggFunction aggFunction, AlgDataType operandType, Aggregate oldAggRel, AggregateCall oldCall, int argOrdinal, int filter ) {
        final Aggregate.AggCallBinding binding = new Aggregate.AggCallBinding(
                typeFactory,
                aggFunction,
                ImmutableList.of( operandType ),
                oldAggRel.getGroupCount(),
                filter >= 0 );
        return AggregateCall.create(
                aggFunction,
                oldCall.isDistinct(),
                oldCall.isApproximate(),
                ImmutableList.of( argOrdinal ),
                filter,
                oldCall.collation,
                aggFunction.inferReturnType( binding ),
                null );
    }


    private RexNode reduceAvg( Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs ) {
        final int nGroups = oldAggRel.getGroupCount();
        final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
        final int iAvgInput = oldCall.getArgList().get( 0 );
        final AlgDataType avgInputType = getFieldType( oldAggRel.getInput(), iAvgInput );
        final AggregateCall sumCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.SUM ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        oldCall.getArgList(),
                        oldCall.filterArg,
                        oldCall.collation,
                        oldAggRel.getGroupCount(),
                        oldAggRel.getInput(),
                        null,
                        null );
        final AggregateCall countCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.COUNT ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        oldCall.getArgList(),
                        oldCall.filterArg,
                        oldCall.collation,
                        oldAggRel.getGroupCount(),
                        oldAggRel.getInput(),
                        null,
                        null );

        // NOTE:  these references are with respect to the output of newAggRel
        RexNode numeratorRef =
                rexBuilder.addAggCall(
                        sumCall,
                        nGroups,
                        oldAggRel.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( avgInputType ) );
        final RexNode denominatorRef =
                rexBuilder.addAggCall(
                        countCall,
                        nGroups,
                        oldAggRel.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( avgInputType ) );

        final AlgDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
        final AlgDataType avgType = typeFactory.createTypeWithNullability( oldCall.getType(), numeratorRef.getType().isNullable() );
        numeratorRef = rexBuilder.ensureType( avgType, numeratorRef, true );
        final RexNode divideRef = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), numeratorRef, denominatorRef );
        return rexBuilder.makeCast( oldCall.getType(), divideRef );
    }


    private RexNode reduceSum( Aggregate oldAggAlg, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping ) {
        final int nGroups = oldAggAlg.getGroupCount();
        RexBuilder rexBuilder = oldAggAlg.getCluster().getRexBuilder();
        int arg = oldCall.getArgList().get( 0 );
        AlgDataType argType = getFieldType( oldAggAlg.getInput(), arg );

        final AggregateCall sumZeroCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.SUM0 ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        oldCall.getArgList(),
                        oldCall.filterArg,
                        oldCall.collation,
                        oldAggAlg.getGroupCount(),
                        oldAggAlg.getInput(),
                        null,
                        oldCall.name );
        final AggregateCall countCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.COUNT ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        oldCall.getArgList(),
                        oldCall.filterArg,
                        oldCall.collation,
                        oldAggAlg.getGroupCount(),
                        oldAggAlg.getInput(),
                        null,
                        null );

        // NOTE:  these references are with respect to the output of newAggAlg
        RexNode sumZeroRef =
                rexBuilder.addAggCall(
                        sumZeroCall,
                        nGroups,
                        oldAggAlg.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( argType ) );
        if ( !oldCall.getType().isNullable() ) {
            // If SUM(x) is not nullable, the validator must have determined that nulls are impossible (because the group is never empty and x is never null). Therefore we translate to SUM0(x).
            return sumZeroRef;
        }
        RexNode countRef =
                rexBuilder.addAggCall(
                        countCall,
                        nGroups,
                        oldAggAlg.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( argType ) );
        return rexBuilder.makeCall(
                OperatorRegistry.get( OperatorName.CASE ),
                rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), countRef, rexBuilder.makeExactLiteral( BigDecimal.ZERO ) ),
                rexBuilder.makeCast( sumZeroRef.getType(), rexBuilder.constantNull() ), sumZeroRef );
    }


    private RexNode reduceStddev( Aggregate oldAggRel, AggregateCall oldCall, boolean biased, boolean sqrt, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs ) {
        // stddev_pop(x) ==>
        //   power(
        //     (sum(x * x) - sum(x) * sum(x) / count(x))
        //     / count(x),
        //     .5)
        //
        // stddev_samp(x) ==>
        //   power(
        //     (sum(x * x) - sum(x) * sum(x) / count(x))
        //     / nullif(count(x) - 1, 0),
        //     .5)
        final int nGroups = oldAggRel.getGroupCount();
        final AlgCluster cluster = oldAggRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();

        assert oldCall.getArgList().size() == 1 : oldCall.getArgList();
        final int argOrdinal = oldCall.getArgList().get( 0 );
        final AlgDataType argOrdinalType = getFieldType( oldAggRel.getInput(), argOrdinal );
        final AlgDataType oldCallType = typeFactory.createTypeWithNullability( oldCall.getType(), argOrdinalType.isNullable() );

        final RexNode argRef = rexBuilder.ensureType( oldCallType, inputExprs.get( argOrdinal ), true );

        final RexNode argSquared = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), argRef, argRef );
        final int argSquaredOrdinal = lookupOrAdd( inputExprs, argSquared );

        final AggregateCall sumArgSquaredAggCall =
                createAggregateCallWithBinding(
                        typeFactory,
                        OperatorRegistry.getAgg( OperatorName.SUM ),
                        argSquared.getType(),
                        oldAggRel,
                        oldCall,
                        argSquaredOrdinal,
                        -1 );

        final RexNode sumArgSquared =
                rexBuilder.addAggCall(
                        sumArgSquaredAggCall,
                        nGroups,
                        oldAggRel.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( sumArgSquaredAggCall.getType() ) );

        final AggregateCall sumArgAggCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.SUM ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        ImmutableList.of( argOrdinal ),
                        oldCall.filterArg,
                        oldCall.collation,
                        oldAggRel.getGroupCount(),
                        oldAggRel.getInput(),
                        null,
                        null );

        final RexNode sumArg =
                rexBuilder.addAggCall(
                        sumArgAggCall,
                        nGroups,
                        oldAggRel.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( sumArgAggCall.getType() ) );
        final RexNode sumArgCast = rexBuilder.ensureType( oldCallType, sumArg, true );
        final RexNode sumSquaredArg = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), sumArgCast, sumArgCast );

        final AggregateCall countArgAggCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.COUNT ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        oldCall.getArgList(),
                        oldCall.filterArg,
                        oldCall.collation,
                        oldAggRel.getGroupCount(),
                        oldAggRel,
                        null,
                        null );

        final RexNode countArg =
                rexBuilder.addAggCall(
                        countArgAggCall,
                        nGroups,
                        oldAggRel.indicator,
                        newCalls,
                        aggCallMapping,
                        ImmutableList.of( argOrdinalType ) );

        final RexNode avgSumSquaredArg = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), sumSquaredArg, countArg );

        final RexNode diff = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), sumArgSquared, avgSumSquaredArg );

        final RexNode denominator;
        if ( biased ) {
            denominator = countArg;
        } else {
            final RexLiteral one = rexBuilder.makeExactLiteral( BigDecimal.ONE );
            final RexNode nul = rexBuilder.makeCast( countArg.getType(), rexBuilder.constantNull() );
            final RexNode countMinusOne = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), countArg, one );
            final RexNode countEqOne = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), countArg, one );
            denominator = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.CASE ), countEqOne, nul, countMinusOne );
        }

        final RexNode div = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), diff, denominator );

        RexNode result = div;
        if ( sqrt ) {
            final RexNode half = rexBuilder.makeExactLiteral( new BigDecimal( "0.5" ) );
            result = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.POWER ), div, half );
        }

        return rexBuilder.makeCast( oldCall.getType(), result );
    }


    private RexNode getSumAggregatedRexNode( Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, RexBuilder rexBuilder, int argOrdinal, int filterArg ) {
        final AggregateCall aggregateCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.SUM ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        ImmutableList.of( argOrdinal ),
                        filterArg,
                        oldCall.collation,
                        oldAggRel.getGroupCount(),
                        oldAggRel.getInput(),
                        null,
                        null );
        return rexBuilder.addAggCall(
                aggregateCall,
                oldAggRel.getGroupCount(),
                oldAggRel.indicator,
                newCalls,
                aggCallMapping,
                ImmutableList.of( aggregateCall.getType() ) );
    }


    private RexNode getSumAggregatedRexNodeWithBinding( Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, AlgDataType operandType, int argOrdinal, int filter ) {
        AlgCluster cluster = oldAggRel.getCluster();
        final AggregateCall sumArgSquaredAggCall = createAggregateCallWithBinding( cluster.getTypeFactory(), OperatorRegistry.getAgg( OperatorName.SUM ), operandType, oldAggRel, oldCall, argOrdinal, filter );

        return cluster.getRexBuilder().addAggCall( sumArgSquaredAggCall, oldAggRel.getGroupCount(), oldAggRel.indicator, newCalls, aggCallMapping, ImmutableList.of( sumArgSquaredAggCall.getType() ) );
    }


    private RexNode getRegrCountRexNode( Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, ImmutableList<Integer> argOrdinals, ImmutableList<AlgDataType> operandTypes, int filterArg ) {
        final AggregateCall countArgAggCall =
                AggregateCall.create(
                        OperatorRegistry.getAgg( OperatorName.REGR_COUNT ),
                        oldCall.isDistinct(),
                        oldCall.isApproximate(),
                        argOrdinals,
                        filterArg,
                        oldCall.collation,
                        oldAggRel.getGroupCount(),
                        oldAggRel,
                        null,
                        null );

        return oldAggRel.getCluster().getRexBuilder().addAggCall(
                countArgAggCall,
                oldAggRel.getGroupCount(),
                oldAggRel.indicator,
                newCalls,
                aggCallMapping,
                operandTypes );
    }


    private RexNode reduceRegrSzz( Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs, int xIndex, int yIndex, int nullFilterIndex ) {
        // regr_sxx(x, y) ==>
        //    sum(y * y, x) - sum(y, x) * sum(y, x) / regr_count(x, y)
        //

        final AlgCluster cluster = oldAggRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
        final AlgDataType argXType = getFieldType( oldAggRel.getInput(), xIndex );
        final AlgDataType argYType = xIndex == yIndex ? argXType : getFieldType( oldAggRel.getInput(), yIndex );
        final AlgDataType nullFilterIndexType = nullFilterIndex == yIndex ? argYType : getFieldType( oldAggRel.getInput(), yIndex );

        final AlgDataType oldCallType = typeFactory.createTypeWithNullability( oldCall.getType(), argXType.isNullable() || argYType.isNullable() || nullFilterIndexType.isNullable() );

        final RexNode argX = rexBuilder.ensureType( oldCallType, inputExprs.get( xIndex ), true );
        final RexNode argY = rexBuilder.ensureType( oldCallType, inputExprs.get( yIndex ), true );
        final RexNode argNullFilter = rexBuilder.ensureType( oldCallType, inputExprs.get( nullFilterIndex ), true );

        final RexNode argXArgY = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), argX, argY );
        final int argSquaredOrdinal = lookupOrAdd( inputExprs, argXArgY );

        final RexNode argXAndYNotNullFilter = rexBuilder.makeCall(
                OperatorRegistry.get( OperatorName.AND ),
                rexBuilder.makeCall(
                        OperatorRegistry.get( OperatorName.AND ),
                        rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), argX ),
                        rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), argY ) ),
                rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), argNullFilter ) );
        final int argXAndYNotNullFilterOrdinal = lookupOrAdd( inputExprs, argXAndYNotNullFilter );
        final RexNode sumXY = getSumAggregatedRexNodeWithBinding( oldAggRel, oldCall, newCalls, aggCallMapping, argXArgY.getType(), argSquaredOrdinal, argXAndYNotNullFilterOrdinal );
        final RexNode sumXYCast = rexBuilder.ensureType( oldCallType, sumXY, true );

        final RexNode sumX = getSumAggregatedRexNode( oldAggRel, oldCall, newCalls, aggCallMapping, rexBuilder, xIndex, argXAndYNotNullFilterOrdinal );
        final RexNode sumY =
                xIndex == yIndex
                        ? sumX
                        : getSumAggregatedRexNode( oldAggRel, oldCall, newCalls, aggCallMapping, rexBuilder, yIndex, argXAndYNotNullFilterOrdinal );

        final RexNode sumXSumY = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), sumX, sumY );

        final RexNode countArg = getRegrCountRexNode( oldAggRel, oldCall, newCalls, aggCallMapping, ImmutableList.of( xIndex ), ImmutableList.of( argXType ), argXAndYNotNullFilterOrdinal );

        RexLiteral zero = rexBuilder.makeExactLiteral( BigDecimal.ZERO );
        RexNode nul = rexBuilder.constantNull();
        final RexNode avgSumXSumY = rexBuilder.makeCall(
                OperatorRegistry.get( OperatorName.CASE ),
                rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), countArg, zero ),
                nul,
                rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), sumXSumY, countArg ) );
        final RexNode avgSumXSumYCast = rexBuilder.ensureType( oldCallType, avgSumXSumY, true );
        final RexNode result = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), sumXYCast, avgSumXSumYCast );
        return rexBuilder.makeCast( oldCall.getType(), result );
    }


    private RexNode reduceCovariance( Aggregate oldAggRel, AggregateCall oldCall, boolean biased, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs ) {
        // covar_pop(x, y) ==>
        //     (sum(x * y) - sum(x) * sum(y) / regr_count(x, y))
        //     / regr_count(x, y)
        //
        // covar_samp(x, y) ==>
        //     (sum(x * y) - sum(x) * sum(y) / regr_count(x, y))
        //     / regr_count(count(x, y) - 1, 0)
        final AlgCluster cluster = oldAggRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
        assert oldCall.getArgList().size() == 2 : oldCall.getArgList();
        final int argXOrdinal = oldCall.getArgList().get( 0 );
        final int argYOrdinal = oldCall.getArgList().get( 1 );
        final AlgDataType argXOrdinalType = getFieldType( oldAggRel.getInput(), argXOrdinal );
        final AlgDataType argYOrdinalType = getFieldType( oldAggRel.getInput(), argYOrdinal );
        final AlgDataType oldCallType = typeFactory.createTypeWithNullability( oldCall.getType(), argXOrdinalType.isNullable() || argYOrdinalType.isNullable() );
        final RexNode argX = rexBuilder.ensureType( oldCallType, inputExprs.get( argXOrdinal ), true );
        final RexNode argY = rexBuilder.ensureType( oldCallType, inputExprs.get( argYOrdinal ), true );
        final RexNode argXAndYNotNullFilter = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), argX ), rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), argY ) );
        final int argXAndYNotNullFilterOrdinal = lookupOrAdd( inputExprs, argXAndYNotNullFilter );
        final RexNode argXY = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), argX, argY );
        final int argXYOrdinal = lookupOrAdd( inputExprs, argXY );
        final RexNode sumXY = getSumAggregatedRexNodeWithBinding( oldAggRel, oldCall, newCalls, aggCallMapping, argXY.getType(), argXYOrdinal, argXAndYNotNullFilterOrdinal );
        final RexNode sumX = getSumAggregatedRexNode( oldAggRel, oldCall, newCalls, aggCallMapping, rexBuilder, argXOrdinal, argXAndYNotNullFilterOrdinal );
        final RexNode sumY = getSumAggregatedRexNode( oldAggRel, oldCall, newCalls, aggCallMapping, rexBuilder, argYOrdinal, argXAndYNotNullFilterOrdinal );
        final RexNode sumXSumY = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), sumX, sumY );
        final RexNode countArg = getRegrCountRexNode(
                oldAggRel,
                oldCall,
                newCalls,
                aggCallMapping,
                ImmutableList.of( argXOrdinal, argYOrdinal ),
                ImmutableList.of( argXOrdinalType, argYOrdinalType ),
                argXAndYNotNullFilterOrdinal );
        final RexNode avgSumSquaredArg = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), sumXSumY, countArg );
        final RexNode diff = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), sumXY, avgSumSquaredArg );
        final RexNode denominator;
        if ( biased ) {
            denominator = countArg;
        } else {
            final RexLiteral one = rexBuilder.makeExactLiteral( BigDecimal.ONE );
            final RexNode nul = rexBuilder.makeCast( countArg.getType(), rexBuilder.constantNull() );
            final RexNode countMinusOne = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.MINUS ), countArg, one );
            final RexNode countEqOne = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), countArg, one );
            denominator = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.CASE ), countEqOne, nul, countMinusOne );
        }
        final RexNode result = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DIVIDE ), diff, denominator );
        return rexBuilder.makeCast( oldCall.getType(), result );
    }


    /**
     * Finds the ordinal of an element in a list, or adds it.
     *
     * @param list List
     * @param element Element to lookup or add
     * @param <T> Element type
     * @return Ordinal of element in list
     */
    private static <T> int lookupOrAdd( List<T> list, T element ) {
        int ordinal = list.indexOf( element );
        if ( ordinal == -1 ) {
            ordinal = list.size();
            list.add( element );
        }
        return ordinal;
    }


    /**
     * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored into Aggregate and subclasses - but it's only needed for some subclasses.
     *
     * @param algBuilder Builder of relational expressions; at the top of its stack is its input
     * @param oldAggregate LogicalAggregate to clone.
     * @param newCalls New list of AggregateCalls
     */
    protected void newAggregateAlg( AlgBuilder algBuilder, Aggregate oldAggregate, List<AggregateCall> newCalls ) {
        algBuilder.aggregate( algBuilder.groupKey( oldAggregate.getGroupSet(), oldAggregate.getGroupSets() ), newCalls );
    }


    /**
     * Add a calc with the expressions to compute the original agg calls from the decomposed ones.
     *
     * @param algBuilder Builder of relational expressions; at the top of its stack is its input
     * @param rowType The output row type of the original aggregate.
     * @param exprs The expressions to compute the original agg calls.
     */
    protected void newCalcAlg( AlgBuilder algBuilder, AlgDataType rowType, List<RexNode> exprs ) {
        algBuilder.project( exprs, rowType.getFieldNames() );
    }


    private AlgDataType getFieldType( AlgNode algNode, int i ) {
        final AlgDataTypeField inputField = algNode.getTupleType().getFields().get( i );
        return inputField.getType();
    }

}

