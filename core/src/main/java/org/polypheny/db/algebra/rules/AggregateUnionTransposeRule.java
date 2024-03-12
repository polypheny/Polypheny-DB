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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.fun.AnyValueAggFunction;
import org.polypheny.db.algebra.fun.BitOpAggFunction;
import org.polypheny.db.algebra.fun.CountAggFunction;
import org.polypheny.db.algebra.fun.MinMaxAggFunction;
import org.polypheny.db.algebra.fun.SumAggFunction;
import org.polypheny.db.algebra.fun.SumEmptyIsZeroAggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that pushes an {@link org.polypheny.db.algebra.core.Aggregate} past a non-distinct {@link org.polypheny.db.algebra.core.Union}.
 */
public class AggregateUnionTransposeRule extends AlgOptRule {

    public static final AggregateUnionTransposeRule INSTANCE = new AggregateUnionTransposeRule( LogicalRelAggregate.class, LogicalRelUnion.class, AlgFactories.LOGICAL_BUILDER );

    private static final Map<Class<? extends AggFunction>, Boolean> SUPPORTED_AGGREGATES = new IdentityHashMap<>();


    static {
        SUPPORTED_AGGREGATES.put( MinMaxAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( CountAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SumAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SumEmptyIsZeroAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( AnyValueAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( BitOpAggFunction.class, true );
    }


    /**
     * Creates an AggregateUnionTransposeRule.
     */
    public AggregateUnionTransposeRule( Class<? extends Aggregate> aggregateClass, Class<? extends Union> unionClass, AlgBuilderFactory algBuilderFactory ) {
        super( operand( aggregateClass, operand( unionClass, any() ) ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Aggregate aggRel = call.alg( 0 );
        Union union = call.alg( 1 );

        if ( !union.all ) {
            // This transformation is only valid for UNION ALL.
            // Consider t1(i) with rows (5), (5) and t2(i) with rows (5), (10), and the query
            // select sum(i) from (select i from t1) union (select i from t2).
            // The correct answer is 15.  If we apply the transformation, we get
            // select sum(i) from (select sum(i) as i from t1) union (select sum(i) as i from t2)
            // which yields 25 (incorrect).
            return;
        }

        int groupCount = aggRel.getGroupSet().cardinality();

        List<AggregateCall> transformedAggCalls =
                transformAggCalls(
                        aggRel.copy( aggRel.getTraitSet(), aggRel.getInput(), false, aggRel.getGroupSet(), null, aggRel.getAggCallList() ),
                        groupCount, aggRel.getAggCallList() );
        if ( transformedAggCalls == null ) {
            // we've detected the presence of something like AVG, which we can't handle
            return;
        }

        // create corresponding aggregates on top of each union child
        final AlgBuilder algBuilder = call.builder();
        int transformCount = 0;
        final AlgMetadataQuery mq = call.getMetadataQuery();
        for ( AlgNode input : union.getInputs() ) {
            boolean alreadyUnique = AlgMdUtil.areColumnsDefinitelyUnique( mq, input, aggRel.getGroupSet() );

            algBuilder.push( input );
            if ( !alreadyUnique ) {
                ++transformCount;
                algBuilder.aggregate( algBuilder.groupKey( aggRel.getGroupSet() ), aggRel.getAggCallList() );
            }
        }

        if ( transformCount == 0 ) {
            // none of the children could benefit from the push-down, so bail out (preventing the infinite loop to which most planners would succumb)
            return;
        }

        // create a new union whose children are the aggregates created above
        algBuilder.union( true, union.getInputs().size() );
        algBuilder.aggregate(
                algBuilder.groupKey( aggRel.getGroupSet(), aggRel.getGroupSets() ),
                transformedAggCalls );
        call.transformTo( algBuilder.build() );
    }


    private List<AggregateCall> transformAggCalls( AlgNode input, int groupCount, List<AggregateCall> origCalls ) {
        final List<AggregateCall> newCalls = new ArrayList<>();
        for ( Ord<AggregateCall> ord : Ord.zip( origCalls ) ) {
            final AggregateCall origCall = ord.e;
            if ( origCall.isDistinct() || !SUPPORTED_AGGREGATES.containsKey( origCall.getAggregation().getClass() ) ) {
                return null;
            }
            final AggFunction aggFun;
            final AlgDataType aggType;
            if ( origCall.getAggregation().getOperatorName() == OperatorName.COUNT ) {
                aggFun = OperatorRegistry.getAgg( OperatorName.SUM0 );
                // count(any) is always not null, however nullability of sum might depend on the number of columns in GROUP BY.
                // Here we use SUM0 since we are sure we will not face nullable inputs nor we'll face empty set.
                aggType = null;
            } else {
                aggFun = origCall.getAggregation();
                aggType = origCall.getType();
            }
            AggregateCall newCall =
                    AggregateCall.create(
                            (Operator & AggFunction) aggFun,
                            origCall.isDistinct(),
                            origCall.isApproximate(),
                            ImmutableList.of( groupCount + ord.i ),
                            -1,
                            origCall.collation,
                            groupCount,
                            input,
                            aggType,
                            origCall.getName() );
            newCalls.add( newCall );
        }
        return newCalls;
    }

}

