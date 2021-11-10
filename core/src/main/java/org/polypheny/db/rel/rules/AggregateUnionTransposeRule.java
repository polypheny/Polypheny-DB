/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.rules;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.core.SqlStdOperatorTable;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.metadata.RelMdUtil;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.fun.SqlAnyValueAggFunction;
import org.polypheny.db.sql.fun.SqlBitOpAggFunction;
import org.polypheny.db.sql.fun.SqlCountAggFunction;
import org.polypheny.db.sql.fun.SqlMinMaxAggFunction;
import org.polypheny.db.sql.fun.SqlSumAggFunction;
import org.polypheny.db.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Planner rule that pushes an {@link org.polypheny.db.rel.core.Aggregate} past a non-distinct {@link org.polypheny.db.rel.core.Union}.
 */
public class AggregateUnionTransposeRule extends RelOptRule {

    public static final AggregateUnionTransposeRule INSTANCE = new AggregateUnionTransposeRule( LogicalAggregate.class, LogicalUnion.class, RelFactories.LOGICAL_BUILDER );

    private static final Map<Class<? extends SqlAggFunction>, Boolean> SUPPORTED_AGGREGATES = new IdentityHashMap<>();


    static {
        SUPPORTED_AGGREGATES.put( SqlMinMaxAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SqlCountAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SqlSumAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SqlSumEmptyIsZeroAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SqlAnyValueAggFunction.class, true );
        SUPPORTED_AGGREGATES.put( SqlBitOpAggFunction.class, true );
    }


    /**
     * Creates an AggregateUnionTransposeRule.
     */
    public AggregateUnionTransposeRule( Class<? extends Aggregate> aggregateClass, Class<? extends Union> unionClass, RelBuilderFactory relBuilderFactory ) {
        super( operand( aggregateClass, operand( unionClass, any() ) ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        Aggregate aggRel = call.rel( 0 );
        Union union = call.rel( 1 );

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
        final RelBuilder relBuilder = call.builder();
        int transformCount = 0;
        final RelMetadataQuery mq = call.getMetadataQuery();
        for ( RelNode input : union.getInputs() ) {
            boolean alreadyUnique = RelMdUtil.areColumnsDefinitelyUnique( mq, input, aggRel.getGroupSet() );

            relBuilder.push( input );
            if ( !alreadyUnique ) {
                ++transformCount;
                relBuilder.aggregate( relBuilder.groupKey( aggRel.getGroupSet() ), aggRel.getAggCallList() );
            }
        }

        if ( transformCount == 0 ) {
            // none of the children could benefit from the push-down, so bail out (preventing the infinite loop to which most planners would succumb)
            return;
        }

        // create a new union whose children are the aggregates created above
        relBuilder.union( true, union.getInputs().size() );
        relBuilder.aggregate(
                relBuilder.groupKey( aggRel.getGroupSet(), aggRel.getGroupSets() ),
                transformedAggCalls );
        call.transformTo( relBuilder.build() );
    }


    private List<AggregateCall> transformAggCalls( RelNode input, int groupCount, List<AggregateCall> origCalls ) {
        final List<AggregateCall> newCalls = new ArrayList<>();
        for ( Ord<AggregateCall> ord : Ord.zip( origCalls ) ) {
            final AggregateCall origCall = ord.e;
            if ( origCall.isDistinct() || !SUPPORTED_AGGREGATES.containsKey( origCall.getAggregation().getClass() ) ) {
                return null;
            }
            final SqlAggFunction aggFun;
            final RelDataType aggType;
            if ( origCall.getAggregation() == SqlStdOperatorTable.COUNT ) {
                aggFun = SqlStdOperatorTable.SUM0;
                // count(any) is always not null, however nullability of sum might depend on the number of columns in GROUP BY.
                // Here we use SUM0 since we are sure we will not face nullable inputs nor we'll face empty set.
                aggType = null;
            } else {
                aggFun = origCall.getAggregation();
                aggType = origCall.getType();
            }
            AggregateCall newCall =
                    AggregateCall.create(
                            aggFun,
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

