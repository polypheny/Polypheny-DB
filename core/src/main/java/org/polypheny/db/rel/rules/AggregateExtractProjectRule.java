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
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptRuleOperand;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.MappingType;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Rule to extract a {@link Project} from an {@link org.polypheny.db.rel.core.Aggregate} and push it down towards the input.
 *
 * What projections can be safely pushed down depends upon which fields the Aggregate uses.
 *
 * To prevent cycles, this rule will not extract a {@code Project} if the {@code Aggregate}s input is already a {@code Project}.
 */
public class AggregateExtractProjectRule extends RelOptRule {

    /**
     * Creates an AggregateExtractProjectRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public AggregateExtractProjectRule( Class<? extends Aggregate> aggregateClass, Class<? extends RelNode> inputClass, RelBuilderFactory relBuilderFactory ) {
        // Predicate prevents matching against an Aggregate whose input is already a Project. Prevents this rule firing repeatedly.
        this( operand( aggregateClass, operandJ( inputClass, null, r -> !(r instanceof Project), any() ) ), relBuilderFactory );
    }


    public AggregateExtractProjectRule( RelOptRuleOperand operand, RelBuilderFactory builderFactory ) {
        super( operand, builderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Aggregate aggregate = call.rel( 0 );
        final RelNode input = call.rel( 1 );
        // Compute which input fields are used.
        // 1. group fields are always used
        final Builder inputFieldsUsed = aggregate.getGroupSet().rebuild();
        // 2. agg functions
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            for ( int i : aggCall.getArgList() ) {
                inputFieldsUsed.set( i );
            }
            if ( aggCall.filterArg >= 0 ) {
                inputFieldsUsed.set( aggCall.filterArg );
            }
        }
        final RelBuilder relBuilder = call.builder().push( input );
        final List<RexNode> projects = new ArrayList<>();
        final Mapping mapping = Mappings.create( MappingType.INVERSE_SURJECTION, aggregate.getInput().getRowType().getFieldCount(), inputFieldsUsed.cardinality() );
        int j = 0;
        for ( int i : inputFieldsUsed.build() ) {
            projects.add( relBuilder.field( i ) );
            mapping.set( i, j++ );
        }

        relBuilder.project( projects );

        final ImmutableBitSet newGroupSet = Mappings.apply( mapping, aggregate.getGroupSet() );

        final Iterable<ImmutableBitSet> newGroupSets = Iterables.transform( aggregate.getGroupSets(), bitSet -> Mappings.apply( mapping, bitSet ) );
        final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            final ImmutableList<RexNode> args = relBuilder.fields( Mappings.apply2( mapping, aggCall.getArgList() ) );
            final RexNode filterArg = aggCall.filterArg < 0 ? null : relBuilder.field( Mappings.apply( mapping, aggCall.filterArg ) );
            newAggCallList.add(
                    relBuilder.aggregateCall( aggCall.getAggregation(), args )
                            .distinct( aggCall.isDistinct() )
                            .filter( filterArg )
                            .approximate( aggCall.isApproximate() )
                            .sort( relBuilder.fields( aggCall.collation ) )
                            .as( aggCall.name ) );
        }

        final RelBuilder.GroupKey groupKey = relBuilder.groupKey( newGroupSet, newGroupSets );
        relBuilder.aggregate( groupKey, newAggCallList );
        call.transformTo( relBuilder.build() );
    }
}
