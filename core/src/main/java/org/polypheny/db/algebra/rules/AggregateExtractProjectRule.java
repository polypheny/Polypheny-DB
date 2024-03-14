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
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.MappingType;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Rule to extract a {@link Project} from an {@link org.polypheny.db.algebra.core.Aggregate} and push it down towards the input.
 *
 * What projections can be safely pushed down depends upon which fields the Aggregate uses.
 *
 * To prevent cycles, this rule will not extract a {@code Project} if the {@code Aggregate}s input is already a {@code Project}.
 */
public class AggregateExtractProjectRule extends AlgOptRule {

    /**
     * Creates an AggregateExtractProjectRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public AggregateExtractProjectRule( Class<? extends Aggregate> aggregateClass, Class<? extends AlgNode> inputClass, AlgBuilderFactory algBuilderFactory ) {
        // Predicate prevents matching against an Aggregate whose input is already a Project. Prevents this rule firing repeatedly.
        this( operand( aggregateClass, operand( inputClass, null, r -> !(r instanceof Project), any() ) ), algBuilderFactory );
    }


    public AggregateExtractProjectRule( AlgOptRuleOperand operand, AlgBuilderFactory builderFactory ) {
        super( operand, builderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate aggregate = call.alg( 0 );
        final AlgNode input = call.alg( 1 );
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
        final AlgBuilder algBuilder = call.builder().push( input );
        final List<RexNode> projects = new ArrayList<>();
        final Mapping mapping = Mappings.create( MappingType.INVERSE_SURJECTION, aggregate.getInput().getTupleType().getFieldCount(), inputFieldsUsed.cardinality() );
        int j = 0;
        for ( int i : inputFieldsUsed.build() ) {
            projects.add( algBuilder.field( i ) );
            mapping.set( i, j++ );
        }

        algBuilder.project( projects );

        final ImmutableBitSet newGroupSet = Mappings.apply( mapping, aggregate.getGroupSet() );

        final Iterable<ImmutableBitSet> newGroupSets = Iterables.transform( aggregate.getGroupSets(), bitSet -> Mappings.apply( mapping, bitSet ) );
        final List<AlgBuilder.AggCall> newAggCallList = new ArrayList<>();
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            final ImmutableList<RexNode> args = algBuilder.fields( Mappings.apply2( mapping, aggCall.getArgList() ) );
            final RexNode filterArg = aggCall.filterArg < 0 ? null : algBuilder.field( Mappings.apply( mapping, aggCall.filterArg ) );
            newAggCallList.add(
                    algBuilder.aggregateCall( aggCall.getAggregation(), args )
                            .distinct( aggCall.isDistinct() )
                            .filter( filterArg )
                            .approximate( aggCall.isApproximate() )
                            .sort( algBuilder.fields( aggCall.collation ) )
                            .as( aggCall.name ) );
        }

        final AlgBuilder.GroupKey groupKey = algBuilder.groupKey( newGroupSet, newGroupSets );
        algBuilder.aggregate( groupKey, newAggCallList );
        call.transformTo( algBuilder.build() );
    }

}
