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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Aggregate.Group;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.mapping.Mappings;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Planner rule that recognizes a {@link Aggregate} on top of a {@link Project} and if possible aggregate through the project or removes the project.
 * <p>
 * This is only possible when the grouping expressions and arguments to the aggregate functions are field references (i.e. not expressions).
 * <p>
 * In some cases, this rule has the effect of trimming: the aggregate will use fewer columns than the project did.
 */
public class AggregateProjectMergeRule extends AlgOptRule {

    public static final AggregateProjectMergeRule INSTANCE = new AggregateProjectMergeRule( Aggregate.class, Project.class, AlgFactories.LOGICAL_BUILDER );


    public AggregateProjectMergeRule( Class<? extends Aggregate> aggregateClass, Class<? extends Project> projectClass, AlgBuilderFactory algBuilderFactory ) {
        super( operand( aggregateClass, operand( projectClass, any() ) ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate aggregate = call.alg( 0 );
        final Project project = call.alg( 1 );
        AlgNode x = apply( call, aggregate, project );
        if ( x != null ) {
            call.transformTo( x );
        }
    }


    public static AlgNode apply( AlgOptRuleCall call, Aggregate aggregate, Project project ) {
        // Find all fields which we need to be straightforward field projections.
        final Set<Integer> interestingFields = new TreeSet<>( aggregate.getGroupSet().asList() );
        for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            interestingFields.addAll( aggregateCall.getArgList() );
            if ( aggregateCall.filterArg >= 0 ) {
                interestingFields.add( aggregateCall.filterArg );
            }
            interestingFields.addAll( AlgCollations.ordinals( aggregateCall.collation ) );
        }

        // Build the map from old to new; abort if any entry is not a straightforward field projection.
        final Map<Integer, Integer> map = new HashMap<>();
        for ( int source : interestingFields ) {
            final RexNode rex = project.getProjects().get( source );
            if ( rex.unwrap( RexIndexRef.class ).isEmpty() ) {
                return null;
            }
            map.put( source, rex.unwrap( RexIndexRef.class ).get().getIndex() );
        }

        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute( map );
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if ( aggregate.getGroupType() != Group.SIMPLE ) {
            newGroupingSets = ImmutableBitSet.ORDERING.immutableSortedCopy( ImmutableBitSet.permute( aggregate.getGroupSets(), map ) );
        }

        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        final int sourceCount = aggregate.getInput().getTupleType().getFieldCount();
        final int targetCount = project.getInput().getTupleType().getFieldCount();
        final TargetMapping targetMapping = Mappings.target( map, sourceCount, targetCount );
        for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            aggCalls.add( aggregateCall.transform( targetMapping ) );
        }

        final Aggregate newAggregate = aggregate.copy( aggregate.getTraitSet(), project.getInput(), aggregate.indicator, newGroupSet, newGroupingSets, aggCalls.build() );

        // Add a project if the group set is not in the same order or contains duplicates.
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( newAggregate );
        final List<Integer> newKeys = aggregate.getGroupSet().asList().stream().map( map::get ).toList();
        if ( !newKeys.equals( newGroupSet.asList() ) ) {
            final List<Integer> posList = new ArrayList<>();
            for ( int newKey : newKeys ) {
                posList.add( newGroupSet.indexOf( newKey ) );
            }
            if ( aggregate.indicator ) {
                for ( int newKey : newKeys ) {
                    posList.add( aggregate.getGroupCount() + newGroupSet.indexOf( newKey ) );
                }
            }
            for ( int i = newAggregate.getGroupCount() + newAggregate.getIndicatorCount(); i < newAggregate.getTupleType().getFieldCount(); i++ ) {
                posList.add( i );
            }
            algBuilder.project( algBuilder.fields( posList ) );
        }

        return algBuilder.build();
    }

}

