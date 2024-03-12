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
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Planner rule that creates a {@code SemiJoinRule} from a {@link org.polypheny.db.algebra.core.Join} on top of a {@link LogicalRelAggregate}.
 */
public abstract class SemiJoinRule extends AlgOptRule {

    private static final Predicate<Join> IS_LEFT_OR_INNER =
            join -> {
                switch ( join.getJoinType() ) {
                    case LEFT:
                    case INNER:
                        return true;
                    default:
                        return false;
                }
            };

    /* Tests if an Aggregate always produces 1 row and 0 columns. */
    private static final Predicate<Aggregate> IS_EMPTY_AGGREGATE = aggregate -> aggregate.getTupleType().getFieldCount() == 0;


    protected SemiJoinRule( Class<Project> projectClass, Class<Join> joinClass, Class<Aggregate> aggregateClass, AlgBuilderFactory algBuilderFactory, String description ) {
        super(
                operand(
                        projectClass,
                        some(
                                operand(
                                        joinClass,
                                        null,
                                        IS_LEFT_OR_INNER,
                                        some(
                                                operand( AlgNode.class, any() ),
                                                operand( aggregateClass, any() ) ) ) ) ),
                algBuilderFactory, description );
    }


    protected SemiJoinRule( Class<Join> joinClass, Class<Aggregate> aggregateClass, AlgBuilderFactory algBuilderFactory, String description ) {
        super(
                operand(
                        joinClass,
                        null,
                        IS_LEFT_OR_INNER,
                        some(
                                operand( AlgNode.class, any() ),
                                operand( aggregateClass, null, IS_EMPTY_AGGREGATE, any() ) ) ),
                algBuilderFactory, description );
    }


    protected void perform( AlgOptRuleCall call, Project project, Join join, AlgNode left, Aggregate aggregate ) {
        final AlgCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if ( project != null ) {
            final ImmutableBitSet bits = AlgOptUtil.InputFinder.bits( project.getProjects(), null );
            final ImmutableBitSet rightBits = ImmutableBitSet.range( left.getTupleType().getFieldCount(), join.getTupleType().getFieldCount() );
            if ( bits.intersects( rightBits ) ) {
                return;
            }
        }
        final JoinInfo joinInfo = join.analyzeCondition();
        if ( !joinInfo.rightSet().equals( ImmutableBitSet.range( aggregate.getGroupCount() ) ) ) {
            // Rule requires that aggregate key to be the same as the join key. By the way, neither a super-set nor a sub-set would work.
            return;
        }
        if ( !joinInfo.isEqui() ) {
            return;
        }
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( left );
        switch ( join.getJoinType() ) {
            case INNER:
                final List<Integer> newRightKeyBuilder = new ArrayList<>();
                final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
                for ( int key : joinInfo.rightKeys ) {
                    newRightKeyBuilder.add( aggregateKeys.get( key ) );
                }
                final ImmutableList<Integer> newRightKeys = ImmutableList.copyOf( newRightKeyBuilder );
                algBuilder.push( aggregate.getInput() );
                final RexNode newCondition = AlgOptUtil.createEquiJoinCondition( algBuilder.peek( 2, 0 ), joinInfo.leftKeys, algBuilder.peek( 2, 1 ), newRightKeys, rexBuilder );
                algBuilder.semiJoin( newCondition );
                break;

            case LEFT:
                // The right-hand side produces no more than 1 row (because of the Aggregate) and no fewer than 1 row (because of LEFT), and therefore we can eliminate the semi-join.
                break;

            default:
                throw new AssertionError( join.getJoinType() );
        }
        if ( project != null ) {
            algBuilder.project( project.getProjects(), project.getTupleType().getFieldNames() );
        }
        call.transformTo( algBuilder.build() );
    }


    /**
     * SemiJoinRule that matches a Project on top of a Join with an Aggregate as its right child.
     */
    public static class ProjectToSemiJoinRule extends SemiJoinRule {

        /**
         * Creates a ProjectToSemiJoinRule.
         */
        public ProjectToSemiJoinRule( Class<Project> projectClass, Class<Join> joinClass, Class<Aggregate> aggregateClass, AlgBuilderFactory algBuilderFactory, String description ) {
            super( projectClass, joinClass, aggregateClass, algBuilderFactory, description );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Project project = call.alg( 0 );
            final Join join = call.alg( 1 );
            final AlgNode left = call.alg( 2 );
            final Aggregate aggregate = call.alg( 3 );
            perform( call, project, join, left, aggregate );
        }

    }


    /**
     * SemiJoinRule that matches a Join with an empty Aggregate as its right child.
     */
    public static class JoinToSemiJoinRule extends SemiJoinRule {

        /**
         * Creates a JoinToSemiJoinRule.
         */
        public JoinToSemiJoinRule( Class<Join> joinClass, Class<Aggregate> aggregateClass, AlgBuilderFactory algBuilderFactory, String description ) {
            super( joinClass, aggregateClass, algBuilderFactory, description );
        }

    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Join join = call.alg( 0 );
        final AlgNode left = call.alg( 1 );
        final Aggregate aggregate = call.alg( 2 );
        perform( call, null, join, left, aggregate );
    }

}

