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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinInfo;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;


/**
 * Planner rule that creates a {@code SemiJoinRule} from a {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Join} on top of a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate}.
 */
public abstract class SemiJoinRule extends RelOptRule {

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
    private static final Predicate<Aggregate> IS_EMPTY_AGGREGATE = aggregate -> aggregate.getRowType().getFieldCount() == 0;

    public static final SemiJoinRule PROJECT = new ProjectToSemiJoinRule( Project.class, Join.class, Aggregate.class, RelFactories.LOGICAL_BUILDER, "SemiJoinRule:project" );

    public static final SemiJoinRule JOIN = new JoinToSemiJoinRule( Join.class, Aggregate.class, RelFactories.LOGICAL_BUILDER, "SemiJoinRule:join" );


    protected SemiJoinRule( Class<Project> projectClass, Class<Join> joinClass, Class<Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory, String description ) {
        super(
                operand(
                        projectClass,
                        some(
                                operandJ(
                                        joinClass,
                                        null,
                                        IS_LEFT_OR_INNER,
                                        some(
                                                operand( RelNode.class, any() ),
                                                operand( aggregateClass, any() ) ) ) ) ),
                relBuilderFactory, description );
    }


    protected SemiJoinRule( Class<Join> joinClass, Class<Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory, String description ) {
        super(
                operandJ(
                        joinClass,
                        null,
                        IS_LEFT_OR_INNER,
                        some(
                                operand( RelNode.class, any() ),
                                operandJ( aggregateClass, null, IS_EMPTY_AGGREGATE, any() ) ) ),
                relBuilderFactory, description );
    }


    protected void perform( RelOptRuleCall call, Project project, Join join, RelNode left, Aggregate aggregate ) {
        final RelOptCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if ( project != null ) {
            final ImmutableBitSet bits = RelOptUtil.InputFinder.bits( project.getProjects(), null );
            final ImmutableBitSet rightBits = ImmutableBitSet.range( left.getRowType().getFieldCount(), join.getRowType().getFieldCount() );
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
        final RelBuilder relBuilder = call.builder();
        relBuilder.push( left );
        switch ( join.getJoinType() ) {
            case INNER:
                final List<Integer> newRightKeyBuilder = new ArrayList<>();
                final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
                for ( int key : joinInfo.rightKeys ) {
                    newRightKeyBuilder.add( aggregateKeys.get( key ) );
                }
                final ImmutableIntList newRightKeys = ImmutableIntList.copyOf( newRightKeyBuilder );
                relBuilder.push( aggregate.getInput() );
                final RexNode newCondition = RelOptUtil.createEquiJoinCondition( relBuilder.peek( 2, 0 ), joinInfo.leftKeys, relBuilder.peek( 2, 1 ), newRightKeys, rexBuilder );
                relBuilder.semiJoin( newCondition );
                break;

            case LEFT:
                // The right-hand side produces no more than 1 row (because of the Aggregate) and no fewer than 1 row (because of LEFT), and therefore we can eliminate the semi-join.
                break;

            default:
                throw new AssertionError( join.getJoinType() );
        }
        if ( project != null ) {
            relBuilder.project( project.getProjects(), project.getRowType().getFieldNames() );
        }
        call.transformTo( relBuilder.build() );
    }


    /**
     * SemiJoinRule that matches a Project on top of a Join with an Aggregate as its right child.
     */
    public static class ProjectToSemiJoinRule extends SemiJoinRule {

        /**
         * Creates a ProjectToSemiJoinRule.
         */
        public ProjectToSemiJoinRule( Class<Project> projectClass, Class<Join> joinClass, Class<Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory, String description ) {
            super( projectClass, joinClass, aggregateClass, relBuilderFactory, description );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Project project = call.rel( 0 );
            final Join join = call.rel( 1 );
            final RelNode left = call.rel( 2 );
            final Aggregate aggregate = call.rel( 3 );
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
        public JoinToSemiJoinRule( Class<Join> joinClass, Class<Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory, String description ) {
            super( joinClass, aggregateClass, relBuilderFactory, description );
        }
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Join join = call.rel( 0 );
        final RelNode left = call.rel( 1 );
        final Aggregate aggregate = call.rel( 2 );
        perform( call, null, join, left, aggregate );
    }
}

