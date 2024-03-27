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

package org.polypheny.db.algebra.stream;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.schema.types.StreamableEntity;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Util;


/**
 * Rules and relational operators for streaming relational expressions.
 */
public class StreamRules {

    private StreamRules() {
    }


    public static final ImmutableList<AlgOptRule> RULES =
            ImmutableList.of(
                    new DeltaProjectTransposeRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaFilterTransposeRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaAggregateTransposeRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaSortTransposeRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaUnionTransposeRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaJoinTransposeRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaScanRule( AlgFactories.LOGICAL_BUILDER ),
                    new DeltaScanToEmptyRule( AlgFactories.LOGICAL_BUILDER ) );


    /**
     * Planner rule that pushes a {@link Delta} through a {@link Project}.
     */
    public static class DeltaProjectTransposeRule extends AlgOptRule {

        /**
         * Creates a DeltaProjectTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaProjectTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Project.class, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Project project = call.alg( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( project.getInput() );
            final LogicalRelProject newProject = LogicalRelProject.create( newDelta, project.getProjects(), project.getTupleType().getFieldNames() );
            call.transformTo( newProject );
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} through a {@link Filter}.
     */
    public static class DeltaFilterTransposeRule extends AlgOptRule {

        /**
         * Creates a DeltaFilterTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaFilterTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Filter.class, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Filter filter = call.alg( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( filter.getInput() );
            final LogicalRelFilter newFilter = LogicalRelFilter.create( newDelta, filter.getCondition() );
            call.transformTo( newFilter );
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} through an {@link Aggregate}.
     */
    public static class DeltaAggregateTransposeRule extends AlgOptRule {

        /**
         * Creates a DeltaAggregateTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaAggregateTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand(
                            Delta.class,
                            operand( Aggregate.class, null, Aggregate::noIndicator, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Aggregate aggregate = call.alg( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( aggregate.getInput() );
            final LogicalRelAggregate newAggregate = LogicalRelAggregate.create( newDelta, aggregate.getGroupSet(), aggregate.groupSets, aggregate.getAggCallList() );
            call.transformTo( newAggregate );
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} through an {@link Sort}.
     */
    public static class DeltaSortTransposeRule extends AlgOptRule {

        /**
         * Creates a DeltaSortTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaSortTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Sort.class, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Sort sort = call.alg( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( sort.getInput() );
            final LogicalRelSort newSort = LogicalRelSort.create( newDelta, sort.collation, sort.offset, sort.fetch );
            call.transformTo( newSort );
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} through an {@link Union}.
     */
    public static class DeltaUnionTransposeRule extends AlgOptRule {

        /**
         * Creates a DeltaUnionTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaUnionTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Union.class, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Union union = call.alg( 1 );
            final List<AlgNode> newInputs = new ArrayList<>();
            for ( AlgNode input : union.getInputs() ) {
                final LogicalDelta newDelta = LogicalDelta.create( input );
                newInputs.add( newDelta );
            }
            final LogicalRelUnion newUnion = LogicalRelUnion.create( newInputs, union.all );
            call.transformTo( newUnion );
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} into a {@link RelScan} of a {@link StreamableEntity}.
     *
     * Very likely, the stream was only represented as a table for uniformity with the other relations in the system. The Delta disappears and the stream can be implemented directly.
     */
    public static class DeltaScanRule extends AlgOptRule {

        /**
         * Creates a DeltaScanRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaScanRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( RelScan.class, none() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            final RelScan<?> scan = call.alg( 1 );
            final AlgCluster cluster = delta.getCluster();
            Optional<StreamableEntity> oStreamableTable = scan.entity.unwrap( StreamableEntity.class );
            if ( oStreamableTable.isPresent() ) {
                final LogicalRelScan newScan = LogicalRelScan.create( cluster, null );
                call.transformTo( newScan );
            }
        }

    }


    /**
     * Planner rule that converts {@link Delta} over a {@link RelScan} of a table other than {@link StreamableEntity} to an empty {@link Values}.
     */
    public static class DeltaScanToEmptyRule extends AlgOptRule {

        /**
         * Creates a DeltaScanToEmptyRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaScanToEmptyRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( RelScan.class, none() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            final RelScan<?> scan = call.alg( 1 );
            Optional<StreamableEntity> oStreamableTable = scan.getEntity().unwrap( StreamableEntity.class );
            final AlgBuilder builder = call.builder();
            if ( oStreamableTable.isEmpty() ) {
                call.transformTo( builder.values( delta.getTupleType() ).build() );
            }
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} through a {@link Join}.
     *
     * We apply something analogous to the <a href="https://en.wikipedia.org/wiki/Product_rule">product rule of differential calculus</a> to implement the transpose:
     *
     * <blockquote><code>stream(x join y) &rarr; x join stream(y) union all stream(x) join y</code></blockquote>
     */
    public static class DeltaJoinTransposeRule extends AlgOptRule {

        /**
         * Creates a DeltaJoinTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaJoinTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Join.class, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Join join = call.alg( 1 );
            final AlgNode left = join.getLeft();
            final AlgNode right = join.getRight();

            final LogicalDelta rightWithDelta = LogicalDelta.create( right );
            final LogicalRelJoin joinL = LogicalRelJoin.create( left, rightWithDelta,
                    join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                    join.isSemiJoinDone() );

            final LogicalDelta leftWithDelta = LogicalDelta.create( left );
            final LogicalRelJoin joinR = LogicalRelJoin.create( leftWithDelta, right,
                    join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                    join.isSemiJoinDone() );

            List<AlgNode> inputsToUnion = new ArrayList<>();
            inputsToUnion.add( joinL );
            inputsToUnion.add( joinR );

            final LogicalRelUnion newNode = LogicalRelUnion.create( inputsToUnion, true );
            call.transformTo( newNode );
        }

    }

}

