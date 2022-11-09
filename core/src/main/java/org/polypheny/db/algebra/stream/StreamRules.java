/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalSort;
import org.polypheny.db.algebra.logical.relational.LogicalUnion;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.schema.StreamableTable;
import org.polypheny.db.schema.Table;
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
            final LogicalProject newProject = LogicalProject.create( newDelta, project.getProjects(), project.getRowType().getFieldNames() );
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
            final LogicalFilter newFilter = LogicalFilter.create( newDelta, filter.getCondition() );
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
                            operandJ( Aggregate.class, null, Aggregate::noIndicator, any() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            Util.discard( delta );
            final Aggregate aggregate = call.alg( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( aggregate.getInput() );
            final LogicalAggregate newAggregate = LogicalAggregate.create( newDelta, aggregate.getGroupSet(), aggregate.groupSets, aggregate.getAggCallList() );
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
            final LogicalSort newSort = LogicalSort.create( newDelta, sort.collation, sort.offset, sort.fetch );
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
            final LogicalUnion newUnion = LogicalUnion.create( newInputs, union.all );
            call.transformTo( newUnion );
        }

    }


    /**
     * Planner rule that pushes a {@link Delta} into a {@link Scan} of a {@link StreamableTable}.
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
                    operand( Delta.class, operand( Scan.class, none() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            final Scan scan = call.alg( 1 );
            final AlgOptCluster cluster = delta.getCluster();
            final AlgOptTable algOptTable = scan.getTable();
            final StreamableTable streamableTable = algOptTable.unwrap( StreamableTable.class );
            if ( streamableTable != null ) {
                final Table table1 = streamableTable.stream();
                final AlgOptTable algOptTable2 =
                        AlgOptTableImpl.create( algOptTable.getRelOptSchema(),
                                algOptTable.getRowType(), table1,
                                ImmutableList.<String>builder()
                                        .addAll( algOptTable.getQualifiedName() )
                                        .add( "(STREAM)" ).build() );
                final LogicalScan newScan = LogicalScan.create( cluster, algOptTable2 );
                call.transformTo( newScan );
            }
        }

    }


    /**
     * Planner rule that converts {@link Delta} over a {@link Scan} of a table other than {@link StreamableTable} to an empty {@link Values}.
     */
    public static class DeltaScanToEmptyRule extends AlgOptRule {

        /**
         * Creates a DeltaScanToEmptyRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DeltaScanToEmptyRule( AlgBuilderFactory algBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Scan.class, none() ) ),
                    algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Delta delta = call.alg( 0 );
            final Scan scan = call.alg( 1 );
            final AlgOptTable algOptTable = scan.getTable();
            final StreamableTable streamableTable = algOptTable.unwrap( StreamableTable.class );
            final AlgBuilder builder = call.builder();
            if ( streamableTable == null ) {
                call.transformTo( builder.values( delta.getRowType() ).build() );
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
            final LogicalJoin joinL = LogicalJoin.create( left, rightWithDelta,
                    join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                    join.isSemiJoinDone(),
                    ImmutableList.copyOf( join.getSystemFieldList() ) );

            final LogicalDelta leftWithDelta = LogicalDelta.create( left );
            final LogicalJoin joinR = LogicalJoin.create( leftWithDelta, right,
                    join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                    join.isSemiJoinDone(),
                    ImmutableList.copyOf( join.getSystemFieldList() ) );

            List<AlgNode> inputsToUnion = new ArrayList<>();
            inputsToUnion.add( joinL );
            inputsToUnion.add( joinR );

            final LogicalUnion newNode = LogicalUnion.create( inputsToUnion, true );
            call.transformTo( newNode );
        }

    }

}

