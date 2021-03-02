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

package org.polypheny.db.rel.stream;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.schema.StreamableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.Util;


/**
 * Rules and relational operators for streaming relational expressions.
 */
public class StreamRules {

    private StreamRules() {
    }


    public static final ImmutableList<RelOptRule> RULES =
            ImmutableList.of(
                    new DeltaProjectTransposeRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaFilterTransposeRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaAggregateTransposeRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaSortTransposeRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaUnionTransposeRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaJoinTransposeRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaTableScanRule( RelFactories.LOGICAL_BUILDER ),
                    new DeltaTableScanToEmptyRule( RelFactories.LOGICAL_BUILDER ) );


    /**
     * Planner rule that pushes a {@link Delta} through a {@link Project}.
     */
    public static class DeltaProjectTransposeRule extends RelOptRule {

        /**
         * Creates a DeltaProjectTransposeRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaProjectTransposeRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Project.class, any() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            Util.discard( delta );
            final Project project = call.rel( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( project.getInput() );
            final LogicalProject newProject = LogicalProject.create( newDelta, project.getProjects(), project.getRowType().getFieldNames() );
            call.transformTo( newProject );
        }
    }


    /**
     * Planner rule that pushes a {@link Delta} through a {@link Filter}.
     */
    public static class DeltaFilterTransposeRule extends RelOptRule {

        /**
         * Creates a DeltaFilterTransposeRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaFilterTransposeRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Filter.class, any() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            Util.discard( delta );
            final Filter filter = call.rel( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( filter.getInput() );
            final LogicalFilter newFilter = LogicalFilter.create( newDelta, filter.getCondition() );
            call.transformTo( newFilter );
        }
    }


    /**
     * Planner rule that pushes a {@link Delta} through an {@link Aggregate}.
     */
    public static class DeltaAggregateTransposeRule extends RelOptRule {

        /**
         * Creates a DeltaAggregateTransposeRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaAggregateTransposeRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand(
                            Delta.class,
                            operandJ( Aggregate.class, null, Aggregate::noIndicator, any() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            Util.discard( delta );
            final Aggregate aggregate = call.rel( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( aggregate.getInput() );
            final LogicalAggregate newAggregate = LogicalAggregate.create( newDelta, aggregate.getGroupSet(), aggregate.groupSets, aggregate.getAggCallList() );
            call.transformTo( newAggregate );
        }
    }


    /**
     * Planner rule that pushes a {@link Delta} through an {@link Sort}.
     */
    public static class DeltaSortTransposeRule extends RelOptRule {

        /**
         * Creates a DeltaSortTransposeRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaSortTransposeRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Sort.class, any() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            Util.discard( delta );
            final Sort sort = call.rel( 1 );
            final LogicalDelta newDelta = LogicalDelta.create( sort.getInput() );
            final LogicalSort newSort = LogicalSort.create( newDelta, sort.collation, sort.offset, sort.fetch );
            call.transformTo( newSort );
        }
    }


    /**
     * Planner rule that pushes a {@link Delta} through an {@link Union}.
     */
    public static class DeltaUnionTransposeRule extends RelOptRule {

        /**
         * Creates a DeltaUnionTransposeRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaUnionTransposeRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Union.class, any() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            Util.discard( delta );
            final Union union = call.rel( 1 );
            final List<RelNode> newInputs = new ArrayList<>();
            for ( RelNode input : union.getInputs() ) {
                final LogicalDelta newDelta = LogicalDelta.create( input );
                newInputs.add( newDelta );
            }
            final LogicalUnion newUnion = LogicalUnion.create( newInputs, union.all );
            call.transformTo( newUnion );
        }
    }


    /**
     * Planner rule that pushes a {@link Delta} into a {@link TableScan} of a {@link StreamableTable}.
     *
     * Very likely, the stream was only represented as a table for uniformity with the other relations in the system. The Delta disappears and the stream can be implemented directly.
     */
    public static class DeltaTableScanRule extends RelOptRule {

        /**
         * Creates a DeltaTableScanRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaTableScanRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( TableScan.class, none() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            final TableScan scan = call.rel( 1 );
            final RelOptCluster cluster = delta.getCluster();
            final RelOptTable relOptTable = scan.getTable();
            final StreamableTable streamableTable = relOptTable.unwrap( StreamableTable.class );
            if ( streamableTable != null ) {
                final Table table1 = streamableTable.stream();
                final RelOptTable relOptTable2 =
                        RelOptTableImpl.create( relOptTable.getRelOptSchema(),
                                relOptTable.getRowType(), table1,
                                ImmutableList.<String>builder()
                                        .addAll( relOptTable.getQualifiedName() )
                                        .add( "(STREAM)" ).build() );
                final RelNode newScan = LogicalTableScan.create( cluster, relOptTable2 );
                call.transformTo( newScan );
            }
        }
    }


    /**
     * Planner rule that converts {@link Delta} over a {@link TableScan} of a table other than {@link StreamableTable} to an empty {@link Values}.
     */
    public static class DeltaTableScanToEmptyRule extends RelOptRule {

        /**
         * Creates a DeltaTableScanToEmptyRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaTableScanToEmptyRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( TableScan.class, none() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            final TableScan scan = call.rel( 1 );
            final RelOptTable relOptTable = scan.getTable();
            final StreamableTable streamableTable = relOptTable.unwrap( StreamableTable.class );
            final RelBuilder builder = call.builder();
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
    public static class DeltaJoinTransposeRule extends RelOptRule {

        /**
         * Creates a DeltaJoinTransposeRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public DeltaJoinTransposeRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operand( Delta.class, operand( Join.class, any() ) ),
                    relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final Delta delta = call.rel( 0 );
            Util.discard( delta );
            final Join join = call.rel( 1 );
            final RelNode left = join.getLeft();
            final RelNode right = join.getRight();

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

            List<RelNode> inputsToUnion = new ArrayList<>();
            inputsToUnion.add( joinL );
            inputsToUnion.add( joinR );

            final LogicalUnion newNode = LogicalUnion.create( inputsToUnion, true );
            call.transformTo( newNode );
        }
    }
}

