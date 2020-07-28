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

package org.polypheny.db.interpreter;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.AggImplementor;
import org.polypheny.db.adapter.enumerable.RexImpTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.InvalidRelException;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.core.Window;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.logical.LogicalWindow;
import org.polypheny.db.rel.metadata.RelMdCollation;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;


/**
 * Utilities pertaining to {@link BindableRel} and {@link BindableConvention}.
 */
public class Bindables {

    private Bindables() {
    }


    public static final RelOptRule BINDABLE_TABLE_SCAN_RULE = new BindableTableScanRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_FILTER_RULE = new BindableFilterRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_PROJECT_RULE = new BindableProjectRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_SORT_RULE = new BindableSortRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_JOIN_RULE = new BindableJoinRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_UNION_RULE = new BindableUnionRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_VALUES_RULE = new BindableValuesRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_AGGREGATE_RULE = new BindableAggregateRule( RelFactories.LOGICAL_BUILDER );

    public static final RelOptRule BINDABLE_WINDOW_RULE = new BindableWindowRule( RelFactories.LOGICAL_BUILDER );

    /**
     * All rules that convert logical relational expression to bindable.
     */
    public static final ImmutableList<RelOptRule> RULES =
            ImmutableList.of(
                    NoneToBindableConverterRule.INSTANCE,
                    BINDABLE_TABLE_SCAN_RULE,
                    BINDABLE_FILTER_RULE,
                    BINDABLE_PROJECT_RULE,
                    BINDABLE_SORT_RULE,
                    BINDABLE_JOIN_RULE,
                    BINDABLE_UNION_RULE,
                    BINDABLE_VALUES_RULE,
                    BINDABLE_AGGREGATE_RULE,
                    BINDABLE_WINDOW_RULE );


    /**
     * Helper method that converts a bindable relational expression into a record iterator.
     *
     * Any bindable can be compiled; if its input is also bindable, it becomes part of the same compilation unit.
     */
    private static Enumerable<Object[]> help( DataContext dataContext, BindableRel rel ) {
        return new Interpreter( dataContext, rel );
    }


    /**
     * Rule that converts a {@link org.polypheny.db.rel.core.TableScan} to bindable convention.
     */
    public static class BindableTableScanRule extends RelOptRule {

        /**
         * Creates a BindableTableScanRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableTableScanRule( RelBuilderFactory relBuilderFactory ) {
            super( operand( LogicalTableScan.class, none() ), relBuilderFactory, null );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            final LogicalTableScan scan = call.rel( 0 );
            final RelOptTable table = scan.getTable();
            if ( BindableTableScan.canHandle( table ) ) {
                call.transformTo( BindableTableScan.create( scan.getCluster(), table ) );
            }
        }
    }


    /**
     * Scan of a table that implements {@link ScannableTable} and therefore can be converted into an {@link Enumerable}.
     */
    public static class BindableTableScan extends TableScan implements BindableRel {

        public final ImmutableList<RexNode> filters;
        public final ImmutableIntList projects;


        /**
         * Creates a BindableTableScan.
         *
         * Use {@link #create} unless you know what you are doing.
         */
        BindableTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, ImmutableList<RexNode> filters, ImmutableIntList projects ) {
            super( cluster, traitSet, table );
            this.filters = Objects.requireNonNull( filters );
            this.projects = Objects.requireNonNull( projects );
            Preconditions.checkArgument( canHandle( table ) );
        }


        /**
         * Creates a BindableTableScan.
         */
        public static BindableTableScan create( RelOptCluster cluster, RelOptTable relOptTable ) {
            return create( cluster, relOptTable, ImmutableList.of(), identity( relOptTable ) );
        }


        /**
         * Creates a BindableTableScan.
         */
        public static BindableTableScan create( RelOptCluster cluster, RelOptTable relOptTable, List<RexNode> filters, List<Integer> projects ) {
            final Table table = relOptTable.unwrap( Table.class );
            final RelTraitSet traitSet =
                    cluster.traitSetOf( BindableConvention.INSTANCE )
                            .replaceIfs( RelCollationTraitDef.INSTANCE, () -> {
                                if ( table != null ) {
                                    return table.getStatistic().getCollations();
                                }
                                return ImmutableList.of();
                            } );
            return new BindableTableScan( cluster, traitSet, relOptTable, ImmutableList.copyOf( filters ), ImmutableIntList.copyOf( projects ) );
        }


        @Override
        public RelDataType deriveRowType() {
            final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
            final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
            for ( int project : projects ) {
                builder.add( fieldList.get( project ) );
            }
            return builder.build();
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public RelWriter explainTerms( RelWriter pw ) {
            return super.explainTerms( pw )
                    .itemIf( "filters", filters, !filters.isEmpty() )
                    .itemIf( "projects", projects, !projects.equals( identity() ) );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            // Cost factor for pushing filters
            double f = filters.isEmpty() ? 1d : 0.5d;

            // Cost factor for pushing fields
            // The "+ 2d" on top and bottom keeps the function fairly smooth.
            double p = ((double) projects.size() + 2d) / ((double) table.getRowType().getFieldCount() + 2d);

            // Multiply the cost by a factor that makes a scan more attractive if filters and projects are pushed to the table scan
            return super.computeSelfCost( planner, mq ).multiplyBy( f * p * 0.01d );
        }


        @Override
        public String relCompareString() {
            return "BindableTableScan$" +
                    String.join( ".", table.getQualifiedName() ) +
                    (filters != null ? filters.stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                    (projects != null ? projects.toString() : "") + "&";
        }


        public static boolean canHandle( RelOptTable table ) {
            return table.unwrap( ScannableTable.class ) != null
                    || table.unwrap( FilterableTable.class ) != null
                    || table.unwrap( ProjectableFilterableTable.class ) != null;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            // TODO: filterable and projectable
            return table.unwrap( ScannableTable.class ).scan( dataContext );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            throw new UnsupportedOperationException(); // TODO:
        }
    }


    /**
     * Rule that converts a {@link Filter} to bindable convention.
     */
    public static class BindableFilterRule extends ConverterRule {

        /**
         * Creates a BindableFilterRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableFilterRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalFilter.class, (Predicate<LogicalFilter>) RelOptUtil::containsMultisetOrWindowedAgg, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableFilterRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalFilter filter = (LogicalFilter) rel;
            return BindableFilter.create( convert( filter.getInput(), filter.getInput().getTraitSet().replace( BindableConvention.INSTANCE ) ), filter.getCondition() );
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Filter} in bindable convention.
     */
    public static class BindableFilter extends Filter implements BindableRel {

        public BindableFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition ) {
            super( cluster, traitSet, input, condition );
            assert getConvention() instanceof BindableConvention;
        }


        /**
         * Creates a BindableFilter.
         */
        public static BindableFilter create( final RelNode input, RexNode condition ) {
            final RelOptCluster cluster = input.getCluster();
            final RelMetadataQuery mq = cluster.getMetadataQuery();
            final RelTraitSet traitSet = cluster.traitSetOf( BindableConvention.INSTANCE ).replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.filter( mq, input ) );
            return new BindableFilter( cluster, traitSet, input, condition );
        }


        @Override
        public BindableFilter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
            return new BindableFilter( getCluster(), traitSet, input, condition );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new FilterNode( implementor.compiler, this );
        }
    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.logical.LogicalProject} to a {@link BindableProject}.
     */
    public static class BindableProjectRule extends ConverterRule {

        /**
         * Creates a BindableProjectRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableProjectRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalProject.class, (Predicate<LogicalProject>) RelOptUtil::containsMultisetOrWindowedAgg, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableProjectRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalProject project = (LogicalProject) rel;
            return new BindableProject(
                    rel.getCluster(),
                    rel.getTraitSet().replace( BindableConvention.INSTANCE ),
                    convert( project.getInput(), project.getInput().getTraitSet().replace( BindableConvention.INSTANCE ) ),
                    project.getProjects(),
                    project.getRowType() );
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Project} in bindable calling convention.
     */
    public static class BindableProject extends Project implements BindableRel {

        public BindableProject( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
            super( cluster, traitSet, input, projects, rowType );
            assert getConvention() instanceof BindableConvention;
        }


        @Override
        public BindableProject copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
            return new BindableProject( getCluster(), traitSet, input, projects, rowType );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new ProjectNode( implementor.compiler, this );
        }
    }


    /**
     * Rule to convert an {@link Sort} to a {@link org.polypheny.db.interpreter.Bindables.BindableSort}.
     */
    public static class BindableSortRule extends ConverterRule {

        /**
         * Creates a BindableSortRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableSortRule( RelBuilderFactory relBuilderFactory ) {
            super( Sort.class, (Predicate<RelNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableSortRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Sort sort = (Sort) rel;
            final RelTraitSet traitSet = sort.getTraitSet().replace( BindableConvention.INSTANCE );
            final RelNode input = sort.getInput();
            return new BindableSort( rel.getCluster(), traitSet, convert( input, input.getTraitSet().replace( BindableConvention.INSTANCE ) ), sort.getCollation(), sort.offset, sort.fetch );
        }
    }


    /**
     * Implementation of {@link Sort} bindable calling convention.
     */
    public static class BindableSort extends Sort implements BindableRel {

        public BindableSort( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traitSet, input, collation, offset, fetch );
            assert getConvention() instanceof BindableConvention;
        }


        @Override
        public BindableSort copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch ) {
            return new BindableSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new SortNode( implementor.compiler, this );
        }
    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.logical.LogicalJoin} to a {@link BindableJoin}.
     */
    public static class BindableJoinRule extends ConverterRule {

        /**
         * Creates a BindableJoinRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableJoinRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalJoin.class, (Predicate<RelNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableJoinRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalJoin join = (LogicalJoin) rel;
            final BindableConvention out = BindableConvention.INSTANCE;
            final RelTraitSet traitSet = join.getTraitSet().replace( out );
            return new BindableJoin(
                    rel.getCluster(),
                    traitSet,
                    convert( join.getLeft(), join.getLeft().getTraitSet().replace( BindableConvention.INSTANCE ) ),
                    convert( join.getRight(), join.getRight().getTraitSet().replace( BindableConvention.INSTANCE ) ),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType() );
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Join} in bindable calling convention.
     */
    public static class BindableJoin extends Join implements BindableRel {

        /**
         * Creates a BindableJoin.
         */
        protected BindableJoin( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType ) {
            super( cluster, traitSet, left, right, condition, variablesSet, joinType );
        }


        @Override
        public BindableJoin copy( RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone ) {
            return new BindableJoin( getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new JoinNode( implementor.compiler, this );
        }
    }


    /**
     * Rule to convert an {@link LogicalUnion} to a {@link BindableUnion}.
     */
    public static class BindableUnionRule extends ConverterRule {

        /**
         * Creates a BindableUnionRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableUnionRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalUnion.class, (Predicate<RelNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableUnionRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalUnion union = (LogicalUnion) rel;
            final BindableConvention out = BindableConvention.INSTANCE;
            final RelTraitSet traitSet = union.getTraitSet().replace( out );
            return new BindableUnion( rel.getCluster(), traitSet, convertList( union.getInputs(), out ), union.all );
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Union} in bindable calling convention.
     */
    public static class BindableUnion extends Union implements BindableRel {

        public BindableUnion( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            super( cluster, traitSet, inputs, all );
        }


        @Override
        public BindableUnion copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            return new BindableUnion( getCluster(), traitSet, inputs, all );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new UnionNode( implementor.compiler, this );
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Values} in bindable calling convention.
     */
    public static class BindableValues extends Values implements BindableRel {

        BindableValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet ) {
            super( cluster, rowType, tuples, traitSet );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert inputs.isEmpty();
            return new BindableValues( getCluster(), rowType, tuples, traitSet );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new ValuesNode( implementor.compiler, this );
        }
    }


    /**
     * Rule that converts a {@link Values} to bindable convention.
     */
    public static class BindableValuesRule extends ConverterRule {

        /**
         * Creates a BindableValuesRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableValuesRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalValues.class, (Predicate<RelNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableValuesRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            LogicalValues values = (LogicalValues) rel;
            return new BindableValues( values.getCluster(), values.getRowType(), values.getTuples(), values.getTraitSet().replace( BindableConvention.INSTANCE ) );
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Aggregate} in bindable calling convention.
     */
    public static class BindableAggregate extends Aggregate implements BindableRel {

        public BindableAggregate( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) throws InvalidRelException {
            super( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );
            assert getConvention() instanceof BindableConvention;

            for ( AggregateCall aggCall : aggCalls ) {
                if ( aggCall.isDistinct() ) {
                    throw new InvalidRelException( "distinct aggregation not supported" );
                }
                AggImplementor implementor2 = RexImpTable.INSTANCE.get( aggCall.getAggregation(), false );
                if ( implementor2 == null ) {
                    throw new InvalidRelException( "aggregation " + aggCall.getAggregation() + " not supported" );
                }
            }
        }


        @Override
        public BindableAggregate copy( RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            try {
                return new BindableAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
            } catch ( InvalidRelException e ) {
                // Semantic error not possible. Must be a bug. Convert to internal error.
                throw new AssertionError( e );
            }
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new AggregateNode( implementor.compiler, this );
        }
    }


    /**
     * Rule that converts an {@link Aggregate} to bindable convention.
     */
    public static class BindableAggregateRule extends ConverterRule {

        /**
         * Creates a BindableAggregateRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableAggregateRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalAggregate.class, (Predicate<RelNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableAggregateRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            final RelTraitSet traitSet = agg.getTraitSet().replace( BindableConvention.INSTANCE );
            try {
                return new BindableAggregate( rel.getCluster(), traitSet, convert( agg.getInput(), traitSet ), agg.indicator, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList() );
            } catch ( InvalidRelException e ) {
                RelOptPlanner.LOGGER.debug( e.toString() );
                return null;
            }
        }
    }


    /**
     * Implementation of {@link org.polypheny.db.rel.core.Window} in bindable convention.
     */
    public static class BindableWindow extends Window implements BindableRel {

        /**
         * Creates an BindableWindowRel.
         */
        BindableWindow( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<RexLiteral> constants, RelDataType rowType, List<Group> groups ) {
            super( cluster, traitSet, input, constants, rowType, groups );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            return new BindableWindow( getCluster(), traitSet, sole( inputs ), constants, rowType, groups );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( BindableConvention.COST_MULTIPLIER );
        }


        @Override
        public Class<Object[]> getElementType() {
            return Object[].class;
        }


        @Override
        public Enumerable<Object[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new WindowNode( implementor.compiler, this );
        }
    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.logical.LogicalWindow} to a {@link BindableWindow}.
     */
    public static class BindableWindowRule extends ConverterRule {

        /**
         * Creates a BindableWindowRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public BindableWindowRule( RelBuilderFactory relBuilderFactory ) {
            super( LogicalWindow.class, (Predicate<RelNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, relBuilderFactory, "BindableWindowRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalWindow winAgg = (LogicalWindow) rel;
            final RelTraitSet traitSet = winAgg.getTraitSet().replace( BindableConvention.INSTANCE );
            final RelNode input = winAgg.getInput();
            final RelNode convertedInput = convert( input, input.getTraitSet().replace( BindableConvention.INSTANCE ) );
            return new BindableWindow( rel.getCluster(), traitSet, convertedInput, winAgg.getConstants(), winAgg.getRowType(), winAgg.groups );
        }
    }
}

