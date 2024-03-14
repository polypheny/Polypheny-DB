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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.Window;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.AggImplementor;
import org.polypheny.db.algebra.enumerable.RexImpTable;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.logical.relational.LogicalWindow;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.schema.types.ProjectableFilterableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Utilities pertaining to {@link BindableAlg} and {@link BindableConvention}.
 */
public class Bindables {

    private Bindables() {
    }


    public static final AlgOptRule BINDABLE_TABLE_SCAN_RULE = new BindableScanRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_FILTER_RULE = new BindableFilterRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_PROJECT_RULE = new BindableProjectRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_SORT_RULE = new BindableSortRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_JOIN_RULE = new BindableJoinRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_UNION_RULE = new BindableUnionRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_VALUES_RULE = new BindableValuesRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_AGGREGATE_RULE = new BindableAggregateRule( AlgFactories.LOGICAL_BUILDER );

    public static final AlgOptRule BINDABLE_WINDOW_RULE = new BindableWindowRule( AlgFactories.LOGICAL_BUILDER );

    /**
     * All rules that convert logical relational expression to bindable.
     */
    public static final ImmutableList<AlgOptRule> RULES =
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
    private static Enumerable<PolyValue[]> help( DataContext dataContext, BindableAlg alg ) {
        return new Interpreter( dataContext, alg );
    }


    /**
     * Rule that converts a {@link RelScan} to bindable convention.
     */
    public static class BindableScanRule extends AlgOptRule {

        /**
         * Creates a BindableScanRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableScanRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( LogicalRelScan.class, none() ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final LogicalRelScan scan = call.alg( 0 );
            final Entity table = scan.entity;
            if ( BindableScan.canHandle( table ) ) {
                call.transformTo( BindableScan.create( scan.getCluster(), scan.entity ) );
            }
        }

    }


    /**
     * Scan of a table that implements {@link ScannableEntity} and therefore can be converted into an {@link Enumerable}.
     */
    public static class BindableScan extends RelScan<Entity> implements BindableAlg {

        public final ImmutableList<RexNode> filters;
        public final ImmutableList<Integer> projects;


        /**
         * Creates a BindableScan.
         *
         * Use {@link #create} unless you know what you are doing.
         */
        BindableScan( AlgCluster cluster, AlgTraitSet traitSet, Entity entity, ImmutableList<RexNode> filters, ImmutableList<Integer> projects ) {
            super( cluster, traitSet, entity );
            this.filters = Objects.requireNonNull( filters );
            this.projects = Objects.requireNonNull( projects );
            Preconditions.checkArgument( canHandle( entity ) );
        }


        /**
         * Creates a BindableScan.
         */
        public static BindableScan create( AlgCluster cluster, Entity entity ) {
            return create( cluster, entity, ImmutableList.of(), identity( entity ) );
        }


        /**
         * Creates a BindableScan.
         */
        public static BindableScan create( AlgCluster cluster, Entity entity, List<RexNode> filters, List<Integer> projects ) {
            final AlgTraitSet traitSet =
                    cluster.traitSetOf( BindableConvention.INSTANCE )
                            .replace( entity.dataModel.getModelTrait() )
                            .replaceIfs( AlgCollationTraitDef.INSTANCE, entity::getCollations );
            return new BindableScan( cluster, traitSet, entity, ImmutableList.copyOf( filters ), ImmutableList.copyOf( projects ) );
        }


        @Override
        public AlgDataType deriveRowType() {
            final AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
            final List<AlgDataTypeField> fieldList = entity.getTupleType().getFields();
            for ( int project : projects ) {
                builder.add( fieldList.get( project ) );
            }
            return builder.build();
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public AlgWriter explainTerms( AlgWriter pw ) {
            return super.explainTerms( pw )
                    .itemIf( "filters", filters, !filters.isEmpty() )
                    .itemIf( "projects", projects, !projects.equals( identity() ) );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            // Cost factor for pushing filters
            double f = filters.isEmpty() ? 1d : 0.5d;

            // Cost factor for pushing fields
            // The "+ 2d" on top and bottom keeps the function fairly smooth.
            double p = ((double) projects.size() + 2d) / ((double) entity.getTupleType().getFieldCount() + 2d);

            // Multiply the cost by a factor that makes a relScan more attractive if filters and projects are pushed to the table relScan
            return super.computeSelfCost( planner, mq ).multiplyBy( f * p * 0.01d * 100.0d );  //TODO(s3lph): Temporary *100, otherwise foreign key enforcement breaks
        }


        @Override
        public String algCompareString() {
            return getClass().getSimpleName() + "$" +
                    "." + entity.id +
                    (filters != null ? filters.stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                    (projects != null ? projects.toString() : "") + "&";
        }


        public static boolean canHandle( Entity entity ) {
            return entity.unwrap( ScannableEntity.class ).isPresent()
                    || entity.unwrap( FilterableEntity.class ).isPresent()
                    || entity.unwrap( ProjectableFilterableEntity.class ).isPresent();
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
            // TODO: filterable and projectable
            return entity.unwrap( ScannableEntity.class ).orElseThrow().scan( dataContext );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            throw new UnsupportedOperationException(); // TODO:
        }


        //
        // TODO: This might be to restrictive
        //
        @Override
        public boolean isImplementationCacheable() {
            return false;
        }

    }


    /**
     * Rule that converts a {@link Filter} to bindable convention.
     */
    public static class BindableFilterRule extends ConverterRule {

        /**
         * Creates a BindableFilterRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableFilterRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalRelFilter.class, AlgOptUtil::containsMultisetOrWindowedAgg, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableFilterRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelFilter filter = (LogicalRelFilter) alg;
            return BindableFilter.create( convert( filter.getInput(), filter.getInput().getTraitSet().replace( BindableConvention.INSTANCE ) ), filter.getCondition() );
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Filter} in bindable convention.
     */
    public static class BindableFilter extends Filter implements BindableAlg {

        public BindableFilter( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
            super( cluster, traitSet, input, condition );
            assert getConvention() instanceof BindableConvention;
        }


        /**
         * Creates a BindableFilter.
         */
        public static BindableFilter create( final AlgNode input, RexNode condition ) {
            final AlgCluster cluster = input.getCluster();
            final AlgMetadataQuery mq = cluster.getMetadataQuery();
            final AlgTraitSet traitSet = cluster.traitSetOf( BindableConvention.INSTANCE ).replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.filter( mq, input ) );
            return new BindableFilter( cluster, traitSet, input, condition );
        }


        @Override
        public BindableFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
            return new BindableFilter( getCluster(), traitSet, input, condition );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new FilterNode( implementor.compiler, this );
        }

    }


    /**
     * Rule to convert a {@link LogicalRelProject} to a {@link BindableProject}.
     */
    public static class BindableProjectRule extends ConverterRule {

        /**
         * Creates a BindableProjectRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableProjectRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalRelProject.class, (Predicate<LogicalRelProject>) AlgOptUtil::containsMultisetOrWindowedAgg, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableProjectRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelProject project = (LogicalRelProject) alg;
            return new BindableProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( BindableConvention.INSTANCE ),
                    convert( project.getInput(), project.getInput().getTraitSet().replace( BindableConvention.INSTANCE ) ),
                    project.getProjects(),
                    project.getTupleType() );
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Project} in bindable calling convention.
     */
    public static class BindableProject extends Project implements BindableAlg {

        public BindableProject( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
            super( cluster, traitSet, input, projects, rowType );
            assert getConvention() instanceof BindableConvention;
        }


        @Override
        public BindableProject copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
            return new BindableProject( getCluster(), traitSet, input, projects, rowType );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
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
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableSortRule( AlgBuilderFactory algBuilderFactory ) {
            super( Sort.class, (Predicate<AlgNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableSortRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Sort sort = (Sort) alg;
            final AlgTraitSet traitSet = sort.getTraitSet().replace( BindableConvention.INSTANCE );
            final AlgNode input = sort.getInput();
            return new BindableSort( alg.getCluster(), traitSet, convert( input, input.getTraitSet().replace( BindableConvention.INSTANCE ) ), sort.getCollation(), sort.offset, sort.fetch );
        }

    }


    /**
     * Implementation of {@link Sort} bindable calling convention.
     */
    public static class BindableSort extends Sort implements BindableAlg {

        public BindableSort( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traitSet, input, collation, null, offset, fetch );
            assert getConvention() instanceof BindableConvention;
        }


        @Override
        public BindableSort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, ImmutableList<RexNode> nodes, RexNode offset, RexNode fetch ) {
            return new BindableSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new SortNode( implementor.compiler, this );
        }

    }


    /**
     * Rule to convert a {@link LogicalRelJoin} to a {@link BindableJoin}.
     */
    public static class BindableJoinRule extends ConverterRule {

        /**
         * Creates a BindableJoinRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableJoinRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalRelJoin.class, (Predicate<AlgNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableJoinRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelJoin join = (LogicalRelJoin) alg;
            final BindableConvention out = BindableConvention.INSTANCE;
            final AlgTraitSet traitSet = join.getTraitSet().replace( out );
            return new BindableJoin(
                    alg.getCluster(),
                    traitSet,
                    convert( join.getLeft(), join.getLeft().getTraitSet().replace( BindableConvention.INSTANCE ) ),
                    convert( join.getRight(), join.getRight().getTraitSet().replace( BindableConvention.INSTANCE ) ),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType() );
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Join} in bindable calling convention.
     */
    public static class BindableJoin extends Join implements BindableAlg {

        /**
         * Creates a BindableJoin.
         */
        protected BindableJoin( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType ) {
            super( cluster, traitSet, left, right, condition, variablesSet, joinType );
        }


        @Override
        public BindableJoin copy( AlgTraitSet traitSet, RexNode conditionExpr, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
            return new BindableJoin( getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new JoinNode( implementor.compiler, this );
        }

    }


    /**
     * Rule to convert an {@link LogicalRelUnion} to a {@link BindableUnion}.
     */
    public static class BindableUnionRule extends ConverterRule {

        /**
         * Creates a BindableUnionRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableUnionRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalRelUnion.class, (Predicate<AlgNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableUnionRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelUnion union = (LogicalRelUnion) alg;
            final BindableConvention out = BindableConvention.INSTANCE;
            final AlgTraitSet traitSet = union.getTraitSet().replace( out );
            return new BindableUnion( alg.getCluster(), traitSet, convertList( union.getInputs(), out ), union.all );
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Union} in bindable calling convention.
     */
    public static class BindableUnion extends Union implements BindableAlg {

        public BindableUnion( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
            super( cluster, traitSet, inputs, all );
        }


        @Override
        public BindableUnion copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
            return new BindableUnion( getCluster(), traitSet, inputs, all );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new UnionNode( implementor.compiler, this );
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Values} in bindable calling convention.
     */
    public static class BindableValues extends Values implements BindableAlg {

        BindableValues( AlgCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traitSet ) {
            super( cluster, rowType, tuples, traitSet );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            assert inputs.isEmpty();
            return new BindableValues( getCluster(), rowType, tuples, traitSet );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
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
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableValuesRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalRelValues.class, (Predicate<AlgNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableValuesRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LogicalRelValues values = (LogicalRelValues) alg;
            return new BindableValues( values.getCluster(), values.getTupleType(), values.getTuples(), values.getTraitSet().replace( BindableConvention.INSTANCE ) );
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Aggregate} in bindable calling convention.
     */
    public static class BindableAggregate extends Aggregate implements BindableAlg {

        public BindableAggregate( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) throws InvalidAlgException {
            super( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );
            assert getConvention() instanceof BindableConvention;

            for ( AggregateCall aggCall : aggCalls ) {
                if ( aggCall.isDistinct() ) {
                    throw new InvalidAlgException( "distinct aggregation not supported" );
                }
                AggImplementor implementor2 = RexImpTable.INSTANCE.get( aggCall.getAggregation(), false );
                if ( implementor2 == null ) {
                    throw new InvalidAlgException( "aggregation " + aggCall.getAggregation() + " not supported" );
                }
            }
        }


        @Override
        public BindableAggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            try {
                return new BindableAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
            } catch ( InvalidAlgException e ) {
                // Semantic error not possible. Must be a bug. Convert to internal error.
                throw new AssertionError( e );
            }
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
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
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableAggregateRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalRelAggregate.class, (Predicate<AlgNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableAggregateRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelAggregate agg = (LogicalRelAggregate) alg;
            final AlgTraitSet traitSet = agg.getTraitSet().replace( BindableConvention.INSTANCE );
            try {
                return new BindableAggregate( alg.getCluster(), traitSet, convert( agg.getInput(), traitSet ), agg.indicator, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList() );
            } catch ( InvalidAlgException e ) {
                AlgPlanner.LOGGER.debug( e.toString() );
                return null;
            }
        }

    }


    /**
     * Implementation of {@link org.polypheny.db.algebra.core.Window} in bindable convention.
     */
    public static class BindableWindow extends Window implements BindableAlg {

        /**
         * Creates an BindableWindowRel.
         */
        BindableWindow( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, List<RexLiteral> constants, AlgDataType rowType, List<Group> groups ) {
            super( cluster, traitSet, input, constants, rowType, groups );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new BindableWindow( getCluster(), traitSet, sole( inputs ), constants, rowType, groups );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( BindableConvention.COST_MULTIPLIER );
        }


        @Override
        public Class<PolyValue[]> getElementType() {
            return PolyValue[].class;
        }


        @Override
        public Enumerable<PolyValue[]> bind( DataContext dataContext ) {
            return help( dataContext, this );
        }


        @Override
        public Node implement( InterpreterImplementor implementor ) {
            return new WindowNode( implementor.compiler, this );
        }

    }


    /**
     * Rule to convert a {@link LogicalWindow} to a {@link BindableWindow}.
     */
    public static class BindableWindowRule extends ConverterRule {

        /**
         * Creates a BindableWindowRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public BindableWindowRule( AlgBuilderFactory algBuilderFactory ) {
            super( LogicalWindow.class, (Predicate<AlgNode>) r -> true, Convention.NONE, BindableConvention.INSTANCE, algBuilderFactory, "BindableWindowRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalWindow winAgg = (LogicalWindow) alg;
            final AlgTraitSet traitSet = winAgg.getTraitSet().replace( BindableConvention.INSTANCE );
            final AlgNode input = winAgg.getInput();
            final AlgNode convertedInput = convert( input, input.getTraitSet().replace( BindableConvention.INSTANCE ) );
            return new BindableWindow( alg.getCluster(), traitSet, convertedInput, winAgg.getConstants(), winAgg.getTupleType(), winAgg.groups );
        }

    }

}

