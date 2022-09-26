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

package org.polypheny.db.adapter.druid;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.Interval;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.rules.AggregateExtractProjectRule;
import org.polypheny.db.algebra.rules.AggregateFilterTransposeRule;
import org.polypheny.db.algebra.rules.FilterAggregateTransposeRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.rules.ProjectFilterTransposeRule;
import org.polypheny.db.algebra.rules.ProjectSortTransposeRule;
import org.polypheny.db.algebra.rules.SortProjectTransposeRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for {@link DruidQuery}.
 */
public class DruidRules {

    private DruidRules() {
    }


    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    public static final DruidFilterRule FILTER = new DruidFilterRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidProjectRule PROJECT = new DruidProjectRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidAggregateRule AGGREGATE = new DruidAggregateRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidAggregateProjectRule AGGREGATE_PROJECT = new DruidAggregateProjectRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidSortRule SORT = new DruidSortRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidSortProjectTransposeRule SORT_PROJECT_TRANSPOSE = new DruidSortProjectTransposeRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidProjectSortTransposeRule PROJECT_SORT_TRANSPOSE = new DruidProjectSortTransposeRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE = new DruidProjectFilterTransposeRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidFilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE = new DruidFilterProjectTransposeRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidAggregateFilterTransposeRule AGGREGATE_FILTER_TRANSPOSE = new DruidAggregateFilterTransposeRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidFilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE = new DruidFilterAggregateTransposeRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidPostAggregationProjectRule POST_AGGREGATION_PROJECT = new DruidPostAggregationProjectRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidAggregateExtractProjectRule PROJECT_EXTRACT_RULE = new DruidAggregateExtractProjectRule( AlgFactories.LOGICAL_BUILDER );
    public static final DruidHavingFilterRule DRUID_HAVING_FILTER_RULE = new DruidHavingFilterRule( AlgFactories.LOGICAL_BUILDER );

    public static final List<AlgOptRule> RULES =
            ImmutableList.of(
                    FILTER,
                    PROJECT_FILTER_TRANSPOSE,
                    AGGREGATE_FILTER_TRANSPOSE,
                    AGGREGATE_PROJECT,
                    PROJECT_EXTRACT_RULE,
                    PROJECT,
                    POST_AGGREGATION_PROJECT,
                    AGGREGATE,
                    FILTER_AGGREGATE_TRANSPOSE,
                    FILTER_PROJECT_TRANSPOSE,
                    PROJECT_SORT_TRANSPOSE,
                    SORT,
                    SORT_PROJECT_TRANSPOSE,
                    DRUID_HAVING_FILTER_RULE );


    /**
     * Rule to push a {@link Filter} into a {@link DruidQuery}.
     */
    public static class DruidFilterRule extends AlgOptRule {

        /**
         * Creates a DruidFilterRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidFilterRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Filter.class, operand( DruidQuery.class, none() ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Filter filter = call.alg( 0 );
            final DruidQuery query = call.alg( 1 );
            final AlgOptCluster cluster = filter.getCluster();
            final AlgBuilder algBuilder = call.builder();
            final RexBuilder rexBuilder = cluster.getRexBuilder();

            if ( !DruidQuery.isValidSignature( query.signature() + 'f' ) ) {
                return;
            }

            final List<RexNode> validPreds = new ArrayList<>();
            final List<RexNode> nonValidPreds = new ArrayList<>();
            final RexExecutor executor = Util.first( cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR );
            final AlgOptPredicateList predicates = call.getMetadataQuery().getPulledUpPredicates( filter.getInput() );
            final RexSimplify simplify = new RexSimplify( rexBuilder, predicates, executor );
            final RexNode cond = simplify.simplifyUnknownAsFalse( filter.getCondition() );
            for ( RexNode e : AlgOptUtil.conjunctions( cond ) ) {
                DruidJsonFilter druidJsonFilter = DruidJsonFilter.toDruidFilters( e, filter.getInput().getRowType(), query );
                if ( druidJsonFilter != null ) {
                    validPreds.add( e );
                } else {
                    nonValidPreds.add( e );
                }
            }

            // Timestamp
            int timestampFieldIdx = query.getRowType().getFieldNames().indexOf( query.druidTable.timestampFieldName );
            AlgNode newDruidQuery = query;
            final Triple<List<RexNode>, List<RexNode>, List<RexNode>> triple = splitFilters( rexBuilder, query, validPreds, nonValidPreds, timestampFieldIdx );
            if ( triple.getLeft().isEmpty() && triple.getMiddle().isEmpty() ) {
                // it sucks, nothing to push
                return;
            }
            final List<RexNode> residualPreds = new ArrayList<>( triple.getRight() );
            List<Interval> intervals = null;
            if ( !triple.getLeft().isEmpty() ) {
                final String timeZone = cluster.getPlanner().getContext().unwrap( PolyphenyDbConnectionConfig.class ).timeZone();
                assert timeZone != null;
                intervals = DruidDateTimeUtils.createInterval( RexUtil.composeConjunction( rexBuilder, triple.getLeft() ) );
                if ( intervals == null || intervals.isEmpty() ) {
                    // Case we have a filter with extract that can not be written as interval push down
                    triple.getMiddle().addAll( triple.getLeft() );
                }
            }

            if ( !triple.getMiddle().isEmpty() ) {
                final AlgNode newFilter = filter.copy( filter.getTraitSet(), Util.last( query.algs ), RexUtil.composeConjunction( rexBuilder, triple.getMiddle() ) );
                newDruidQuery = DruidQuery.extendQuery( query, newFilter );
            }
            if ( intervals != null && !intervals.isEmpty() ) {
                newDruidQuery = DruidQuery.extendQuery( (DruidQuery) newDruidQuery, intervals );
            }
            if ( !residualPreds.isEmpty() ) {
                newDruidQuery = algBuilder
                        .push( newDruidQuery )
                        .filter( residualPreds )
                        .build();
            }
            call.transformTo( newDruidQuery );
        }


        /**
         * Given a list of conditions that contain Druid valid operations and a list that contains those that contain any non-supported operation, it outputs a triple with three different categories:
         * 1-l) condition filters on the timestamp column,
         * 2-m) condition filters that can be pushed to Druid,
         * 3-r) condition filters that cannot be pushed to Druid.
         */
        private static Triple<List<RexNode>, List<RexNode>, List<RexNode>> splitFilters( final RexBuilder rexBuilder, final DruidQuery input, final List<RexNode> validPreds, final List<RexNode> nonValidPreds, final int timestampFieldIdx ) {
            final List<RexNode> timeRangeNodes = new ArrayList<>();
            final List<RexNode> pushableNodes = new ArrayList<>();
            final List<RexNode> nonPushableNodes = new ArrayList<>( nonValidPreds );
            // Number of columns with the dimensions and timestamp
            for ( RexNode conj : validPreds ) {
                final AlgOptUtil.InputReferencedVisitor visitor = new AlgOptUtil.InputReferencedVisitor();
                conj.accept( visitor );
                if ( visitor.inputPosReferenced.contains( timestampFieldIdx ) && visitor.inputPosReferenced.size() == 1 ) {
                    timeRangeNodes.add( conj );
                } else {
                    pushableNodes.add( conj );
                }
            }
            return ImmutableTriple.of( timeRangeNodes, pushableNodes, nonPushableNodes );
        }

    }


    /**
     * Rule to Push a Having {@link Filter} into a {@link DruidQuery}
     */
    public static class DruidHavingFilterRule extends AlgOptRule {

        public DruidHavingFilterRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Filter.class, operand( DruidQuery.class, none() ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Filter filter = call.alg( 0 );
            final DruidQuery query = call.alg( 1 );

            if ( !DruidQuery.isValidSignature( query.signature() + 'h' ) ) {
                return;
            }

            final RexNode cond = filter.getCondition();
            final DruidJsonFilter druidJsonFilter = DruidJsonFilter.toDruidFilters( cond, query.getTopNode().getRowType(), query );
            if ( druidJsonFilter != null ) {
                final AlgNode newFilter = filter.copy( filter.getTraitSet(), Util.last( query.algs ), filter.getCondition() );
                final DruidQuery newDruidQuery = DruidQuery.extendQuery( query, newFilter );
                call.transformTo( newDruidQuery );
            }
        }

    }


    /**
     * Rule to push a {@link Project} into a {@link DruidQuery}.
     */
    public static class DruidProjectRule extends AlgOptRule {

        /**
         * Creates a DruidProjectRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidProjectRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Project.class, operand( DruidQuery.class, none() ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Project project = call.alg( 0 );
            final DruidQuery query = call.alg( 1 );
            final AlgOptCluster cluster = project.getCluster();
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            if ( !DruidQuery.isValidSignature( query.signature() + 'p' ) ) {
                return;
            }

            if ( DruidQuery.computeProjectAsScan( project, query.getTable().getRowType(), query ) != null ) {
                // All expressions can be pushed to Druid in their entirety.
                final AlgNode newProject = project.copy( project.getTraitSet(), ImmutableList.of( Util.last( query.algs ) ) );
                AlgNode newNode = DruidQuery.extendQuery( query, newProject );
                call.transformTo( newNode );
                return;
            }

            final Pair<List<RexNode>, List<RexNode>> pair = splitProjects( rexBuilder, query, project.getProjects() );
            if ( pair == null ) {
                // We can't push anything useful to Druid.
                return;
            }
            final List<RexNode> above = pair.left;
            final List<RexNode> below = pair.right;
            final AlgDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
            final AlgNode input = Util.last( query.algs );
            for ( RexNode e : below ) {
                final String name;
                if ( e instanceof RexInputRef ) {
                    name = input.getRowType().getFieldNames().get( ((RexInputRef) e).getIndex() );
                } else {
                    name = null;
                }
                // TODO (PCP)
                String physicalColumnName = name;
                builder.add( name, physicalColumnName, e.getType() );
            }
            final AlgNode newProject = project.copy( project.getTraitSet(), input, below, builder.build() );
            final DruidQuery newQuery = DruidQuery.extendQuery( query, newProject );
            final AlgNode newProject2 = project.copy( project.getTraitSet(), newQuery, above, project.getRowType() );
            call.transformTo( newProject2 );
        }


        private static Pair<List<RexNode>, List<RexNode>> splitProjects( final RexBuilder rexBuilder, final AlgNode input, List<RexNode> nodes ) {
            final AlgOptUtil.InputReferencedVisitor visitor = new AlgOptUtil.InputReferencedVisitor();
            for ( RexNode node : nodes ) {
                node.accept( visitor );
            }
            if ( visitor.inputPosReferenced.size() == input.getRowType().getFieldCount() ) {
                // All inputs are referenced
                return null;
            }
            final List<RexNode> belowNodes = new ArrayList<>();
            final List<AlgDataType> belowTypes = new ArrayList<>();
            final List<Integer> positions = Lists.newArrayList( visitor.inputPosReferenced );
            for ( int i : positions ) {
                final RexNode node = rexBuilder.makeInputRef( input, i );
                belowNodes.add( node );
                belowTypes.add( node.getType() );
            }
            final List<RexNode> aboveNodes = new ArrayList<>();
            for ( RexNode node : nodes ) {
                aboveNodes.add(
                        node.accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef( RexInputRef ref ) {
                                        final int index = positions.indexOf( ref.getIndex() );
                                        return rexBuilder.makeInputRef( belowTypes.get( index ), index );
                                    }
                                } ) );
            }
            return Pair.of( aboveNodes, belowNodes );
        }

    }


    /**
     * Rule to push a {@link Project} into a {@link DruidQuery} as a Post aggregator.
     */
    public static class DruidPostAggregationProjectRule extends AlgOptRule {

        /**
         * Creates a DruidPostAggregationProjectRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidPostAggregationProjectRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Project.class, operand( DruidQuery.class, none() ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            Project project = call.alg( 0 );
            DruidQuery query = call.alg( 1 );
            if ( !DruidQuery.isValidSignature( query.signature() + 'o' ) ) {
                return;
            }
            boolean hasRexCalls = false;
            for ( RexNode rexNode : project.getChildExps() ) {
                if ( rexNode instanceof RexCall ) {
                    hasRexCalls = true;
                    break;
                }
            }
            // Only try to push down Project when there will be Post aggregators in result DruidQuery
            if ( hasRexCalls ) {

                final AlgNode topNode = query.getTopNode();
                final Aggregate topAgg;
                if ( topNode instanceof Aggregate ) {
                    topAgg = (Aggregate) topNode;
                } else {
                    topAgg = (Aggregate) ((Filter) topNode).getInput();
                }

                for ( RexNode rexNode : project.getProjects() ) {
                    if ( DruidExpressions.toDruidExpression( rexNode, topAgg.getRowType(), query ) == null ) {
                        return;
                    }
                }
                final AlgNode newProject = project.copy( project.getTraitSet(), ImmutableList.of( Util.last( query.algs ) ) );
                final DruidQuery newQuery = DruidQuery.extendQuery( query, newProject );
                call.transformTo( newQuery );
            }
        }

    }


    /**
     * Rule to push an {@link org.polypheny.db.algebra.core.Aggregate} into a {@link DruidQuery}.
     */
    public static class DruidAggregateRule extends AlgOptRule {

        /**
         * Creates a DruidAggregateRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidAggregateRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Aggregate.class, operand( DruidQuery.class, none() ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Aggregate aggregate = call.alg( 0 );
            final DruidQuery query = call.alg( 1 );
            final AlgNode topDruidNode = query.getTopNode();
            final Project project = topDruidNode instanceof Project ? (Project) topDruidNode : null;
            if ( !DruidQuery.isValidSignature( query.signature() + 'a' ) ) {
                return;
            }

            if ( aggregate.indicator || aggregate.getGroupSets().size() != 1 ) {
                return;
            }
            if ( DruidQuery.computeProjectGroupSet( project, aggregate.getGroupSet(), query.table.getRowType(), query ) == null ) {
                return;
            }
            final List<String> aggNames = Util.skip( aggregate.getRowType().getFieldNames(), aggregate.getGroupSet().cardinality() );
            if ( DruidQuery.computeDruidJsonAgg( aggregate.getAggCallList(), aggNames, project, query ) == null ) {
                return;
            }
            final AlgNode newAggregate = aggregate.copy( aggregate.getTraitSet(), ImmutableList.of( query.getTopNode() ) );
            call.transformTo( DruidQuery.extendQuery( query, newAggregate ) );
        }

    }


    /**
     * Rule to push an {@link org.polypheny.db.algebra.core.Aggregate} and {@link Project} into a {@link DruidQuery}.
     */
    public static class DruidAggregateProjectRule extends AlgOptRule {

        /**
         * Creates a DruidAggregateProjectRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidAggregateProjectRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Aggregate.class, operand( Project.class, operand( DruidQuery.class, none() ) ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Aggregate aggregate = call.alg( 0 );
            final Project project = call.alg( 1 );
            final DruidQuery query = call.alg( 2 );
            if ( !DruidQuery.isValidSignature( query.signature() + 'p' + 'a' ) ) {
                return;
            }
            if ( aggregate.indicator || aggregate.getGroupSets().size() != 1 ) {
                return;
            }
            if ( DruidQuery.computeProjectGroupSet( project, aggregate.getGroupSet(), query.table.getRowType(), query ) == null ) {
                return;
            }
            final List<String> aggNames = Util.skip( aggregate.getRowType().getFieldNames(), aggregate.getGroupSet().cardinality() );
            if ( DruidQuery.computeDruidJsonAgg( aggregate.getAggCallList(), aggNames, project, query ) == null ) {
                return;
            }
            final AlgNode newProject = project.copy( project.getTraitSet(), ImmutableList.of( Util.last( query.algs ) ) );
            final AlgNode newAggregate = aggregate.copy( aggregate.getTraitSet(), ImmutableList.of( newProject ) );
            List<Integer> filterRefs = getFilterRefs( aggregate.getAggCallList() );
            final DruidQuery query2;
            if ( filterRefs.size() > 0 ) {
                query2 = optimizeFilteredAggregations( call, query, (Project) newProject, (Aggregate) newAggregate );
            } else {
                final DruidQuery query1 = DruidQuery.extendQuery( query, newProject );
                query2 = DruidQuery.extendQuery( query1, newAggregate );
            }
            call.transformTo( query2 );
        }


        /**
         * Returns an array of unique filter references from the given list of {@link AggregateCall}
         */
        private Set<Integer> getUniqueFilterRefs( List<AggregateCall> calls ) {
            Set<Integer> refs = new HashSet<>();
            for ( AggregateCall call : calls ) {
                if ( call.hasFilter() ) {
                    refs.add( call.filterArg );
                }
            }
            return refs;
        }


        /**
         * Attempts to optimize any aggregations with filters in the DruidQuery.
         *
         * Uses the following steps:
         * <ol>
         * <li>Tries to abstract common filters out into the "filter" field;
         * <li>Eliminates expressions that are always true or always false when possible;
         * <li>ANDs aggregate filters together with the outer filter to allow for pruning of data.
         * </ol>
         *
         * Should be called before pushing both the aggregate and project into Druid. Assumes that at least one aggregate call has a filter attached to it.
         */
        private DruidQuery optimizeFilteredAggregations( AlgOptRuleCall call, DruidQuery query, Project project, Aggregate aggregate ) {
            Filter filter = null;
            final RexBuilder builder = query.getCluster().getRexBuilder();
            final RexExecutor executor = Util.first( query.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR );
            final AlgNode scan = query.algs.get( 0 ); // first alg is the table scan
            final AlgOptPredicateList predicates = call.getMetadataQuery().getPulledUpPredicates( scan );
            final RexSimplify simplify = new RexSimplify( builder, predicates, executor );

            // if the druid query originally contained a filter
            boolean containsFilter = false;
            for ( AlgNode node : query.algs ) {
                if ( node instanceof Filter ) {
                    filter = (Filter) node;
                    containsFilter = true;
                    break;
                }
            }

            // if every aggregate call has a filter arg reference
            boolean allHaveFilters = allAggregatesHaveFilters( aggregate.getAggCallList() );

            Set<Integer> uniqueFilterRefs = getUniqueFilterRefs( aggregate.getAggCallList() );

            // One of the pre-conditions for this method
            assert uniqueFilterRefs.size() > 0;

            List<AggregateCall> newCalls = new ArrayList<>();

            // OR all the filters so that they can ANDed to the outer filter
            List<RexNode> disjunctions = new ArrayList<>();
            for ( Integer i : uniqueFilterRefs ) {
                disjunctions.add( stripFilter( project.getProjects().get( i ) ) );
            }
            RexNode filterNode = RexUtil.composeDisjunction( builder, disjunctions );

            // Erase references to filters
            for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
                if ( (uniqueFilterRefs.size() == 1
                        && allHaveFilters) // filters get extracted
                        || project.getProjects().get( aggCall.filterArg ).isAlwaysTrue() ) {
                    aggCall = aggCall.copy( aggCall.getArgList(), -1, aggCall.collation );
                }
                newCalls.add( aggCall );
            }
            aggregate = aggregate.copy( aggregate.getTraitSet(), aggregate.getInput(), aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), newCalls );

            if ( containsFilter ) {
                // AND the current filterNode with the filter node inside filter
                filterNode = builder.makeCall( OperatorRegistry.get( OperatorName.AND ), filterNode, filter.getCondition() );
            }

            // Simplify the filter as much as possible
            RexNode tempFilterNode = filterNode;
            filterNode = simplify.simplifyUnknownAsFalse( filterNode );

            // It's possible that after simplification that the expression is now always false. Druid cannot handle such a filter.
            // This will happen when the below expression (f_n+1 may not exist):
            // f_n+1 AND (f_1 OR f_2 OR ... OR f_n) simplifies to be something always false.
            // f_n+1 cannot be false, since it came from a pushed filter alg node and each f_i cannot be false, since DruidAggregateProjectRule would have caught that.
            // So, the only solution is to revert back to the un simplified version and let Druid handle a filter that is ultimately unsatisfiable.
            if ( filterNode.isAlwaysFalse() ) {
                filterNode = tempFilterNode;
            }

            filter = LogicalFilter.create( scan, filterNode );

            boolean addNewFilter = !filter.getCondition().isAlwaysTrue() && allHaveFilters;
            // Assumes that Filter nodes are always right after Scan nodes (which are always present)
            int startIndex = containsFilter && addNewFilter ? 2 : 1;

            List<AlgNode> newNodes = constructNewNodes( query.algs, addNewFilter, startIndex, filter, project, aggregate );

            return DruidQuery.create( query.getCluster(), aggregate.getTraitSet().replace( query.getConvention() ), query.getTable(), query.druidTable, newNodes );
        }


        // Returns true if and only if every AggregateCall in calls has a filter argument.
        private static boolean allAggregatesHaveFilters( List<AggregateCall> calls ) {
            for ( AggregateCall call : calls ) {
                if ( !call.hasFilter() ) {
                    return false;
                }
            }
            return true;
        }


        /**
         * Returns a new List of RelNodes in the order of the given order of the oldNodes, the given {@link Filter}, and any extra nodes.
         */
        private static List<AlgNode> constructNewNodes( List<AlgNode> oldNodes, boolean addFilter, int startIndex, AlgNode filter, AlgNode... trailingNodes ) {
            List<AlgNode> newNodes = new ArrayList<>();

            // The first item should always be the Table scan, so any filter would go after that
            newNodes.add( oldNodes.get( 0 ) );

            if ( addFilter ) {
                newNodes.add( filter );
                // This is required so that each {@link AlgNode} is linked to the one before it
                if ( startIndex < oldNodes.size() ) {
                    AlgNode next = oldNodes.get( startIndex );
                    newNodes.add( next.copy( next.getTraitSet(), Collections.singletonList( filter ) ) );
                    startIndex++;
                }
            }

            // Add the rest of the nodes from oldNodes
            for ( int i = startIndex; i < oldNodes.size(); i++ ) {
                newNodes.add( oldNodes.get( i ) );
            }

            // Add the trailing nodes (need to link them)
            for ( AlgNode node : trailingNodes ) {
                newNodes.add( node.copy( node.getTraitSet(), Collections.singletonList( Util.last( newNodes ) ) ) );
            }

            return newNodes;
        }


        // Removes the IS_TRUE in front of RexCalls, if they exist
        private static RexNode stripFilter( RexNode node ) {
            if ( node.getKind() == Kind.IS_TRUE ) {
                return ((RexCall) node).getOperands().get( 0 );
            }
            return node;
        }


        private static List<Integer> getFilterRefs( List<AggregateCall> calls ) {
            List<Integer> refs = new ArrayList<>();
            for ( AggregateCall call : calls ) {
                if ( call.hasFilter() ) {
                    refs.add( call.filterArg );
                }
            }
            return refs;
        }

    }


    /**
     * Rule to push an {@link Sort} through a {@link Project}. Useful to transform to complex Druid queries.
     */
    public static class DruidSortProjectTransposeRule extends SortProjectTransposeRule {

        /**
         * Creates a DruidSortProjectTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidSortProjectTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Sort.class, operand( Project.class, operand( DruidQuery.class, none() ) ) ), algBuilderFactory, null );
        }

    }


    /**
     * Rule to push back {@link Project} through a {@link Sort}. Useful if after pushing Sort, we could not push it inside DruidQuery.
     */
    public static class DruidProjectSortTransposeRule extends ProjectSortTransposeRule {

        /**
         * Creates a DruidProjectSortTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidProjectSortTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Project.class, operand( Sort.class, operand( DruidQuery.class, none() ) ) ), algBuilderFactory, null );
        }

    }


    /**
     * Rule to push a {@link Sort} into a {@link DruidQuery}.
     */
    public static class DruidSortRule extends AlgOptRule {

        /**
         * Creates a DruidSortRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidSortRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Sort.class, operand( DruidQuery.class, none() ) ), algBuilderFactory, null );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            final Sort sort = call.alg( 0 );
            final DruidQuery query = call.alg( 1 );
            if ( !DruidQuery.isValidSignature( query.signature() + 'l' ) ) {
                return;
            }
            // Either it is:
            // - a pure limit above a query of type scan
            // - a sort and limit on a dimension/metric part of the druid group by query
            if ( sort.offset != null && RexLiteral.intValue( sort.offset ) != 0 ) {
                // offset not supported by Druid
                return;
            }
            if ( query.getQueryType() == QueryType.SCAN && !AlgOptUtil.isPureLimit( sort ) ) {
                return;
            }

            final AlgNode newSort = sort.copy( sort.getTraitSet(), ImmutableList.of( Util.last( query.algs ) ) );
            call.transformTo( DruidQuery.extendQuery( query, newSort ) );
        }

    }


    /**
     * Rule to push a {@link Project} past a {@link Filter} when {@code Filter} is on top of a {@link DruidQuery}.
     */
    public static class DruidProjectFilterTransposeRule
            extends ProjectFilterTransposeRule {

        /**
         * Creates a DruidProjectFilterTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidProjectFilterTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Project.class, operand( Filter.class, operand( DruidQuery.class, none() ) ) ), expr -> false, algBuilderFactory );
        }

    }


    /**
     * Rule to push a {@link Filter} past a {@link Project} when {@code Project} is on top of a {@link DruidQuery}.
     */
    public static class DruidFilterProjectTransposeRule extends FilterProjectTransposeRule {

        /**
         * Creates a DruidFilterProjectTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidFilterProjectTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Filter.class, operand( Project.class, operand( DruidQuery.class, none() ) ) ), true, true, algBuilderFactory );
        }

    }


    /**
     * Rule to push an {@link org.polypheny.db.algebra.core.Aggregate} past a {@link Filter} when {@code Filter} is on top of a {@link DruidQuery}.
     */
    public static class DruidAggregateFilterTransposeRule extends AggregateFilterTransposeRule {

        /**
         * Creates a DruidAggregateFilterTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidAggregateFilterTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Aggregate.class, operand( Filter.class, operand( DruidQuery.class, none() ) ) ), algBuilderFactory );
        }

    }


    /**
     * Rule to push an {@link Filter} past an {@link org.polypheny.db.algebra.core.Aggregate} when {@code Aggregate} is on top of a {@link DruidQuery}.
     */
    public static class DruidFilterAggregateTransposeRule extends FilterAggregateTransposeRule {

        /**
         * Creates a DruidFilterAggregateTransposeRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidFilterAggregateTransposeRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Filter.class, operand( Aggregate.class, operand( DruidQuery.class, none() ) ) ), algBuilderFactory );
        }

    }


    /**
     * Rule to extract a {@link Project} from {@link org.polypheny.db.algebra.core.Aggregate} on top of {@link DruidQuery} based on the fields used in the aggregate.
     */
    public static class DruidAggregateExtractProjectRule extends AggregateExtractProjectRule {

        /**
         * Creates a DruidAggregateExtractProjectRule.
         *
         * @param algBuilderFactory Builder for relational expressions
         */
        public DruidAggregateExtractProjectRule( AlgBuilderFactory algBuilderFactory ) {
            super( operand( Aggregate.class, operand( DruidQuery.class, none() ) ), algBuilderFactory );
        }

    }

}

