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
 */

package org.polypheny.db.processing;


import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.EnumerableBindable.EnumerableToBindableConverterRule;
import org.polypheny.db.adapter.enumerable.EnumerableInterpreterRule;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.interpreter.Bindables;
import org.polypheny.db.jdbc.PolyphenyDbPrepare.SparkHandler;
import org.polypheny.db.jdbc.PolyphenyDbPrepare.SparkHandler.RuleSetBuilder;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.volcano.VolcanoCost;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.rel.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.rel.rules.AggregateValuesRule;
import org.polypheny.db.rel.rules.FilterAggregateTransposeRule;
import org.polypheny.db.rel.rules.FilterJoinRule;
import org.polypheny.db.rel.rules.FilterProjectTransposeRule;
import org.polypheny.db.rel.rules.FilterTableScanRule;
import org.polypheny.db.rel.rules.JoinAssociateRule;
import org.polypheny.db.rel.rules.JoinCommuteRule;
import org.polypheny.db.rel.rules.JoinPushExpressionsRule;
import org.polypheny.db.rel.rules.JoinPushThroughJoinRule;
import org.polypheny.db.rel.rules.ProjectFilterTransposeRule;
import org.polypheny.db.rel.rules.ProjectMergeRule;
import org.polypheny.db.rel.rules.ProjectTableScanRule;
import org.polypheny.db.rel.rules.ProjectWindowTransposeRule;
import org.polypheny.db.rel.rules.ReduceExpressionsRule;
import org.polypheny.db.rel.rules.SortJoinTransposeRule;
import org.polypheny.db.rel.rules.SortProjectTransposeRule;
import org.polypheny.db.rel.rules.SortRemoveConstantKeysRule;
import org.polypheny.db.rel.rules.SortUnionTransposeRule;
import org.polypheny.db.rel.rules.TableScanRule;
import org.polypheny.db.rel.rules.ValuesReduceRule;
import org.polypheny.db.rel.stream.StreamRules;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.transaction.Transaction;


public class VolcanoQueryProcessor extends AbstractQueryProcessor {

    @Getter
    private final VolcanoPlanner planner;


    public static final List<RelOptRule> ENUMERABLE_RULES =
            ImmutableList.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE );

    public static final List<RelOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    TableScanRule.INSTANCE,
                    RuntimeConfig.JOIN_COMMUTE.getBoolean()
                            ? JoinAssociateRule.INSTANCE
                            : ProjectMergeRule.INSTANCE,
                    FilterTableScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    AggregateReduceFunctionsRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    ProjectWindowTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    SortProjectTransposeRule.INSTANCE,
                    SortJoinTransposeRule.INSTANCE,
                    SortRemoveConstantKeysRule.INSTANCE,
                    SortUnionTransposeRule.INSTANCE );

    public static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
            ImmutableList.of(
                    ReduceExpressionsRule.PROJECT_INSTANCE,
                    ReduceExpressionsRule.FILTER_INSTANCE,
                    ReduceExpressionsRule.CALC_INSTANCE,
                    ReduceExpressionsRule.JOIN_INSTANCE,
                    ValuesReduceRule.FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_INSTANCE,
                    AggregateValuesRule.INSTANCE );


    protected VolcanoQueryProcessor( Transaction transaction ) {
        super( transaction );
        planner = new VolcanoPlanner( VolcanoCost.FACTORY, Contexts.of( transaction.getPrepareContext().config() ) );
        planner.addRelTraitDef( ConventionTraitDef.INSTANCE );
        if ( ENABLE_COLLATION_TRAIT ) {
            planner.addRelTraitDef( RelCollationTraitDef.INSTANCE );
            planner.registerAbstractRelationalRules();
        }
        RelOptUtil.registerAbstractRels( planner );
        for ( RelOptRule rule : DEFAULT_RULES ) {
            planner.addRule( rule );
        }
        if ( ENABLE_BINDABLE ) {
            for ( RelOptRule rule : Bindables.RULES ) {
                planner.addRule( rule );
            }
        }
        planner.addRule( Bindables.BINDABLE_TABLE_SCAN_RULE );
        planner.addRule( ProjectTableScanRule.INSTANCE );
        planner.addRule( ProjectTableScanRule.INTERPRETER );

        if ( ENABLE_ENUMERABLE ) {
            for ( RelOptRule rule : ENUMERABLE_RULES ) {
                planner.addRule( rule );
            }
            planner.addRule( EnumerableInterpreterRule.INSTANCE );
        }

        if ( ENABLE_BINDABLE && ENABLE_ENUMERABLE ) {
            planner.addRule( EnumerableToBindableConverterRule.INSTANCE );
        }

        if ( ENABLE_STREAM ) {
            for ( RelOptRule rule : StreamRules.RULES ) {
                planner.addRule( rule );
            }
        }

        // Change the below to enable constant-reduction.
        if ( CONSTANT_REDUCTION ) {
            for ( RelOptRule rule : CONSTANT_REDUCTION_RULES ) {
                planner.addRule( rule );
            }
        }

        final SparkHandler spark = transaction.getPrepareContext().spark();
        if ( spark.enabled() ) {
            spark.registerRules(
                    new RuleSetBuilder() {
                        @Override
                        public void addRule( RelOptRule rule ) {
                            // TODO:
                        }


                        @Override
                        public void removeRule( RelOptRule rule ) {
                            // TODO:
                        }
                    } );
        }

        final DataContext dataContext = transaction.getPrepareContext().getDataContext();
        planner.setExecutor( new RexExecutorImpl( dataContext ) );
    }

}
