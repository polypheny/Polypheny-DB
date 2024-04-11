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
 */

package org.polypheny.db.processing;


import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.enumerable.EnumerableInterpreterRule;
import org.polypheny.db.algebra.enumerable.EnumerableRules;
import org.polypheny.db.algebra.enumerable.common.EnumerableBindable.EnumerableToBindableConverterRule;
import org.polypheny.db.algebra.enumerable.common.EnumerableModifyToStreamerRule;
import org.polypheny.db.algebra.enumerable.document.DocumentAggregateToAggregateRule;
import org.polypheny.db.algebra.enumerable.document.DocumentFilterToCalcRule;
import org.polypheny.db.algebra.enumerable.document.DocumentProjectToCalcRule;
import org.polypheny.db.algebra.enumerable.document.DocumentSortToSortRule;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.algebra.rules.AggregateValuesRule;
import org.polypheny.db.algebra.rules.AllocationToPhysicalModifyRule;
import org.polypheny.db.algebra.rules.AllocationToPhysicalScanRule;
import org.polypheny.db.algebra.rules.FilterAggregateTransposeRule;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.rules.FilterScanRule;
import org.polypheny.db.algebra.rules.JoinAssociateRule;
import org.polypheny.db.algebra.rules.JoinCommuteRule;
import org.polypheny.db.algebra.rules.JoinPushExpressionsRule;
import org.polypheny.db.algebra.rules.JoinPushThroughJoinRule;
import org.polypheny.db.algebra.rules.ProjectFilterTransposeRule;
import org.polypheny.db.algebra.rules.ProjectMergeRule;
import org.polypheny.db.algebra.rules.ProjectScanRule;
import org.polypheny.db.algebra.rules.ProjectWindowTransposeRule;
import org.polypheny.db.algebra.rules.ReduceExpressionsRules;
import org.polypheny.db.algebra.rules.ScanRule;
import org.polypheny.db.algebra.rules.SortJoinTransposeRule;
import org.polypheny.db.algebra.rules.SortProjectTransposeRule;
import org.polypheny.db.algebra.rules.SortRemoveConstantKeysRule;
import org.polypheny.db.algebra.rules.SortUnionTransposeRule;
import org.polypheny.db.algebra.rules.ValuesReduceRule;
import org.polypheny.db.algebra.stream.StreamRules;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.interpreter.Bindables;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoCost;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.plan.volcano.VolcanoPlannerPhase;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.transaction.Statement;


@Getter
public class VolcanoQueryProcessor extends AbstractQueryProcessor {

    private final VolcanoPlanner planner;


    public static final List<AlgOptRule> ENUMERABLE_RULES =
            ImmutableList.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_CONSTRAINT_ENFORCER_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_TRUE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_FALSE_RULE,
                    EnumerableRules.ENUMERABLE_CALC_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                    DocumentProjectToCalcRule.INSTANCE,
                    DocumentFilterToCalcRule.INSTANCE,
                    DocumentAggregateToAggregateRule.INSTANCE,
                    DocumentSortToSortRule.INSTANCE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_MODIFY_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableModifyToStreamerRule.REL_INSTANCE,
                    EnumerableModifyToStreamerRule.DOC_INSTANCE,
                    EnumerableModifyToStreamerRule.GRAPH_INSTANCE,
                    EnumerableRules.ENUMERABLE_STREAMER_RULE,
                    EnumerableRules.ENUMERABLE_CONTEXT_SWITCHER_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_DOCUMENT_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TRANSFORMER_RULE,
                    EnumerableRules.ENUMERABLE_GRAPH_MATCH_RULE,
                    EnumerableRules.ENUMERABLE_UNWIND_RULE,
                    EnumerableRules.ENUMERABLE_DOCUMENT_UNWIND_RULE,
                    EnumerableRules.ENUMERABLE_GRAPH_TRANSFORMER_RULE );

    public static final List<AlgOptRule> PRE_PROCESS_RULES =
            ImmutableList.of(
                    AllocationToPhysicalScanRule.REL_INSTANCE,
                    AllocationToPhysicalScanRule.DOC_INSTANCE,
                    AllocationToPhysicalScanRule.GRAPH_INSTANCE,
                    AllocationToPhysicalModifyRule.REL_INSTANCE,
                    AllocationToPhysicalModifyRule.DOC_INSTANCE,
                    AllocationToPhysicalModifyRule.GRAPH_INSTANCE,
                    ScanRule.INSTANCE
            );


    public static final List<AlgOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    RuntimeConfig.JOIN_COMMUTE.getBoolean()
                            ? JoinAssociateRule.INSTANCE
                            : ProjectMergeRule.INSTANCE,
                    FilterScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    DocumentAggregateToAggregateRule.INSTANCE,
                    DocumentSortToSortRule.INSTANCE,
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

    public static final List<AlgOptRule> CONSTANT_REDUCTION_RULES =
            ImmutableList.of(
                    ReduceExpressionsRules.PROJECT_INSTANCE,
                    ReduceExpressionsRules.FILTER_INSTANCE,
                    ReduceExpressionsRules.CALC_INSTANCE,
                    ReduceExpressionsRules.JOIN_INSTANCE,
                    ValuesReduceRule.FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_INSTANCE,
                    AggregateValuesRule.INSTANCE );


    public VolcanoQueryProcessor( Statement statement ) {
        super( statement );
        planner = new VolcanoPlanner( VolcanoCost.FACTORY, Contexts.of( statement.getPrepareContext().config() ) );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        if ( ENABLE_COLLATION_TRAIT ) {
            planner.addAlgTraitDef( AlgCollationTraitDef.INSTANCE );
            planner.registerAbstractAlgebraRules();
        }

        AlgOptUtil.registerAbstractAlgs( planner );
        for ( AlgOptRule preProcessRule : PRE_PROCESS_RULES ) {
            planner.addRule( preProcessRule, VolcanoPlannerPhase.PRE_PROCESS );
        }

        for ( AlgOptRule rule : DEFAULT_RULES ) {
            planner.addRule( rule );
        }

        if ( ENABLE_BINDABLE ) {
            for ( AlgOptRule rule : Bindables.RULES ) {
                planner.addRule( rule );
            }
        }
        planner.addRule( Bindables.BINDABLE_TABLE_SCAN_RULE );
        planner.addRule( ProjectScanRule.INSTANCE );
        planner.addRule( ProjectScanRule.INTERPRETER );

        if ( ENABLE_MODEL_TRAIT ) {
            planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
            planner.registerModelRules();
        }

        if ( ENABLE_ENUMERABLE ) {
            for ( AlgOptRule rule : ENUMERABLE_RULES ) {
                planner.addRule( rule );
            }
            planner.addRule( EnumerableInterpreterRule.INSTANCE );
        }

        if ( ENABLE_BINDABLE && ENABLE_ENUMERABLE ) {
            planner.addRule( EnumerableToBindableConverterRule.INSTANCE );
        }

        if ( ENABLE_STREAM ) {
            for ( AlgOptRule rule : StreamRules.RULES ) {
                planner.addRule( rule );
            }
        }

        // Change the below to enable constant-reduction.
        if ( CONSTANT_REDUCTION ) {
            for ( AlgOptRule rule : CONSTANT_REDUCTION_RULES ) {
                planner.addRule( rule );
            }
        }

        final DataContext dataContext = statement.getPrepareContext().getDataContext();
        planner.setExecutor( new RexExecutorImpl( dataContext ) );
    }

}
