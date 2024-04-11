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

package org.polypheny.db.tools;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgFieldTrimmer;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.enumerable.EnumerableRules;
import org.polypheny.db.algebra.enumerable.common.EnumerableModifyToStreamerRule;
import org.polypheny.db.algebra.enumerable.document.DocumentAggregateToAggregateRule;
import org.polypheny.db.algebra.enumerable.document.DocumentFilterToCalcRule;
import org.polypheny.db.algebra.enumerable.document.DocumentProjectToCalcRule;
import org.polypheny.db.algebra.enumerable.document.DocumentSortToSortRule;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.ChainedAlgMetadataProvider;
import org.polypheny.db.algebra.metadata.DefaultAlgMetadataProvider;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.algebra.rules.AllocationToPhysicalModifyRule;
import org.polypheny.db.algebra.rules.AllocationToPhysicalScanRule;
import org.polypheny.db.algebra.rules.CalcMergeRule;
import org.polypheny.db.algebra.rules.FilterAggregateTransposeRule;
import org.polypheny.db.algebra.rules.FilterCalcMergeRule;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.rules.FilterScanRule;
import org.polypheny.db.algebra.rules.FilterToCalcRule;
import org.polypheny.db.algebra.rules.JoinAssociateRule;
import org.polypheny.db.algebra.rules.JoinCommuteRule;
import org.polypheny.db.algebra.rules.JoinPushThroughJoinRule;
import org.polypheny.db.algebra.rules.JoinToMultiJoinRule;
import org.polypheny.db.algebra.rules.LoptOptimizeJoinRule;
import org.polypheny.db.algebra.rules.MultiJoin;
import org.polypheny.db.algebra.rules.MultiJoinOptimizeBushyRule;
import org.polypheny.db.algebra.rules.ProjectCalcMergeRule;
import org.polypheny.db.algebra.rules.ProjectMergeRule;
import org.polypheny.db.algebra.rules.ProjectToCalcRule;
import org.polypheny.db.algebra.rules.ScanRule;
import org.polypheny.db.algebra.rules.SemiJoinRules;
import org.polypheny.db.algebra.rules.SortProjectTransposeRule;
import org.polypheny.db.algebra.rules.SubQueryRemoveRule;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.interpreter.NoneToBindableConverterRule;
import org.polypheny.db.plan.AlgOptCostImpl;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.hep.HepMatchOrder;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgram;
import org.polypheny.db.plan.hep.HepProgramBuilder;


/**
 * Utilities for creating {@link Program}s.
 */
public class Programs {

    public static final ImmutableList<AlgOptRule> CALC_RULES =
            ImmutableList.of(
                    NoneToBindableConverterRule.INSTANCE,
                    EnumerableRules.ENUMERABLE_CALC_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                    DocumentProjectToCalcRule.INSTANCE,
                    DocumentFilterToCalcRule.INSTANCE,
                    DocumentAggregateToAggregateRule.INSTANCE,
                    DocumentSortToSortRule.INSTANCE,
                    CalcMergeRule.INSTANCE,
                    FilterCalcMergeRule.INSTANCE,
                    ProjectCalcMergeRule.INSTANCE,
                    FilterToCalcRule.INSTANCE,
                    ProjectToCalcRule.INSTANCE,
                    CalcMergeRule.INSTANCE,

                    // REVIEW jvs: Do we still need these two?  Doesn't the combination of CalcMergeRule, FilterToCalcRule, and ProjectToCalcRule have the same effect?
                    FilterCalcMergeRule.INSTANCE,
                    ProjectCalcMergeRule.INSTANCE );

    /**
     * Program that converts filters and projects to {@link Calc}s.
     */
    public static final Program CALC_PROGRAM = calc( DefaultAlgMetadataProvider.INSTANCE );

    /**
     * Program that expands sub-queries.
     */
    public static final Program SUB_QUERY_PROGRAM = subQuery( DefaultAlgMetadataProvider.INSTANCE );

    public static final ImmutableSet<AlgOptRule> RULE_SET =
            ImmutableSet.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_TRUE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_FALSE_RULE,
                    EnumerableRules.ENUMERABLE_STREAMER_RULE,
                    EnumerableRules.ENUMERABLE_CONTEXT_SWITCHER_RULE,
                    EnumerableModifyToStreamerRule.REL_INSTANCE,
                    EnumerableModifyToStreamerRule.DOC_INSTANCE,
                    EnumerableModifyToStreamerRule.GRAPH_INSTANCE,
                    EnumerableRules.ENUMERABLE_BATCH_ITERATOR_RULE,
                    EnumerableRules.ENUMERABLE_CONSTRAINT_ENFORCER_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_MODIFY_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_DOCUMENT_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_CALC_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                    DocumentProjectToCalcRule.INSTANCE,
                    DocumentFilterToCalcRule.INSTANCE,
                    DocumentAggregateToAggregateRule.INSTANCE,
                    DocumentSortToSortRule.INSTANCE,
                    SemiJoinRules.PROJECT,
                    SemiJoinRules.JOIN,
                    ScanRule.INSTANCE,
                    AllocationToPhysicalScanRule.REL_INSTANCE,
                    AllocationToPhysicalScanRule.DOC_INSTANCE,
                    AllocationToPhysicalScanRule.GRAPH_INSTANCE,
                    AllocationToPhysicalModifyRule.REL_INSTANCE,
                    AllocationToPhysicalModifyRule.DOC_INSTANCE,
                    AllocationToPhysicalModifyRule.GRAPH_INSTANCE,
                    RuntimeConfig.JOIN_COMMUTE.getBoolean()
                            ? JoinAssociateRule.INSTANCE
                            : ProjectMergeRule.INSTANCE,
                    FilterScanRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    AggregateReduceFunctionsRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    SortProjectTransposeRule.INSTANCE );


    // private constructor for utility class
    private Programs() {
    }


    /**
     * Creates a program that executes a rule set.
     */
    public static Program of( RuleSet ruleSet ) {
        return new RuleSetProgram( ruleSet );
    }


    /**
     * Creates a list of programs based on an array of rule sets.
     */
    public static List<Program> listOf( RuleSet... ruleSets ) {
        return Arrays.stream( ruleSets ).map( Programs::of ).toList();
    }


    /**
     * Creates a list of programs based on a list of rule sets.
     */
    public static List<Program> listOf( List<RuleSet> ruleSets ) {
        return ruleSets.stream().map( Programs::of ).toList();
    }


    /**
     * Creates a program from a list of rules.
     */
    public static Program ofRules( AlgOptRule... rules ) {
        return of( RuleSets.ofList( rules ) );
    }


    /**
     * Creates a program from a list of rules.
     */
    public static Program ofRules( Iterable<? extends AlgOptRule> rules ) {
        return of( RuleSets.ofList( rules ) );
    }


    /**
     * Creates a program that executes a sequence of programs.
     */
    public static Program sequence( Program... programs ) {
        return new SequenceProgram( ImmutableList.copyOf( programs ) );
    }


    /**
     * Creates a program that executes a list of rules in a HEP planner.
     */
    public static Program hep( Iterable<? extends AlgOptRule> rules, boolean noDag, AlgMetadataProvider metadataProvider ) {
        final HepProgramBuilder builder = HepProgram.builder();
        for ( AlgOptRule rule : rules ) {
            builder.addRuleInstance( rule );
        }
        return of( builder.build(), noDag, metadataProvider );
    }


    /**
     * Creates a program that executes a {@link HepProgram}.
     */
    public static Program of( final HepProgram hepProgram, final boolean noDag, final AlgMetadataProvider metadataProvider ) {
        return ( planner, alg, requiredOutputTraits ) -> {
            final HepPlanner hepPlanner = new HepPlanner(
                    hepProgram,
                    null,
                    noDag,
                    null,
                    AlgOptCostImpl.FACTORY );

            List<AlgMetadataProvider> list = new ArrayList<>();
            if ( metadataProvider != null ) {
                list.add( metadataProvider );
            }
            hepPlanner.registerMetadataProviders( list );
            AlgMetadataProvider plannerChain = ChainedAlgMetadataProvider.of( list );
            alg.getCluster().setMetadataProvider( plannerChain );

            hepPlanner.setRoot( alg );
            return hepPlanner.findBestExp();
        };
    }


    /**
     * Creates a program that invokes heuristic join-order optimization (via {@link JoinToMultiJoinRule}, {@link MultiJoin} and
     * {@link LoptOptimizeJoinRule}) if there are 6 or more joins (7 or more relations).
     */
    public static Program heuristicJoinOrder( final Iterable<? extends AlgOptRule> rules, final boolean bushy, final int minJoinCount ) {
        return ( planner, alg, requiredOutputTraits ) -> {
            final int joinCount = AlgOptUtil.countJoins( alg );
            final Program program;
            if ( joinCount < minJoinCount ) {
                program = ofRules( rules );
            } else {
                // Create a program that gathers together joins as a MultiJoin.
                final HepProgram hep = new HepProgramBuilder()
                        .addRuleInstance( FilterJoinRule.FILTER_ON_JOIN )
                        .addMatchOrder( HepMatchOrder.BOTTOM_UP )
                        .addRuleInstance( JoinToMultiJoinRule.INSTANCE )
                        .build();
                final Program program1 = of( hep, false, DefaultAlgMetadataProvider.INSTANCE );

                // Create a program that contains a rule to expand a MultiJoin into heuristically ordered joins.
                // We use the rule set passed in, but remove JoinCommuteRule and JoinPushThroughJoinRule, because they cause exhaustive search.
                final List<AlgOptRule> list = Lists.newArrayList( rules );
                list.removeAll(
                        ImmutableList.of(
                                JoinCommuteRule.INSTANCE,
                                JoinAssociateRule.INSTANCE,
                                JoinPushThroughJoinRule.LEFT,
                                JoinPushThroughJoinRule.RIGHT ) );
                list.add( bushy
                        ? MultiJoinOptimizeBushyRule.INSTANCE
                        : LoptOptimizeJoinRule.INSTANCE );
                final Program program2 = ofRules( list );

                program = sequence( program1, program2 );
            }
            return program.run( planner, alg, requiredOutputTraits );
        };
    }


    public static Program calc( AlgMetadataProvider metadataProvider ) {
        return hep( CALC_RULES, true, metadataProvider );
    }


    public static Program subQuery( AlgMetadataProvider metadataProvider ) {
        return hep(
                ImmutableList.of(
                        (AlgOptRule) SubQueryRemoveRule.FILTER,
                        SubQueryRemoveRule.PROJECT,
                        SubQueryRemoveRule.JOIN ),
                true,
                metadataProvider );
    }


    public static Program getProgram() {
        return ( planner, alg, requiredOutputTraits ) -> null;
    }


    /**
     * Returns the standard program used by Prepare.
     */
    public static Program standard() {
        return standard( DefaultAlgMetadataProvider.INSTANCE );
    }


    /**
     * Returns the standard program with user metadata provider.
     */
    public static Program standard( AlgMetadataProvider metadataProvider ) {
        final Program program1 =
                ( planner, alg, requiredOutputTraits ) -> {
                    planner.setRoot( alg );

                    final AlgNode rootAlg2 =
                            alg.getTraitSet().equals( requiredOutputTraits )
                                    ? alg
                                    : planner.changeTraits( alg, requiredOutputTraits );
                    assert rootAlg2 != null;

                    planner.setRoot( rootAlg2 );
                    final AlgPlanner planner2 = planner.chooseDelegate();
                    final AlgNode rootAlg3 = planner2.findBestExp();
                    assert rootAlg3 != null : "could not implement exp";
                    return rootAlg3;
                };

        return sequence(
                subQuery( metadataProvider ),
                new DecorrelateProgram(),
                new TrimFieldsProgram(),
                program1,

                // Second planner pass to do physical "tweaks". This the first time that EnumerableCalcRel is introduced.
                calc( metadataProvider ) );
    }


    /**
     * Program backed by a {@link RuleSet}.
     */
    static class RuleSetProgram implements Program {

        final RuleSet ruleSet;


        private RuleSetProgram( RuleSet ruleSet ) {
            this.ruleSet = ruleSet;
        }


        @Override
        public AlgNode run( AlgPlanner planner, AlgNode alg, AlgTraitSet requiredOutputTraits ) {
            planner.clear();
            for ( AlgOptRule rule : ruleSet ) {
                planner.addRule( rule );
            }
            if ( !alg.getTraitSet().equals( requiredOutputTraits ) ) {
                alg = planner.changeTraits( alg, requiredOutputTraits );
            }
            planner.setRoot( alg );
            return planner.findBestExp();

        }

    }


    /**
     * Program that runs sub-programs, sending the output of the previous as input to the next.
     */
    private static class SequenceProgram implements Program {

        private final ImmutableList<Program> programs;


        SequenceProgram( ImmutableList<Program> programs ) {
            this.programs = programs;
        }


        @Override
        public AlgNode run( AlgPlanner planner, AlgNode alg, AlgTraitSet requiredOutputTraits ) {
            for ( Program program : programs ) {
                alg = program.run( planner, alg, requiredOutputTraits );
            }
            return alg;
        }

    }


    /**
     * Program that de-correlates a query.
     * <p>
     * To work around "Decorrelator gets field offsets confused if fields have been trimmed", disable field-trimming in
     * {#@link SqlToRelConverter}, and run {@link TrimFieldsProgram} after this program.
     */
    private static class DecorrelateProgram implements Program {

        @Override
        public AlgNode run( AlgPlanner planner, AlgNode alg, AlgTraitSet requiredOutputTraits ) {
            Optional<PolyphenyDbConnectionConfig> oConfig = planner.getContext().unwrap( PolyphenyDbConnectionConfig.class );
            if ( oConfig.isPresent() && oConfig.get().forceDecorrelate() ) {
                final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( alg.getCluster(), null );
                return AlgDecorrelator.decorrelateQuery( alg, algBuilder );
            }
            return alg;
        }

    }


    /**
     * Program that trims fields.
     */
    private static class TrimFieldsProgram implements Program {

        @Override
        public AlgNode run( AlgPlanner planner, AlgNode alg, AlgTraitSet requiredOutputTraits ) {
            final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( alg.getCluster(), null );
            return new AlgFieldTrimmer( null, algBuilder ).trim( alg );
        }

    }

}

