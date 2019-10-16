/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.tools;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRules;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.NoneToBindableConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCostImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepMatchOrder;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.ChainedRelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.DefaultRelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateExpandDistinctAggregatesRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateReduceFunctionsRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterAggregateTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterCalcMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterProjectTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterToCalcRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinAssociateRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinCommuteRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinPushThroughJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinToMultiJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.LoptOptimizeJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.MultiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.MultiJoinOptimizeBushyRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectCalcMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectToCalcRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SemiJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortProjectTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SubQueryRemoveRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.TableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.RelDecorrelator;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.RelFieldTrimmer;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Utilities for creating {@link Program}s.
 */
public class Programs {

    public static final ImmutableList<RelOptRule> CALC_RULES =
            ImmutableList.of(
                    NoneToBindableConverterRule.INSTANCE,
                    EnumerableRules.ENUMERABLE_CALC_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                    CalcMergeRule.INSTANCE,
                    FilterCalcMergeRule.INSTANCE,
                    ProjectCalcMergeRule.INSTANCE,
                    FilterToCalcRule.INSTANCE,
                    ProjectToCalcRule.INSTANCE,
                    CalcMergeRule.INSTANCE,

                    // REVIEW jvs 9-Apr-2006: Do we still need these two?  Doesn't the combination of CalcMergeRule, FilterToCalcRule, and ProjectToCalcRule have the same effect?
                    FilterCalcMergeRule.INSTANCE,
                    ProjectCalcMergeRule.INSTANCE );

    /**
     * Program that converts filters and projects to {@link Calc}s.
     */
    public static final Program CALC_PROGRAM = calc( DefaultRelMetadataProvider.INSTANCE );

    /**
     * Program that expands sub-queries.
     */
    public static final Program SUB_QUERY_PROGRAM = subQuery( DefaultRelMetadataProvider.INSTANCE );

    public static final ImmutableSet<RelOptRule> RULE_SET =
            ImmutableSet.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    SemiJoinRule.PROJECT,
                    SemiJoinRule.JOIN,
                    TableScanRule.INSTANCE,
                    PolyphenyDbPrepareImpl.COMMUTE
                            ? JoinAssociateRule.INSTANCE
                            : ProjectMergeRule.INSTANCE,
                    FilterTableScanRule.INSTANCE,
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
        return Lists.transform( Arrays.asList( ruleSets ), Programs::of );
    }


    /**
     * Creates a list of programs based on a list of rule sets.
     */
    public static List<Program> listOf( List<RuleSet> ruleSets ) {
        return Lists.transform( ruleSets, Programs::of );
    }


    /**
     * Creates a program from a list of rules.
     */
    public static Program ofRules( RelOptRule... rules ) {
        return of( RuleSets.ofList( rules ) );
    }


    /**
     * Creates a program from a list of rules.
     */
    public static Program ofRules( Iterable<? extends RelOptRule> rules ) {
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
    public static Program hep( Iterable<? extends RelOptRule> rules, boolean noDag, RelMetadataProvider metadataProvider ) {
        final HepProgramBuilder builder = HepProgram.builder();
        for ( RelOptRule rule : rules ) {
            builder.addRuleInstance( rule );
        }
        return of( builder.build(), noDag, metadataProvider );
    }


    /**
     * Creates a program that executes a {@link HepProgram}.
     */
    public static Program of( final HepProgram hepProgram, final boolean noDag, final RelMetadataProvider metadataProvider ) {
        return ( planner, rel, requiredOutputTraits ) -> {
            final HepPlanner hepPlanner = new HepPlanner(
                    hepProgram,
                    null,
                    noDag,
                    null,
                    RelOptCostImpl.FACTORY );

            List<RelMetadataProvider> list = new ArrayList<>();
            if ( metadataProvider != null ) {
                list.add( metadataProvider );
            }
            hepPlanner.registerMetadataProviders( list );
            RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of( list );
            rel.getCluster().setMetadataProvider( plannerChain );

            hepPlanner.setRoot( rel );
            return hepPlanner.findBestExp();
        };
    }


    /**
     * Creates a program that invokes heuristic join-order optimization (via {@link JoinToMultiJoinRule}, {@link MultiJoin} and
     * {@link LoptOptimizeJoinRule}) if there are 6 or more joins (7 or more relations).
     */
    public static Program heuristicJoinOrder( final Iterable<? extends RelOptRule> rules, final boolean bushy, final int minJoinCount ) {
        return ( planner, rel, requiredOutputTraits ) -> {
            final int joinCount = RelOptUtil.countJoins( rel );
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
                final Program program1 = of( hep, false, DefaultRelMetadataProvider.INSTANCE );

                // Create a program that contains a rule to expand a MultiJoin into heuristically ordered joins.
                // We use the rule set passed in, but remove JoinCommuteRule and JoinPushThroughJoinRule, because they cause exhaustive search.
                final List<RelOptRule> list = Lists.newArrayList( rules );
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
            return program.run( planner, rel, requiredOutputTraits );
        };
    }


    public static Program calc( RelMetadataProvider metadataProvider ) {
        return hep( CALC_RULES, true, metadataProvider );
    }


    @Deprecated // to be removed before 2.0
    public static Program subquery( RelMetadataProvider metadataProvider ) {
        return subQuery( metadataProvider );
    }


    public static Program subQuery( RelMetadataProvider metadataProvider ) {
        return hep(
                ImmutableList.of(
                        (RelOptRule) SubQueryRemoveRule.FILTER,
                        SubQueryRemoveRule.PROJECT,
                        SubQueryRemoveRule.JOIN ),
                true,
                metadataProvider );
    }


    public static Program getProgram() {
        return ( planner, rel, requiredOutputTraits ) -> null;
    }


    /**
     * Returns the standard program used by Prepare.
     */
    public static Program standard() {
        return standard( DefaultRelMetadataProvider.INSTANCE );
    }


    /**
     * Returns the standard program with user metadata provider.
     */
    public static Program standard( RelMetadataProvider metadataProvider ) {
        final Program program1 =
                ( planner, rel, requiredOutputTraits ) -> {
                    planner.setRoot( rel );

                    final RelNode rootRel2 =
                            rel.getTraitSet().equals( requiredOutputTraits )
                                    ? rel
                                    : planner.changeTraits( rel, requiredOutputTraits );
                    assert rootRel2 != null;

                    planner.setRoot( rootRel2 );
                    final RelOptPlanner planner2 = planner.chooseDelegate();
                    final RelNode rootRel3 = planner2.findBestExp();
                    assert rootRel3 != null : "could not implement exp";
                    return rootRel3;
                };

        return sequence( subQuery( metadataProvider ),
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
        public RelNode run( RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits ) {
            planner.clear();
            for ( RelOptRule rule : ruleSet ) {
                planner.addRule( rule );
            }
            if ( !rel.getTraitSet().equals( requiredOutputTraits ) ) {
                rel = planner.changeTraits( rel, requiredOutputTraits );
            }
            planner.setRoot( rel );
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
        public RelNode run( RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits ) {
            for ( Program program : programs ) {
                rel = program.run( planner, rel, requiredOutputTraits );
            }
            return rel;
        }
    }


    /**
     * Program that de-correlates a query.
     *
     * To work around "Decorrelator gets field offsets confused if fields have been trimmed", disable field-trimming in {@link SqlToRelConverter}, and run {@link TrimFieldsProgram} after this program.
     */
    private static class DecorrelateProgram implements Program {

        @Override
        public RelNode run( RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits ) {
            final PolyphenyDbConnectionConfig config = planner.getContext().unwrap( PolyphenyDbConnectionConfig.class );
            if ( config != null && config.forceDecorrelate() ) {
                final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create( rel.getCluster(), null );
                return RelDecorrelator.decorrelateQuery( rel, relBuilder );
            }
            return rel;
        }
    }


    /**
     * Program that trims fields.
     */
    private static class TrimFieldsProgram implements Program {

        @Override
        public RelNode run( RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits ) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create( rel.getCluster(), null );
            return new RelFieldTrimmer( null, relBuilder ).trim( rel );
        }
    }
}

