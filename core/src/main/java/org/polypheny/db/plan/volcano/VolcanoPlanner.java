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

package org.polypheny.db.plan.volcano;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.convert.Converter;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.enumerable.common.ModelSwitcherRule;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.JaninoRelMetadataProvider;
import org.polypheny.db.algebra.rules.AggregateJoinTransposeRule;
import org.polypheny.db.algebra.rules.AggregateProjectMergeRule;
import org.polypheny.db.algebra.rules.AggregateRemoveRule;
import org.polypheny.db.algebra.rules.CalcRemoveRule;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.JoinAssociateRule;
import org.polypheny.db.algebra.rules.JoinCommuteRule;
import org.polypheny.db.algebra.rules.LpgToEnumerableRule;
import org.polypheny.db.algebra.rules.SemiJoinRules;
import org.polypheny.db.algebra.rules.SortRemoveRule;
import org.polypheny.db.algebra.rules.UnionToDistinctRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AbstractAlgPlanner;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptCostFactory;
import org.polypheny.db.plan.AlgOptListener;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SaffronProperties;
import org.polypheny.db.util.Util;


/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively according to a dynamic programming algorithm.
 */
@Slf4j
public class VolcanoPlanner extends AbstractAlgPlanner {

    protected static final double COST_IMPROVEMENT = .5;


    protected AlgSubset root;

    /**
     * If true, the planner keeps applying rules as long as they continue to reduce the cost. If false, the planner
     * terminates as soon as it has found any implementation, no matter how expensive.
     */
    public boolean ambitious = true;

    /**
     * If true, and if {@link #ambitious} is true, the planner waits a finite number of iterations for the cost to improve.
     *
     * The number of iterations K is equal to the number of iterations required to get the first finite plan. After the
     * first finite plan, it continues to fire rules to try to improve it. The planner sets a target cost of the current
     * best cost multiplied by {@link #COST_IMPROVEMENT}. If it does not meet that cost target within K steps, it quits,
     * and uses the current best plan. If it meets the cost, it sets a new, lower target, and has another K iterations
     * to meet it. And so forth.
     *
     * If false, the planner continues to fire rules until the rule queue is empty.
     */
    protected boolean impatient = false;

    /**
     * Operands that apply to a given class of {@link AlgNode}.
     *
     * Any operand can be an 'entry point' to a rule call, when a {@link AlgNode} is registered which matches the operand.
     * This map allows us to narrow down operands based on the class of the AlgNode.
     */
    private final Multimap<Class<? extends AlgNode>, AlgOptRuleOperand> classOperands = LinkedListMultimap.create();

    /**
     * List of all sets. Used only for debugging.
     */
    final List<AlgSet> allSets = new ArrayList<>();

    /**
     * Canonical map from {@link String digest} to the unique {@link AlgNode algebra expression} with that digest.
     *
     * Row type is part of the key for the rare occasion that similar expressions have different types, e.g. variants
     * of {@code Project(child=rel#1, a=null)} where a is a null INTEGER or a null VARCHAR(10).
     */
    private final Map<String, AlgNode> mapDigestToAlg = new HashMap<>();

    /**
     * Map each registered expression ({@link AlgNode}) to its equivalence set ({@link AlgSubset}).
     *
     * We use an {@link IdentityHashMap} to simplify the process of merging {@link AlgSet} objects. Most {@link AlgNode}
     * objects are identified by their digest, which involves the set that their child algebra expressions belong to.
     * If those children belong to the same set, we have to be careful, otherwise it gets incestuous.
     */
    private final IdentityHashMap<AlgNode, AlgSubset> mapAlg2Subset = new IdentityHashMap<>();

    /**
     * The importance of algebra expressions.
     *
     * The map contains only RelNodes whose importance has been overridden using
     * {@link AlgPlanner#setImportance(AlgNode, double)}. Other RelNodes are presumed to have 'normal' importance.
     *
     * If a {@link AlgNode} has 0 importance, all {@link AlgOptRuleCall}s using it are ignored, and future
     * RelOptRuleCalls are not queued up.
     */
    final Map<AlgNode, Double> algImportances = new HashMap<>();

    /**
     * Holds rule calls waiting to be fired.
     */
    final RuleQueue ruleQueue = new RuleQueue( this );

    /**
     * Holds the currently registered AlgTraitDefs.
     */
    private final List<AlgTraitDef<?>> traitDefs = new ArrayList<>();

    /**
     * Set of all registered rules.
     */
    protected final Set<AlgOptRule> ruleSet = new HashSet<>();

    private int nextSetId = 0;

    /**
     * Incremented every time an algebra expression is registered or two sets are merged.
     * Tells us whether anything is going on.
     */
    private int registerCount;

    /**
     * Listener for this planner, or null if none set.
     */
    AlgOptListener listener;

    /**
     * Dump of the root algebra expression, as it was before any rules were applied. For debugging.
     */
    private String originalRootString;

    private AlgNode originalRoot;

    /**
     * Whether the planner can accept new rules.
     */
    @Setter
    private boolean locked;

    final Map<AlgNode, Provenance> provenances = new HashMap<>();

    private final Deque<VolcanoRuleCall> ruleCallStack = new ArrayDeque<>();

    /**
     * Zero cost, according to {@link #costFactory}. Not necessarily a {@link VolcanoCost}.
     */
    private final AlgOptCost zeroCost;

    /**
     * Maps rule classes to their name, to ensure that the names are unique and conform to rules.
     */
    private final SetMultimap<String, Class<?>> ruleNames = LinkedHashMultimap.create();


    /**
     * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize it, the caller must register the
     * desired set of algations, rules, and calling conventions.
     */
    public VolcanoPlanner() {
        this( null, null );
    }


    /**
     * Creates a uninitialized <code>VolcanoPlanner</code>. To fully initialize it, the caller must register the
     * desired set of relations, rules, and calling conventions.
     */
    public VolcanoPlanner( Context externalContext ) {
        this( null, externalContext );
    }


    /**
     * Creates a {@code VolcanoPlanner} with a given cost factory.
     */
    public VolcanoPlanner( AlgOptCostFactory costFactory, Context externalContext ) {
        super( costFactory == null ? VolcanoCost.FACTORY : costFactory, externalContext );
        this.zeroCost = this.costFactory.makeZeroCost();
    }


    protected VolcanoPlannerPhaseRuleMappingInitializer
    getPhaseRuleMappingInitializer() {
        return phaseRuleMap -> {
            // Disable all phases except OPTIMIZE by adding one useless rule name.
            phaseRuleMap.get( VolcanoPlannerPhase.PRE_PROCESS_MDR ).add( "xxx" );
            phaseRuleMap.get( VolcanoPlannerPhase.PRE_PROCESS ).add( "xxx" );
            phaseRuleMap.get( VolcanoPlannerPhase.CLEANUP ).add( "xxx" );
        };
    }


    // implement RelOptPlanner
    @Override
    public boolean isRegistered( AlgNode alg ) {
        return mapAlg2Subset.get( alg ) != null;
    }


    @Override
    public void setRoot( AlgNode alg ) {
        // We're registered all the rules, and therefore {@link AlgNode} classes, we're interested in, and have not yet started
        // calling metadata providers.
        // So now is a good time to tell the metadata layer what to expect.
        registerMetadataAlgs();

        this.root = registerImpl( alg, null );
        if ( this.originalRoot == null ) {
            this.originalRoot = alg;
        }
        this.originalRootString = AlgOptUtil.toString( root, ExplainLevel.ALL_ATTRIBUTES );

        // Making a node the root changes its importance.
        this.ruleQueue.recompute( this.root );
        ensureRootConverters();
    }


    @Override
    public AlgNode getRoot() {
        return root;
    }


    /**
     * Finds an expression's equivalence set. If the expression is not registered, returns null.
     *
     * @param alg Relational expression
     * @return Equivalence set that expression belongs to, or null if it is not registered
     */
    public AlgSet getSet( AlgNode alg ) {
        assert alg != null : "pre: alg != null";
        final AlgSubset subset = getSubset( alg );
        if ( subset != null ) {
            assert subset.set != null;
            return subset.set;
        }
        return null;
    }


    @Override
    public boolean addAlgTraitDef( AlgTraitDef<?> algTraitDef ) {
        if ( traitDefs.contains( algTraitDef ) ) {
            return false;
        }
        return traitDefs.add( algTraitDef );
    }


    @Override
    public void clearAlgTraitDefs() {
        traitDefs.clear();
    }


    @Override
    public List<AlgTraitDef<?>> getAlgTraitDefs() {
        return traitDefs;
    }


    @Override
    public AlgTraitSet emptyTraitSet() {
        AlgTraitSet traitSet = super.emptyTraitSet();
        for ( AlgTraitDef<?> traitDef : traitDefs ) {
            if ( traitDef.multiple() ) {
                // TODO: restructure RelTraitSet to allow a list of entries for any given trait
            }
            traitSet = traitSet.plus( traitDef.getDefault() );
        }
        return traitSet;
    }


    @Override
    public void clear() {
        super.clear();
        for ( AlgOptRule rule : ImmutableList.copyOf( ruleSet ) ) {
            removeRule( rule );
        }
        this.classOperands.clear();
        this.allSets.clear();
        this.mapDigestToAlg.clear();
        this.mapAlg2Subset.clear();
        this.algImportances.clear();
        this.ruleQueue.clear();
        this.ruleNames.clear();
    }


    @Override
    public List<AlgOptRule> getRules() {
        return ImmutableList.copyOf( ruleSet );
    }


    @Override
    public boolean addRule( AlgOptRule rule, VolcanoPlannerPhase phase ) {
        if ( ruleSet.contains( rule ) ) {
            // Rule already exists.
            return false;
        }
        ruleQueue.addPhaseRuleMapping( phase, rule );
        return addRule( rule );
    }


    @Override
    public boolean addRule( AlgOptRule rule ) {
        if ( locked ) {
            return false;
        }
        if ( ruleSet.contains( rule ) ) {
            // Rule already exists.
            return false;
        }
        final boolean added = ruleSet.add( rule );
        assert added;

        final String ruleName = rule.toString();
        if ( ruleNames.put( ruleName, rule.getClass() ) ) {
            Set<Class<?>> x = ruleNames.get( ruleName );
            if ( x.size() > 1 ) {
                throw new GenericRuntimeException( "Rule description '" + ruleName + "' is not unique; classes: " + x );
            }
        }

        mapRuleDescription( rule );

        // Each of this rule's operands is an 'entry point' for a rule call. Register each operand against all concrete sub-classes that could match it.
        for ( AlgOptRuleOperand operand : rule.getOperands() ) {
            for ( Class<? extends AlgNode> subClass : subClasses( operand.getMatchedClass() ) ) {
                classOperands.put( subClass, operand );
            }
        }

        // If this is a converter rule, check that it operates on one of the kinds of trait we are interested in, and if so, register the rule with the trait.
        if ( rule instanceof ConverterRule ) {
            ConverterRule converterRule = (ConverterRule) rule;

            final AlgTrait<?> ruleTrait = converterRule.getInTrait();
            final AlgTraitDef<?> ruleTraitDef = ruleTrait.getTraitDef();
            if ( traitDefs.contains( ruleTraitDef ) ) {
                ruleTraitDef.registerConverterRule( this, converterRule );
            }
        }

        return true;
    }


    @Override
    public boolean removeRule( AlgOptRule rule ) {
        if ( !ruleSet.remove( rule ) ) {
            // Rule was not present.
            return false;
        }

        // Remove description.
        unmapRuleDescription( rule );

        // Remove operands.
        classOperands.values().removeIf( entry -> entry.getRule().equals( rule ) );

        // Remove trait mappings. (In particular, entries from conversion graph.)
        if ( rule instanceof ConverterRule ) {
            ConverterRule converterRule = (ConverterRule) rule;
            final AlgTrait<?> ruleTrait = converterRule.getInTrait();
            final AlgTraitDef<?> ruleTraitDef = ruleTrait.getTraitDef();
            if ( traitDefs.contains( ruleTraitDef ) ) {
                ruleTraitDef.deregisterConverterRule( this, converterRule );
            }
        }
        return true;
    }


    @Override
    protected void onNewClass( AlgNode node ) {
        super.onNewClass( node );

        // Create mappings so that instances of this class will match existing operands.
        final Class<? extends AlgNode> clazz = node.getClass();
        for ( AlgOptRule rule : ruleSet ) {
            for ( AlgOptRuleOperand operand : rule.getOperands() ) {
                if ( operand.getMatchedClass().isAssignableFrom( clazz ) ) {
                    classOperands.put( clazz, operand );
                }
            }
        }
    }


    @Override
    public AlgNode changeTraits( final AlgNode alg, AlgTraitSet toTraits ) {
        assert !alg.getTraitSet().equals( toTraits );
        assert toTraits.allSimple();

        AlgSubset alg2 = ensureRegistered( alg, null );
        if ( alg2.getTraitSet().equals( toTraits ) ) {
            return alg2;
        }

        return alg2.set.getOrCreateSubset( alg.getCluster(), toTraits.simplify() );
    }


    /**
     * Finds the most efficient expression to implement the query given via
     * {@link AlgPlanner#setRoot(AlgNode)}.
     *
     * The algorithm executes repeatedly in a series of phases. In each phase the exact rules that may be fired varies.
     * The mapping of phases to rule sets is maintained in the {@link #ruleQueue}.
     *
     * In each phase, the planner sets the initial importance of the existing AlgSubSets ({@link #setInitialImportance()}).
     * The planner then iterates over the rule matches presented by the rule queue until:
     *
     * <ol>
     * <li>The rule queue becomes empty.</li>
     * <li>For ambitious planners: No improvements to the plan have been made recently (specifically within a number of
     * iterations that is 10% of the number of iterations necessary to first reach an implementable plan or 25 iterations
     * whichever is larger).</li>
     * <li>For non-ambitious planners: When an implementable plan is found.</li>
     * </ol>
     *
     * Furthermore, after every 10 iterations without an implementable plan, AlgSubSets that contain only logical RelNodes
     * are given an importance boost via {@link #injectImportanceBoost()}. Once an implementable plan is found,
     * the artificially raised importance values are cleared (see {@link #clearImportanceBoost()}).
     *
     * @return the most efficient {@link AlgNode} tree found for implementing the given query
     */
    @Override
    public AlgNode findBestExp() {
        ensureRootConverters();
        int cumulativeTicks = 0;
        for ( VolcanoPlannerPhase phase : VolcanoPlannerPhase.values() ) {
            setInitialImportance();

            AlgOptCost targetCost = costFactory.makeHugeCost();
            int tick = 0;
            int firstFiniteTick = -1;
            int splitCount = 0;
            int giveUpTick = Integer.MAX_VALUE;

            while ( true ) {
                ++tick;
                ++cumulativeTicks;
                if ( root.bestCost.isLe( targetCost ) ) {
                    if ( firstFiniteTick < 0 ) {
                        firstFiniteTick = cumulativeTicks;
                        clearImportanceBoost();
                    }
                    if ( ambitious ) {
                        // Choose a slightly more ambitious target cost, and try again. If it took us 1000 iterations to
                        // find our first finite plan, give ourselves another 100 iterations to reduce the cost by 10%.
                        targetCost = root.bestCost.multiplyBy( 0.9 );
                        ++splitCount;
                        if ( impatient ) {
                            if ( firstFiniteTick < 10 ) {
                                // It's possible pre-processing can create an implementable plan -- give us
                                // some time to actually optimize it.
                                giveUpTick = cumulativeTicks + 25;
                            } else {
                                giveUpTick = cumulativeTicks + Math.max( firstFiniteTick / 10, 25 );
                            }
                        }
                    } else {
                        break;
                    }
                } else if ( cumulativeTicks > giveUpTick ) {
                    // We haven't made progress recently. Take the current best.
                    break;
                } else if ( root.bestCost.isInfinite() && ((tick % 10) == 0) ) {
                    injectImportanceBoost();
                }

                LOGGER.debug( "PLANNER = {}; TICK = {}/{}; PHASE = {}; COST = {}", this, cumulativeTicks, tick, phase.toString(), root.bestCost );

                VolcanoRuleMatch match = ruleQueue.popMatch( phase );
                if ( match == null ) {
                    break;
                }

                assert match.getRule().matches( match );
                match.onMatch();

                // The root may have been merged with another subset. Find the new root subset.
                root = canonize( root );
            }

            ruleQueue.phaseCompleted( phase );
        }
        if ( LOGGER.isTraceEnabled() ) {
            StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter( sw );
            dump( pw );
            pw.flush();
            LOGGER.trace( sw.toString() );
        }
        AlgNode cheapest = root.buildCheapestPlan( this );
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug( "Cheapest plan:\n{}", AlgOptUtil.toString( cheapest, ExplainLevel.ALL_ATTRIBUTES ) );
            LOGGER.debug( "Provenance:\n{}", provenance( cheapest ) );
        }
        return cheapest;
    }


    /**
     * Informs {@link JaninoRelMetadataProvider} about the different kinds of {@link AlgNode} that we will be dealing with.
     * It will reduce the number of times that we need to re-generate the provider.
     */
    private void registerMetadataAlgs() {
        JaninoRelMetadataProvider.DEFAULT.register( classOperands.keySet() );
    }


    /**
     * Ensures that the subset that is the root algebra expression contains converters to all other subsets
     * in its equivalence set.
     *
     * Thus, the planner tries to find cheap implementations of those other subsets, which can then be converted to the root.
     * This is the only place in the plan where explicit converters are required; elsewhere, a consumer will be asking for
     * the result in a particular convention, but the root has no consumers.
     */
    void ensureRootConverters() {
        final Set<AlgSubset> subsets = new HashSet<>();
        for ( AlgNode alg : root.getAlgs() ) {
            if ( alg instanceof AbstractConverter ) {
                subsets.add( (AlgSubset) ((AbstractConverter) alg).getInput() );
            }
        }
        for ( AlgSubset subset : root.set.subsets ) {
            final ImmutableList<AlgTrait<?>> difference = root.getTraitSet().difference( subset.getTraitSet() );
            if ( difference.size() == 1 && subsets.add( subset ) ) {
                register(
                        new AbstractConverter( subset.getCluster(), subset, difference.get( 0 ).getTraitDef(), root.getTraitSet() ),
                        root );
            }
        }
    }


    /**
     * Returns a multi-line string describing the provenance of a tree of algebra expressions. For each node in the
     * tree, prints the rule that created the node, if any. Recursively describes the provenance of the algebra
     * expressions that are the arguments to that rule.
     *
     * Thus, every algebra expression and rule invocation that affected the final outcome is described in the
     * provenance. This can be useful when finding the root cause of "mistakes" in a query plan.
     *
     * @param root Root algebra expression in a tree
     * @return Multi-line string describing the rules that created the tree
     */
    private String provenance( AlgNode root ) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter( sw );
        final List<AlgNode> nodes = new ArrayList<>();
        new AlgVisitor() {
            @Override
            public void visit( AlgNode node, int ordinal, AlgNode parent ) {
                nodes.add( node );
                super.visit( node, ordinal, parent );
            }
            // CHECKSTYLE: IGNORE 1
        }.go( root );
        final Set<AlgNode> visited = new HashSet<>();
        for ( AlgNode node : nodes ) {
            provenanceRecurse( pw, node, 0, visited );
        }
        pw.flush();
        return sw.toString();
    }


    /**
     * Helper for {@link #provenance(AlgNode)}.
     */
    private void provenanceRecurse( PrintWriter pw, AlgNode node, int i, Set<AlgNode> visited ) {
        Spaces.append( pw, i * 2 );
        if ( !visited.add( node ) ) {
            pw.println( "alg#" + node.getId() + " (see above)" );
            return;
        }
        pw.println( node );
        final Provenance o = provenances.get( node );
        Spaces.append( pw, i * 2 + 2 );
        if ( o == Provenance.EMPTY ) {
            pw.println( "no parent" );
        } else if ( o instanceof DirectProvenance ) {
            AlgNode alg = ((DirectProvenance) o).source;
            pw.println( "direct" );
            provenanceRecurse( pw, alg, i + 2, visited );
        } else if ( o instanceof RuleProvenance ) {
            RuleProvenance rule = (RuleProvenance) o;
            pw.println( "call#" + rule.callId + " rule [" + rule.rule + "]" );
            for ( AlgNode alg : rule.algs ) {
                provenanceRecurse( pw, alg, i + 2, visited );
            }
        } else if ( o == null && node instanceof AlgSubset ) {
            // A few operands recognize subsets, not individual algs. The first alg in the subset is deemed to have created it.
            final AlgSubset subset = (AlgSubset) node;
            pw.println( "subset " + subset );
            provenanceRecurse( pw, subset.getAlgList().get( 0 ), i + 2, visited );
        } else {
            throw new AssertionError( "bad type " + o );
        }
    }


    private void setInitialImportance() {
        AlgVisitor visitor =
                new AlgVisitor() {
                    int depth = 0;
                    final Set<AlgSubset> visitedSubsets = new HashSet<>();


                    @Override
                    public void visit( AlgNode p, int ordinal, AlgNode parent ) {
                        if ( p instanceof AlgSubset ) {
                            AlgSubset subset = (AlgSubset) p;

                            if ( visitedSubsets.contains( subset ) ) {
                                return;
                            }

                            if ( subset != root ) {
                                Double importance = Math.pow( 0.9, depth );
                                ruleQueue.updateImportance( subset, importance );
                            }

                            visitedSubsets.add( subset );

                            depth++;
                            for ( AlgNode alg : subset.getAlgs() ) {
                                visit( alg, -1, subset );
                            }
                            depth--;
                        } else {
                            super.visit( p, ordinal, parent );
                        }
                    }
                };

        visitor.go( root );
    }


    /**
     * Finds RelSubsets in the plan that contain only algs of {@link Convention#NONE} and boosts their importance by 25%.
     */
    private void injectImportanceBoost() {
        final Set<AlgSubset> requireBoost = new HashSet<>();

        SUBSET_LOOP:
        for ( AlgSubset subset : ruleQueue.subsetImportances.keySet() ) {
            for ( AlgNode alg : subset.getAlgs() ) {
                if ( alg.getConvention() != Convention.NONE ) {
                    continue SUBSET_LOOP;
                }
            }

            requireBoost.add( subset );
        }

        ruleQueue.boostImportance( requireBoost, 1.25 );
    }


    /**
     * Clear all importance boosts.
     */
    private void clearImportanceBoost() {
        Collection<AlgSubset> empty = Collections.emptySet();

        ruleQueue.boostImportance( empty, 1.0 );
    }


    @Override
    public AlgSubset register( AlgNode alg, AlgNode equivAlg ) {
        assert !isRegistered( alg ) : "pre: isRegistered(alg)";
        final AlgSet set;
        if ( equivAlg == null ) {
            set = null;
        } else {
            assert AlgOptUtil.equal(
                    "alg rowtype",
                    alg.getTupleType(),
                    "equivAlg rowtype",
                    equivAlg.getTupleType(),
                    Litmus.THROW );
            set = getSet( equivAlg );
        }
        final AlgSubset subset = registerImpl( alg, set );

        // Checking if tree is valid considerably slows down planning. Only doing it if logger level is debug or finer
        assert !LOGGER.isDebugEnabled() || isValid( Litmus.THROW );

        return subset;
    }


    @Override
    public AlgSubset ensureRegistered( AlgNode alg, AlgNode equivAlg ) {
        final AlgSubset subset = getSubset( alg );
        if ( subset != null ) {
            if ( equivAlg != null ) {
                final AlgSubset equivSubset = getSubset( equivAlg );
                if ( subset.set != equivSubset.set ) {
                    merge( equivSubset.set, subset.set );
                }
            }
            return subset;
        } else {
            return register( alg, equivAlg );
        }
    }


    /**
     * Checks internal consistency.
     */
    protected boolean isValid( Litmus litmus ) {
        for ( AlgSet set : allSets ) {
            if ( set.equivalentSet != null ) {
                return litmus.fail( "set [{}] has been merged: it should not be in the list", set );
            }
            for ( AlgSubset subset : set.subsets ) {
                if ( subset.set != set ) {
                    return litmus.fail( "subset [{}] is in wrong set [{}]", subset.getDescription(), set );
                }
                for ( AlgNode alg : subset.getAlgs() ) {
                    AlgOptCost algCost = getCost( alg, alg.getCluster().getMetadataQuery() );
                    if ( algCost.isLt( subset.bestCost ) ) {
                        return litmus.fail( "alg [{}] has lower cost {} than best cost {} of subset [{}]", alg.getDescription(), algCost, subset.bestCost, subset.getDescription() );
                    }
                }
            }
        }
        return litmus.succeed();
    }


    public void registerModelRules() {
        // Graph
        addRule( LpgToEnumerableRule.PROJECT_TO_ENUMERABLE );
        addRule( LpgToEnumerableRule.FILTER_TO_ENUMERABLE );
        addRule( LpgToEnumerableRule.AGGREGATE_TO_ENUMERABLE );
        addRule( LpgToEnumerableRule.VALUES_TO_ENUMERABLE );

        addRule( ModelSwitcherRule.MODEL_SWITCHER_RULE_GRAPH_DOC );
        addRule( ModelSwitcherRule.MODEL_SWITCHER_RULE_GRAPH_REL );
        addRule( ModelSwitcherRule.MODEL_SWITCHER_RULE_REL_DOC );
        addRule( ModelSwitcherRule.MODEL_SWITCHER_RULE_REL_GRAPH );
        addRule( ModelSwitcherRule.MODEL_SWITCHER_RULE_DOC_REL );
        addRule( ModelSwitcherRule.MODEL_SWITCHER_RULE_DOC_GRAPH );
    }


    public void registerAbstractAlgebraRules() {
        addRule( FilterJoinRule.FILTER_ON_JOIN );
        addRule( FilterJoinRule.JOIN );
        addRule( AbstractConverter.ExpandConversionRule.INSTANCE );
        addRule( JoinCommuteRule.INSTANCE );
        addRule( SemiJoinRules.PROJECT );
        addRule( SemiJoinRules.JOIN );
        if ( RuntimeConfig.JOIN_COMMUTE.getBoolean() ) {
            addRule( JoinAssociateRule.INSTANCE );
        }
        addRule( AggregateRemoveRule.INSTANCE );
        addRule( UnionToDistinctRule.INSTANCE );
        //addRule( ProjectRemoveRule.INSTANCE );
        addRule( AggregateJoinTransposeRule.INSTANCE );
        addRule( AggregateProjectMergeRule.INSTANCE );
        addRule( CalcRemoveRule.INSTANCE );
        addRule( SortRemoveRule.INSTANCE );

        // todo: rule which makes Project({OrdinalRef}) disappear
    }


    @Override
    public AlgOptCost getCost( AlgNode alg, AlgMetadataQuery mq ) {
        assert alg != null : "pre-condition: alg != null";
        if ( alg instanceof AlgSubset ) {
            return ((AlgSubset) alg).bestCost;
        }
        if ( alg.getTraitSet().getTrait( ConventionTraitDef.INSTANCE ) == Convention.NONE ) {
            return costFactory.makeInfiniteCost();
        }
        AlgOptCost cost = mq.getNonCumulativeCost( alg );
        if ( !zeroCost.isLt( cost ) ) {
            // cost must be positive, so nudge it
            cost = costFactory.makeTinyCost();
        }
        for ( AlgNode input : alg.getInputs() ) {
            cost = cost.plus( getCost( input, mq ) );
        }
        return cost;
    }


    /**
     * Returns the subset that an algebra expression belongs to.
     *
     * @param alg Algebraic expression
     * @return Subset it belongs to, or null if it is not registered
     */
    public AlgSubset getSubset( AlgNode alg ) {
        assert alg != null : "pre: alg != null";
        if ( alg instanceof AlgSubset ) {
            return (AlgSubset) alg;
        } else {
            return mapAlg2Subset.get( alg );
        }
    }


    public AlgSubset getSubset( AlgNode alg, AlgTraitSet traits ) {
        return getSubset( alg, traits, false );
    }


    public AlgSubset getSubset( AlgNode alg, AlgTraitSet traits, boolean createIfMissing ) {
        if ( (alg instanceof AlgSubset) && (alg.getTraitSet().equals( traits )) ) {
            return (AlgSubset) alg;
        }
        AlgSet set = getSet( alg );
        if ( set == null ) {
            return null;
        }
        if ( createIfMissing ) {
            return set.getOrCreateSubset( alg.getCluster(), traits );
        }
        return set.getSubset( traits );
    }


    private AlgNode changeTraitsUsingConverters( AlgNode alg, AlgTraitSet toTraits, boolean allowAbstractConverters ) {
        final AlgTraitSet fromTraits = alg.getTraitSet();

        assert fromTraits.size() >= toTraits.size();

        final boolean allowInfiniteCostConverters =
                SaffronProperties.INSTANCE.allowInfiniteCostConverters().get();

        // Traits may build on top of another...for example a collation trait would typically come after a distribution
        // trait since distribution destroys collation; so when doing the conversion below we use fromTraits as the trait
        // of the just previously converted AlgNode. Also, toTraits may have fewer traits than fromTraits, excess traits
        // will be left as is. Finally, any null entries in toTraits are ignored.
        AlgNode converted = alg;
        for ( int i = 0; (converted != null) && (i < toTraits.size()); i++ ) {
            AlgTrait<?> fromTrait = converted.getTraitSet().getTrait( i );
            final AlgTraitDef traitDef = fromTrait.getTraitDef();
            AlgTrait toTrait = toTraits.getTrait( i );

            if ( toTrait == null ) {
                continue;
            }

            assert traitDef == toTrait.getTraitDef();
//            if (fromTrait.subsumes(toTrait)) {
            if ( fromTrait.equals( toTrait ) ) {
                // No need to convert; it's already correct.
                continue;
            }

            alg = traitDef.convert( this, converted, toTrait, allowInfiniteCostConverters );
            if ( alg != null ) {
                assert alg.getTraitSet().getTrait( traitDef ).satisfies( toTrait );
                alg = completeConversion( alg, allowInfiniteCostConverters, toTraits, Expressions.list( traitDef ) );
                if ( alg != null ) {
                    register( alg, converted );
                }
            }

            if ( (alg == null) && allowAbstractConverters ) {
                AlgTraitSet stepTraits = converted.getTraitSet().replace( toTrait );
                alg = getSubset( converted, stepTraits );
            }

            converted = alg;
        }

        // make sure final converted traitset subsumes what was required
        assert converted == null || converted.getTraitSet().satisfies( toTraits );

        return converted;
    }


    /**
     * Converts traits using well-founded induction. We don't require that each conversion preserves all traits that
     * have previously been converted, but if it changes "locked in" traits we'll try some other conversion.
     *
     * @param alg Relational expression
     * @param allowInfiniteCostConverters Whether to allow infinite converters
     * @param toTraits Target trait set
     * @param usedTraits Traits that have been locked in
     * @return Converted algebra expression
     */
    private AlgNode completeConversion(
            AlgNode alg,
            boolean allowInfiniteCostConverters,
            AlgTraitSet toTraits,
            Expressions.FluentList<AlgTraitDef<?>> usedTraits ) {
        if ( true ) {
            return alg;
        }
        for ( AlgTrait<?> trait : alg.getTraitSet() ) {
            if ( toTraits.contains( trait ) ) {
                // We're already a match on this trait type.
                continue;
            }
            final AlgTraitDef traitDef = trait.getTraitDef();
            AlgNode alg2 = traitDef.convert( this, alg, toTraits.getTrait( traitDef ), allowInfiniteCostConverters );

            // if any of the used traits have been knocked out, we could be heading for a cycle.
            for ( AlgTraitDef<?> usedTrait : usedTraits ) {
                if ( !alg2.getTraitSet().contains( usedTrait ) ) {
                    continue;
                }
            }
            // recursive call, to convert one more trait
            alg = completeConversion( alg2, allowInfiniteCostConverters, toTraits, usedTraits.append( traitDef ) );
            if ( alg != null ) {
                return alg;
            }
        }
        assert alg.getTraitSet().equals( toTraits );
        return alg;
    }


    AlgNode changeTraitsUsingConverters( AlgNode alg, AlgTraitSet toTraits ) {
        return changeTraitsUsingConverters( alg, toTraits, false );
    }


    void checkForSatisfiedConverters( AlgSet set, AlgNode alg ) {
        int i = 0;
        while ( i < set.abstractConverters.size() ) {
            AbstractConverter converter = set.abstractConverters.get( i );
            AlgNode converted = changeTraitsUsingConverters( alg, converter.getTraitSet() );
            if ( converted == null ) {
                i++; // couldn't convert this; move on to the next
            } else {
                if ( !isRegistered( converted ) ) {
                    registerImpl( converted, set );
                }
                set.abstractConverters.remove( converter ); // success
            }
        }
    }


    @Override
    public void setImportance( AlgNode alg, double importance ) {
        assert alg != null;
        if ( importance == 0d ) {
            algImportances.put( alg, importance );
        }
    }


    /**
     * Dumps the internal state of this VolcanoPlanner to a writer.
     *
     * @param pw Print writer
     * @see #normalizePlan(String)
     */
    public void dump( PrintWriter pw ) {
        pw.println( "Root: " + root.getDescription() );
        pw.println( "Original alg:" );
        pw.println( originalRootString );
        pw.println( "Sets:" );
        Ordering<AlgSet> ordering = Ordering.from( Comparator.comparingInt( o -> o.id ) );
        for ( AlgSet set : ordering.immutableSortedCopy( allSets ) ) {
            pw.println( "Set#" + set.id + ", type: " + set.subsets.get( 0 ).getTupleType() );
            int j = -1;
            for ( AlgSubset subset : set.subsets ) {
                ++j;
                pw.println( "\t" + subset.getDescription() + ", best=" + ((subset.best == null)
                        ? "null"
                        : ("alg#" + subset.best.getId())) + ", importance=" + ruleQueue.getImportance( subset ) );
                assert subset.set == set;
                for ( int k = 0; k < j; k++ ) {
                    assert !set.subsets.get( k ).getTraitSet().equals( subset.getTraitSet() );
                }
                for ( AlgNode alg : subset.getAlgs() ) {
                    // "\t\trel#34:JavaProject(rel#32:JavaFilter(...), ...)"
                    pw.print( "\t\t" + alg.getDescription() );
                    for ( AlgNode input : alg.getInputs() ) {
                        AlgSubset inputSubset = getSubset( input, input.getTraitSet() );
                        AlgSet inputSet = inputSubset.set;
                        if ( input instanceof AlgSubset ) {
                            final Iterator<AlgNode> algs = inputSubset.getAlgs().iterator();
                            if ( algs.hasNext() ) {
                                input = algs.next();
                                assert input.getTraitSet().satisfies( inputSubset.getTraitSet() );
                                assert inputSet.algs.contains( input );
                                assert inputSet.subsets.contains( inputSubset );
                            }
                        }
                    }
                    Double importance = algImportances.get( alg );
                    if ( importance != null ) {
                        pw.print( ", importance=" + importance );
                    }
                    AlgMetadataQuery mq = alg.getCluster().getMetadataQuery();
                    pw.print( ", rowcount=" + mq.getTupleCount( alg ) );
                    pw.println( ", cumulative cost=" + getCost( alg, mq ) );
                }
            }
        }
        pw.println();
    }


    /**
     * Re-computes the digest of a {@link AlgNode}.
     *
     * Since an algebra expression's digest contains the identifiers of its children, this method needs to be called
     * when the child has been renamed, for example if the child's set merges with another.
     *
     * @param alg Algebra expression
     */
    void rename( AlgNode alg ) {
        final String oldDigest = alg.getDigest();
        if ( fixUpInputs( alg ) ) {
            final AlgNode removed = mapDigestToAlg.remove( oldDigest );
            assert removed == alg;
            final String newDigest = alg.recomputeDigest();
            LOGGER.trace( "Rename #{} from '{}' to '{}'", alg.getId(), oldDigest, newDigest );
            final AlgNode equivAlg = mapDigestToAlg.put( newDigest, alg );
            if ( equivAlg != null ) {
                assert equivAlg != alg;

                // There's already an equivalent with the same name, and we just knocked it out. Put it back, and forget about 'rel'.
                LOGGER.trace( "After renaming alg#{} it is now equivalent to alg#{}", alg.getId(), equivAlg.getId() );

                assert AlgOptUtil.equal(
                        "alg rowtype",
                        alg.getTupleType(),
                        "equivAlg rowtype",
                        equivAlg.getTupleType(),
                        Litmus.THROW );

                mapDigestToAlg.put( newDigest, equivAlg );

                AlgSubset equivAlgSubset = getSubset( equivAlg );
                ruleQueue.recompute( equivAlgSubset, true );

                // Remove back-links from children.
                for ( AlgNode input : alg.getInputs() ) {
                    ((AlgSubset) input).set.parents.remove( alg );
                }

                // Remove alg from its subset. (This may leave the subset empty, but if so, that will be dealt
                // with when the sets get merged.)
                final AlgSubset subset = mapAlg2Subset.put( alg, equivAlgSubset );
                assert subset != null;
                boolean existed = subset.set.algs.remove( alg );
                assert existed : "alg was not known to its set";
                final AlgSubset equivSubset = getSubset( equivAlg );
                if ( equivSubset != subset ) {
                    // The equivalent algebra expression is in a different subset, therefore the sets are equivalent.
                    assert equivSubset.getTraitSet().equals( subset.getTraitSet() );
                    assert equivSubset.set != subset.set;
                    merge( equivSubset.set, subset.set );
                }
            }
        }
    }


    /**
     * Registers a {@link AlgNode}, which has already been registered, in a new {@link AlgSet}.
     *
     * @param set Set
     * @param alg Relational expression
     */
    void reregister( AlgSet set, AlgNode alg ) {
        // Is there an equivalent algebra expression? (This might have just occurred because the algebra
        // expression's child was just found to be equivalent to another set.)
        AlgNode equivRel = mapDigestToAlg.get( alg.getDigest() );
        if ( equivRel != null && equivRel != alg ) {
            assert equivRel.getClass() == alg.getClass();
            assert equivRel.getTraitSet().equals( alg.getTraitSet() );
            assert AlgOptUtil.equal(
                    "alg rowtype",
                    alg.getTupleType(),
                    "equivAlg rowtype",
                    equivRel.getTupleType(),
                    Litmus.THROW );

            AlgSubset equivAlgSubset = getSubset( equivRel );
            ruleQueue.recompute( equivAlgSubset, true );
            return;
        }

        // Add the algebra expression into the correct set and subset.
        AlgSubset subset2 = addAlgToSet( alg, set );
    }


    /**
     * If a subset has one or more equivalent subsets (owing to a set having merged with another),
     * returns the subset which is the leader of the equivalence class.
     *
     * @param subset Subset
     * @return Leader of subset's equivalence class
     */
    private AlgSubset canonize( final AlgSubset subset ) {
        if ( subset.set.equivalentSet == null ) {
            return subset;
        }
        AlgSet set = subset.set;
        do {
            set = set.equivalentSet;
        } while ( set.equivalentSet != null );
        return set.getOrCreateSubset( subset.getCluster(), subset.getTraitSet() );
    }


    /**
     * Fires all rules matched by an algebra expression.
     *
     * @param alg Algebra expression which has just been created (or maybe from the queue)
     * @param deferred If true, each time a rule matches, just add an entry to the queue.
     */
    public void fireRules( AlgNode alg, boolean deferred ) {
        for ( AlgOptRuleOperand operand : classOperands.get( alg.getClass() ) ) {
            if ( operand.matches( alg ) ) {
                final VolcanoRuleCall ruleCall;
                if ( deferred ) {
                    ruleCall = new DeferringRuleCall( this, operand );
                } else {
                    ruleCall = new VolcanoRuleCall( this, operand );
                }
                ruleCall.match( alg );
            }
        }
    }


    @Override
    public void addRuleDuringRuntime( AlgOptRule rule ) {
        if ( getRuleByDescription( rule.toString() ) != null || !addRule( rule ) ) {
            return;
        }
        AlgOptRuleOperand operand = rule.getOperand();

        List<Pair<AlgNode, AlgSet>> matches = new ArrayList<>();
        for ( AlgSet set : allSets ) {
            for ( AlgNode node : set.getAlgsFromAllSubsets() ) {
                if ( operand.matches( node ) ) {
                    matches.add( Pair.of( node, set ) );
                }
            }

        }
        for ( Pair<AlgNode, AlgSet> pair : matches ) {
            fireRules( pair.left, true );
        }

    }


    private boolean fixUpInputs( AlgNode alg ) {
        List<AlgNode> inputs = alg.getInputs();
        int i = -1;
        int changeCount = 0;
        for ( AlgNode input : inputs ) {
            ++i;
            if ( input instanceof AlgSubset subset ) {
                AlgSubset newSubset = canonize( subset );
                if ( newSubset != subset ) {
                    alg.replaceInput( i, newSubset );
                    if ( subset.set != newSubset.set ) {
                        subset.set.parents.remove( alg );
                        newSubset.set.parents.add( alg );
                    }
                    changeCount++;
                }
            }
        }
        return changeCount > 0;
    }


    private AlgSet merge( AlgSet set, AlgSet set2 ) {
        assert set != set2 : "pre: set != set2";

        // Find the root of set2's equivalence tree.
        set = equivRoot( set );
        set2 = equivRoot( set2 );

        // Looks like set2 was already marked as equivalent to set. Nothing to do.
        if ( set2 == set ) {
            return set;
        }

        // If necessary, swap the sets, so we're always merging the newer set into the older.
        if ( set.id > set2.id ) {
            AlgSet t = set;
            set = set2;
            set2 = t;
        }

        // Merge.
        set.mergeWith( this, set2 );

        // Was the set we merged with the root? If so, the result is the new root.
        if ( set2 == getSet( root ) ) {
            root = set.getOrCreateSubset( root.getCluster(), root.getTraitSet() );
            ensureRootConverters();
        }

        return set;
    }


    private static AlgSet equivRoot( AlgSet s ) {
        AlgSet p = s; // iterates at twice the rate, to detect cycles
        while ( s.equivalentSet != null ) {
            p = forward2( s, p );
            s = s.equivalentSet;
        }
        return s;
    }


    /**
     * Moves forward two links, checking for a cycle at each.
     */
    private static AlgSet forward2( AlgSet s, AlgSet p ) {
        p = forward1( s, p );
        p = forward1( s, p );
        return p;
    }


    /**
     * Moves forward one link, checking for a cycle.
     */
    private static AlgSet forward1( AlgSet s, AlgSet p ) {
        if ( p != null ) {
            p = p.equivalentSet;
            if ( p == s ) {
                throw new AssertionError( "cycle in equivalence tree" );
            }
        }
        return p;
    }


    /**
     * Registers a new expression <code>exp</code> and queues up rule matches. If <code>set</code> is not null, makes
     * the expression part of that equivalence set. If an identical expression is already registered, we don't need to
     * register this one and nor should we queue up rule matches.
     *
     * @param alg algebra expression to register. Must be either a {@link AlgSubset}, or an unregistered {@link AlgNode}
     * @param set set that alg belongs to, or <code>null</code>
     * @return the equivalence-set
     */
    private AlgSubset registerImpl( AlgNode alg, AlgSet set ) {
        if ( alg instanceof AlgSubset ) {
            return registerSubset( set, (AlgSubset) alg );
        }

        assert !isRegistered( alg ) : "already been registered: " + alg;
        if ( alg.getCluster().getPlanner() != this ) {
            throw new AssertionError( "Relational expression " + alg + " belongs to a different planner than is currently being used." );
        }

        // Now is a good time to ensure that the algebra expression implements the interface required by its calling convention.
        final AlgTraitSet traits = alg.getTraitSet();
        final Convention convention = traits.getTrait( ConventionTraitDef.INSTANCE );
        assert convention != null;
        if ( !convention.getInterface().isInstance( alg ) && !(alg instanceof Converter) ) {
            throw new AssertionError( "Algebraic expression " + alg + " has calling-convention " + convention + " but does not implement the required interface '" + convention.getInterface() + "' of that convention" );
        }
        if ( traits.size() != traitDefs.size() ) {
            throw new AssertionError( "Algebraic expression " + alg + " does not have the correct number of traits: " + traits.size() + " != " + traitDefs.size() );
        }

        // Ensure that its sub-expressions are registered.
        alg = alg.onRegister( this );

        //log.warn( "size is: " + provenanceMap.size() );
        // Record its provenance. (Rule call may be null.)
        if ( ruleCallStack.isEmpty() ) {
            if ( LOGGER.isDebugEnabled() ) {
                provenances.put( alg, Provenance.EMPTY );
            }
        } else {
            final VolcanoRuleCall ruleCall = ruleCallStack.peek();
            if ( LOGGER.isDebugEnabled() ) {
                provenances.put( alg, new RuleProvenance( ruleCall.rule, ImmutableList.copyOf( ruleCall.algs ), ruleCall.id ) );
            }
        }

        // If it is equivalent to an existing expression, return the set that the equivalent expression belongs to.
        String key = alg.getDigest();
        AlgNode equivExp = mapDigestToAlg.get( key );
        if ( equivExp == null ) {
            // do nothing
        } else if ( equivExp == alg ) {
            return getSubset( alg );
        } else {
            assert AlgOptUtil.equal(
                    "left", equivExp.getTupleType(),
                    "right", alg.getTupleType(),
                    Litmus.THROW );
            AlgSet equivSet = getSet( equivExp );
            if ( equivSet != null ) {
                LOGGER.trace( "Register: alg#{} is equivalent to {}", alg.getId(), equivExp.getDescription() );
                return registerSubset( set, getSubset( equivExp ) );
            }
        }

        // Converters are in the same set as their children.
        if ( alg instanceof Converter ) {
            final AlgNode input = ((Converter) alg).getInput();
            final AlgSet childSet = getSet( input );
            if ( (set != null) && (set != childSet) && (set.equivalentSet == null) ) {
                LOGGER.trace( "Register #{} {} (and merge sets, because it is a conversion)", alg.getId(), alg.getDigest() );
                merge( set, childSet );
                registerCount++;

                // During the mergers, the child set may have changed, and since we're not registered yet, we won't have
                // been informed. So check whether we are now equivalent to an existing expression.
                if ( fixUpInputs( alg ) ) {
                    alg.recomputeDigest();
                    key = alg.getDigest();
                    AlgNode equivAlg = mapDigestToAlg.get( key );
                    if ( (equivAlg != alg) && (equivAlg != null) ) {
                        assert AlgOptUtil.equal(
                                "alg rowtype",
                                alg.getTupleType(),
                                "equivAlg rowtype",
                                equivAlg.getTupleType(),
                                Litmus.THROW );

                        // Make sure this bad alg didn't get into the set in any way (fixupInputs will do this but it
                        // doesn't know if it should so it does it anyway)
                        set.obliterateAlgNode( alg );

                        // There is already an equivalent expression. Use that one, and forget about this one.
                        return getSubset( equivAlg );
                    }
                }
            } else {
                set = childSet;
            }
        }

        // Place the expression in the appropriate equivalence set.
        if ( set == null ) {
            set = new AlgSet(
                    nextSetId++,
                    Util.minus( AlgOptUtil.getVariablesSet( alg ), alg.getVariablesSet() ),
                    AlgOptUtil.getVariablesUsed( alg ) );
            this.allSets.add( set );
        }

        // Chain to find 'live' equivalent set, just in case several sets are merging at the same time.
        while ( set.equivalentSet != null ) {
            set = set.equivalentSet;
        }

        // Allow each alg to register its own rules.
        registerClass( alg );

        registerCount++;
        final int subsetBeforeCount = set.subsets.size();
        AlgSubset subset = addAlgToSet( alg, set );

        final AlgNode xx = mapDigestToAlg.put( key, alg );
        assert xx == null || xx == alg : alg.getDigest();

        LOGGER.trace( "Register {} in {}", alg.getDescription(), subset.getDescription() );

        // This algebra expression may have been registered while we recursively registered its children.
        // If this is the case, we're done.
        if ( xx != null ) {
            return subset;
        }

        // Create back-links from its children, which makes children more important.
        if ( alg == this.root ) {
            ruleQueue.subsetImportances.put( subset, 1.0 ); // todo: remove
        }
        for ( AlgNode input : alg.getInputs() ) {
            AlgSubset childSubset = (AlgSubset) input;
            childSubset.set.parents.add( alg );

            // Child subset is more important now a new parent uses it.
            ruleQueue.recompute( childSubset );
        }
        if ( alg == this.root ) {
            ruleQueue.subsetImportances.remove( subset );
        }

        // Remember abstract converters until they're satisfied
        if ( alg instanceof AbstractConverter ) {
            set.abstractConverters.add( (AbstractConverter) alg );
        }

        // If this set has any unsatisfied converters, try to satisfy them.
        checkForSatisfiedConverters( set, alg );

        // Make sure this alg's subset importance is updated
        ruleQueue.recompute( subset, true );

        // Queue up all rules triggered by this algexp's creation.
        fireRules( alg, true );

        // It's a new subset.
        if ( set.subsets.size() > subsetBeforeCount ) {
            fireRules( subset, true );
        }

        return subset;
    }


    private AlgSubset addAlgToSet( AlgNode alg, AlgSet set ) {
        AlgSubset subset = set.add( alg );
        mapAlg2Subset.put( alg, subset );

        // While a tree of AlgNodes is being registered, sometimes nodes' costs improve and the subset doesn't
        // hear about it. You can end up with a subset with a single alg of cost 99 which thinks its best cost is 100.
        // We think this happens because the back-links to parents are not established.
        // So, give the subset another change to figure out its cost.
        final AlgMetadataQuery mq = alg.getCluster().getMetadataQuery();
        subset.propagateCostImprovements( this, mq, alg, new HashSet<>() );

        return subset;
    }


    private AlgSubset registerSubset( AlgSet set, AlgSubset subset ) {
        if ( (set != subset.set) && (set != null) && (set.equivalentSet == null) ) {
            LOGGER.trace( "Register #{} {}, and merge sets", subset.getId(), subset );
            merge( set, subset.set );
            registerCount++;
        }
        return subset;
    }


    // implement AlgOptPlanner
    @Override
    public void addListener( AlgOptListener newListener ) {
        // TODO jvs 6-Apr-2006:  new superclass AbstractAlgOptPlanner now defines a multicast listener; just need to hook it in
        if ( listener != null ) {
            throw Util.needToImplement( "multiple VolcanoPlanner listeners" );
        }
        listener = newListener;
    }


    // implement RelOptPlanner
    @Override
    public void registerMetadataProviders( List<AlgMetadataProvider> list ) {
        list.add( 0, new VolcanoRelMetadataProvider() );
    }


    // implement RelOptPlanner
    @Override
    public long getAlgMetadataTimestamp( AlgNode alg ) {
        AlgSubset subset = getSubset( alg );
        if ( subset == null ) {
            return 0;
        } else {
            return subset.timestamp;
        }
    }


    /**
     * Normalizes references to subsets within the string representation of a plan.
     *
     * This is useful when writing tests: it helps to ensure that tests don't break when an extra rule is introduced
     * that generates a new subset and causes subsequent subset numbers to be off by one.
     *
     * For example,
     *
     * <blockquote>
     * FennelAggRel.FENNEL_EXEC(child=Subset#17.FENNEL_EXEC,groupCount=1,
     * EXPR$1=COUNT())<br>
     * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#2.FENNEL_EXEC,
     * key=[0], discardDuplicates=false)<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
     * child=Subset#4.FENNEL_EXEC, expr#0..8={inputs}, expr#9=3456,
     * DEPTNO=$t7, $f0=$t9)<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
     * table=[CATALOG, SALES, EMP])</blockquote>
     *
     * becomes
     *
     * <blockquote>
     * FennelAggRel.FENNEL_EXEC(child=Subset#{0}.FENNEL_EXEC, groupCount=1,
     * EXPR$1=COUNT())<br>
     * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#{1}.FENNEL_EXEC,
     * key=[0], discardDuplicates=false)<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
     * child=Subset#{2}.FENNEL_EXEC,expr#0..8={inputs},expr#9=3456,DEPTNO=$t7,
     * $f0=$t9)<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
     * table=[CATALOG, SALES, EMP])</blockquote>
     *
     * @param plan Plan
     * @return Normalized plan
     */
    public static String normalizePlan( String plan ) {
        if ( plan == null ) {
            return null;
        }
        final Pattern poundDigits = Pattern.compile( "Subset#[0-9]+\\." );
        int i = 0;
        while ( true ) {
            final Matcher matcher = poundDigits.matcher( plan );
            if ( !matcher.find() ) {
                return plan;
            }
            final String token = matcher.group(); // e.g. "Subset#23."
            plan = plan.replace( token, "Subset#{" + i++ + "}." );
        }
    }


    public void ensureRegistered( AlgNode alg, AlgNode equivAlg, VolcanoRuleCall ruleCall ) {
        ruleCallStack.push( ruleCall );
        ensureRegistered( alg, equivAlg );
        ruleCallStack.pop();
    }


    /**
     * A rule call which defers its actions. Whereas {@link AlgOptRuleCall} invokes the rule when it finds a match,
     * a <code>DeferringRuleCall</code> creates a {@link VolcanoRuleMatch} which can be invoked later.
     */
    private static class DeferringRuleCall extends VolcanoRuleCall {

        DeferringRuleCall( VolcanoPlanner planner, AlgOptRuleOperand operand ) {
            super( planner, operand );
        }


        /**
         * Rather than invoking the rule (as the base method does), creates a {@link VolcanoRuleMatch}
         * which can be invoked later.
         */
        @Override
        protected void onMatch() {
            final VolcanoRuleMatch match = new VolcanoRuleMatch( volcanoPlanner, getOperand0(), algs, nodeInputs );
            volcanoPlanner.ruleQueue.addMatch( match );
        }

    }


    /**
     * Where a {@link AlgNode} came from.
     */
    private abstract static class Provenance {

        public static final Provenance EMPTY = new UnknownProvenance();

    }


    /**
     * We do not know where this {@link AlgNode} came from. Probably created by hand, or by sql-to-alg converter.
     */
    private static class UnknownProvenance extends Provenance {

    }


    /**
     * A {@link AlgNode} that came directly from another {@link AlgNode} via a copy.
     */
    static class DirectProvenance extends Provenance {

        final AlgNode source;


        DirectProvenance( AlgNode source ) {
            this.source = source;
        }

    }


    /**
     * A {@link AlgNode} that came via the firing of a rule.
     */
    static class RuleProvenance extends Provenance {

        final AlgOptRule rule;
        final ImmutableList<AlgNode> algs;
        final int callId;


        RuleProvenance( AlgOptRule rule, ImmutableList<AlgNode> algs, int callId ) {
            this.rule = rule;
            this.algs = algs;
            this.callId = callId;
        }

    }

}
