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

package org.polypheny.db.plan.hep;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.Converter;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.convert.TraitMatchingRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AbstractAlgPlanner;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptCostFactory;
import org.polypheny.db.plan.AlgOptCostImpl;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.CommonRelSubExprRule;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.volcano.VolcanoPlannerPhase;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.graph.BreadthFirstIterator;
import org.polypheny.db.util.graph.CycleDetector;
import org.polypheny.db.util.graph.DefaultDirectedGraph;
import org.polypheny.db.util.graph.DefaultEdge;
import org.polypheny.db.util.graph.DepthFirstIterator;
import org.polypheny.db.util.graph.DirectedGraph;
import org.polypheny.db.util.graph.Graphs;
import org.polypheny.db.util.graph.TopologicalOrderIterator;


/**
 * HepPlanner is a heuristic implementation of the {@link AlgPlanner} interface.
 */
@Slf4j
public class HepPlanner extends AbstractAlgPlanner {

    private final HepProgram mainProgram;

    private HepProgram currentProgram;

    private HepAlgVertex root;

    private AlgTraitSet requestedRootTraits;

    private final Map<String, HepAlgVertex> mapDigestToVertex = new HashMap<>();

    // NOTE jvs 24-Apr-2006:  We use LinkedHashSet in order to provide deterministic behavior.
    private final Set<AlgOptRule> allRules = new LinkedHashSet<>();

    private int nTransformations;

    private int graphSizeLastGC;

    private int nTransformationsLastGC;

    private final boolean noDag;

    /**
     * Query graph, with edges directed from parent to child. This is a single-rooted DAG, possibly with additional roots corresponding to discarded plan fragments which remain to be garbage-collected.
     */
    private final DirectedGraph<HepAlgVertex, DefaultEdge> graph = DefaultDirectedGraph.create();

    private final Function2<AlgNode, AlgNode, Void> onCopyHook;


    /**
     * Creates a new HepPlanner that allows DAG.
     *
     * @param program program controlling rule application
     */
    public HepPlanner( HepProgram program ) {
        this( program, null, false, null, AlgOptCostImpl.FACTORY );
    }


    /**
     * Creates a new HepPlanner that allows DAG.
     *
     * @param program program controlling rule application
     * @param context to carry while planning
     */
    public HepPlanner( HepProgram program, Context context ) {
        this( program, context, false, null, AlgOptCostImpl.FACTORY );
    }


    /**
     * Creates a new HepPlanner with the option to keep the graph a tree (noDag = true) or allow DAG (noDag = false).
     *
     * @param noDag If false, create shared nodes if expressions are identical
     * @param program Program controlling rule application
     * @param onCopyHook Function to call when a node is copied
     */
    public HepPlanner( HepProgram program, Context context, boolean noDag, Function2<AlgNode, AlgNode, Void> onCopyHook, AlgOptCostFactory costFactory ) {
        super( costFactory, context );
        this.mainProgram = program;
        this.onCopyHook = Util.first( onCopyHook, Functions.ignore2() );
        this.noDag = noDag;
    }


    // implement AlgPlanner
    @Override
    public void setRoot( AlgNode alg ) {
        root = addAlgToGraph( alg );
        dumpGraph();
    }


    // implement AlgPlanner
    @Override
    public AlgNode getRoot() {
        return root;
    }


    @Override
    public List<AlgOptRule> getRules() {
        return ImmutableList.copyOf( allRules );
    }


    // implement AlgPlanner
    @Override
    public boolean addRule( AlgOptRule rule ) {
        boolean added = allRules.add( rule );
        if ( added ) {
            mapRuleDescription( rule );
        }
        return added;
    }


    @Override
    public boolean addRule( AlgOptRule rule, VolcanoPlannerPhase phase ) {
        return addRule( rule );
    }


    @Override
    public void clear() {
        super.clear();
        for ( AlgOptRule rule : ImmutableList.copyOf( allRules ) ) {
            removeRule( rule );
        }
    }


    @Override
    public boolean removeRule( AlgOptRule rule ) {
        unmapRuleDescription( rule );
        return allRules.remove( rule );
    }


    // implement AlgPlanner
    @Override
    public AlgNode changeTraits( AlgNode alg, AlgTraitSet toTraits ) {
        // Ignore traits, except for the root, where we remember what the final conversion should be.
        if ( (alg == root) || (alg == root.getCurrentAlg()) ) {
            requestedRootTraits = toTraits;
        }
        return alg;
    }


    // implement AlgPlanner
    @Override
    public AlgNode findBestExp() {
        assert root != null;

        executeProgram( mainProgram );

        // Get rid of everything except what's in the final plan.
        collectGarbage();

        return buildFinalPlan( root );
    }


    private void executeProgram( HepProgram program ) {
        HepProgram savedProgram = currentProgram;
        currentProgram = program;
        currentProgram.initialize( program == mainProgram );
        for ( HepInstruction instruction : currentProgram.instructions ) {
            instruction.execute( this );
            int delta = nTransformations - nTransformationsLastGC;
            if ( delta > graphSizeLastGC ) {
                // The number of transformations performed since the last garbage collection is greater than the number of vertices in the graph at that time.  That means there should be a reasonable amount of garbage to collect now.
                // We do it this way to amortize garbage collection cost over multiple instructions, while keeping the highwater memory usage proportional to the graph size.
                collectGarbage();
            }
        }
        currentProgram = savedProgram;
    }


    void executeInstruction( HepInstruction.MatchLimit instruction ) {
        LOGGER.trace( "Setting match limit to {}", instruction.limit );
        currentProgram.matchLimit = instruction.limit;
    }


    void executeInstruction( HepInstruction.MatchOrder instruction ) {
        LOGGER.trace( "Setting match order to {}", instruction.order );
        currentProgram.matchOrder = instruction.order;
    }


    void executeInstruction( HepInstruction.RuleInstance instruction ) {
        if ( skippingGroup() ) {
            return;
        }
        if ( instruction.rule == null ) {
            assert instruction.ruleDescription != null;
            instruction.rule = getRuleByDescription( instruction.ruleDescription );
            LOGGER.trace( "Looking up rule with description {}, found {}", instruction.ruleDescription, instruction.rule );
        }
        if ( instruction.rule != null ) {
            applyRules( Collections.singleton( instruction.rule ), true );
        }
    }


    void executeInstruction( HepInstruction.RuleClass<?> instruction ) {
        if ( skippingGroup() ) {
            return;
        }
        LOGGER.trace( "Applying rule class {}", instruction.ruleClass );
        if ( instruction.ruleSet == null ) {
            instruction.ruleSet = new LinkedHashSet<>();
            for ( AlgOptRule rule : allRules ) {
                if ( instruction.ruleClass.isInstance( rule ) ) {
                    instruction.ruleSet.add( rule );
                }
            }
        }
        applyRules( instruction.ruleSet, true );
    }


    void executeInstruction( HepInstruction.RuleCollection instruction ) {
        if ( skippingGroup() ) {
            return;
        }
        applyRules( instruction.rules, true );
    }


    private boolean skippingGroup() {
        if ( currentProgram.group != null ) {
            // Skip if we've already collected the ruleset.
            return !currentProgram.group.collecting;
        } else {
            // Not grouping.
            return false;
        }
    }


    void executeInstruction( HepInstruction.ConverterRules instruction ) {
        assert currentProgram.group == null;
        if ( instruction.ruleSet == null ) {
            instruction.ruleSet = new LinkedHashSet<>();
            for ( AlgOptRule rule : allRules ) {
                if ( !(rule instanceof ConverterRule) ) {
                    continue;
                }
                ConverterRule converter = (ConverterRule) rule;
                if ( converter.isGuaranteed() != instruction.guaranteed ) {
                    continue;
                }

                // Add the rule itself to work top-down
                instruction.ruleSet.add( converter );
                if ( !instruction.guaranteed ) {
                    // Add a TraitMatchingRule to work bottom-up
                    instruction.ruleSet.add( new TraitMatchingRule( converter, AlgFactories.LOGICAL_BUILDER ) );
                }
            }
        }
        applyRules( instruction.ruleSet, instruction.guaranteed );
    }


    void executeInstruction( HepInstruction.CommonRelSubExprRules instruction ) {
        assert currentProgram.group == null;
        if ( instruction.ruleSet == null ) {
            instruction.ruleSet = new LinkedHashSet<>();
            for ( AlgOptRule rule : allRules ) {
                if ( !(rule instanceof CommonRelSubExprRule) ) {
                    continue;
                }
                instruction.ruleSet.add( rule );
            }
        }
        applyRules( instruction.ruleSet, true );
    }


    void executeInstruction( HepInstruction.Subprogram instruction ) {
        LOGGER.trace( "Entering subprogram" );
        for ( ; ; ) {
            int nTransformationsBefore = nTransformations;
            executeProgram( instruction.subprogram );
            if ( nTransformations == nTransformationsBefore ) {
                // Nothing happened this time around.
                break;
            }
        }
        LOGGER.trace( "Leaving subprogram" );
    }


    void executeInstruction( HepInstruction.BeginGroup instruction ) {
        assert currentProgram.group == null;
        currentProgram.group = instruction.endGroup;
        LOGGER.trace( "Entering group" );
    }


    void executeInstruction( HepInstruction.EndGroup instruction ) {
        assert currentProgram.group == instruction;
        currentProgram.group = null;
        instruction.collecting = false;
        applyRules( instruction.ruleSet, true );
        LOGGER.trace( "Leaving group" );
    }


    private int depthFirstApply( Iterator<HepAlgVertex> iter, Collection<AlgOptRule> rules, boolean forceConversions, int nMatches ) {
        while ( iter.hasNext() ) {
            HepAlgVertex vertex = iter.next();
            for ( AlgOptRule rule : rules ) {
                HepAlgVertex newVertex = applyRule( rule, vertex, forceConversions );
                if ( newVertex == null || newVertex == vertex ) {
                    continue;
                }
                ++nMatches;
                if ( nMatches >= currentProgram.matchLimit ) {
                    return nMatches;
                }
                // To the extent possible, pick up where we left off; have to create a new iterator because old one was invalidated by transformation.
                Iterator<HepAlgVertex> depthIter = getGraphIterator( newVertex );
                nMatches = depthFirstApply( depthIter, rules, forceConversions, nMatches );
                break;
            }
        }
        return nMatches;
    }


    private void applyRules( Collection<AlgOptRule> rules, boolean forceConversions ) {
        if ( currentProgram.group != null ) {
            assert currentProgram.group.collecting;
            currentProgram.group.ruleSet.addAll( rules );
            return;
        }

        LOGGER.trace( "Applying rule set {}", rules );

        boolean fullRestartAfterTransformation = currentProgram.matchOrder != HepMatchOrder.ARBITRARY && currentProgram.matchOrder != HepMatchOrder.DEPTH_FIRST;

        int nMatches = 0;

        boolean fixedPoint;
        do {
            Iterator<HepAlgVertex> iter = getGraphIterator( root );
            fixedPoint = true;
            while ( iter.hasNext() ) {
                HepAlgVertex vertex = iter.next();
                for ( AlgOptRule rule : rules ) {
                    HepAlgVertex newVertex = applyRule( rule, vertex, forceConversions );
                    if ( newVertex == null || newVertex == vertex ) {
                        continue;
                    }
                    ++nMatches;
                    if ( nMatches >= currentProgram.matchLimit ) {
                        return;
                    }
                    if ( fullRestartAfterTransformation ) {
                        iter = getGraphIterator( root );
                    } else {
                        // To the extent possible, pick up where we left off; have to create a new iterator because old one was invalidated by transformation.
                        iter = getGraphIterator( newVertex );
                        if ( currentProgram.matchOrder == HepMatchOrder.DEPTH_FIRST ) {
                            nMatches = depthFirstApply( iter, rules, forceConversions, nMatches );
                            if ( nMatches >= currentProgram.matchLimit ) {
                                return;
                            }
                        }
                        // Remember to go around again since we're skipping some stuff.
                        fixedPoint = false;
                    }
                    break;
                }
            }
        } while ( !fixedPoint );
    }


    private Iterator<HepAlgVertex> getGraphIterator( HepAlgVertex start ) {
        // Make sure there's no garbage, because topological sort doesn't start from a specific root, and rules can't deal with firing on garbage.

        // FIXME jvs 25-Sept-2006: I had to move this earlier because of FRG-215, which is still under investigation. Once we figure that one out, move down to location below for better optimizer performance.
        collectGarbage();

        switch ( currentProgram.matchOrder ) {
            case ARBITRARY:
            case DEPTH_FIRST:
                return DepthFirstIterator.of( graph, start ).iterator();

            case TOP_DOWN:
                assert start == root;
                // see above
/*
        collectGarbage();
*/
                return TopologicalOrderIterator.of( graph ).iterator();

            case BOTTOM_UP:
            default:
                assert start == root;

                // see above
/*
        collectGarbage();
*/

                // TODO jvs 4-Apr-2006:  enhance TopologicalOrderIterator to support reverse walk.
                final List<HepAlgVertex> list = new ArrayList<>();
                for ( HepAlgVertex vertex : TopologicalOrderIterator.of( graph ) ) {
                    list.add( vertex );
                }
                Collections.reverse( list );
                return list.iterator();
        }
    }


    /**
     * Returns whether the vertex is valid.
     */
    private boolean belongsToDag( HepAlgVertex vertex ) {
        String digest = vertex.getCurrentAlg().getDigest();
        return mapDigestToVertex.get( digest ) != null;
    }


    private HepAlgVertex applyRule( AlgOptRule rule, HepAlgVertex vertex, boolean forceConversions ) {
        if ( !belongsToDag( vertex ) ) {
            return null;
        }
        AlgTrait parentTrait = null;
        List<AlgNode> parents = null;
        if ( rule instanceof ConverterRule ) {
            // Guaranteed converter rules require special casing to make sure they only fire where actually needed, otherwise they tend to fire to infinity and beyond.
            ConverterRule converterRule = (ConverterRule) rule;
            if ( converterRule.isGuaranteed() || !forceConversions ) {
                if ( !doesConverterApply( converterRule, vertex ) ) {
                    return null;
                }
                parentTrait = converterRule.getOutTrait();
            }
        } else if ( rule instanceof CommonRelSubExprRule ) {
            // Only fire CommonRelSubExprRules if the vertex is a common subexpression.
            List<HepAlgVertex> parentVertices = getVertexParents( vertex );
            if ( parentVertices.size() < 2 ) {
                return null;
            }
            parents = new ArrayList<>();
            for ( HepAlgVertex pVertex : parentVertices ) {
                parents.add( pVertex.getCurrentAlg() );
            }
        }

        final List<AlgNode> bindings = new ArrayList<>();
        final Map<AlgNode, List<AlgNode>> nodeChildren = new HashMap<>();
        boolean match = matchOperands( rule.getOperand(), vertex.getCurrentAlg(), bindings, nodeChildren );

        if ( !match ) {
            return null;
        }

        HepRuleCall call = new HepRuleCall( this, rule.getOperand(), bindings.toArray( new AlgNode[0] ), nodeChildren, parents );

        // Allow the rule to apply its own side-conditions.
        if ( !rule.matches( call ) ) {
            return null;
        }

        fireRule( call );

        if ( !call.getResults().isEmpty() ) {
            return applyTransformationResults( vertex, call, parentTrait );
        }

        return null;
    }


    private boolean doesConverterApply( ConverterRule converterRule, HepAlgVertex vertex ) {
        AlgTrait outTrait = converterRule.getOutTrait();
        List<HepAlgVertex> parents = Graphs.predecessorListOf( graph, vertex );
        for ( HepAlgVertex parent : parents ) {
            AlgNode parentAlg = parent.getCurrentAlg();
            if ( parentAlg instanceof Converter ) {
                // We don't support converter chains.
                continue;
            }
            if ( parentAlg.getTraitSet().contains( outTrait ) ) {
                // This parent wants the traits produced by the converter.
                return true;
            }
        }
        return (vertex == root)
                && (requestedRootTraits != null)
                && requestedRootTraits.contains( outTrait );
    }


    /**
     * Retrieves the parent vertices of a vertex.  If a vertex appears multiple times as an input into a parent, then that counts as multiple parents, one per input reference.
     *
     * @param vertex the vertex
     * @return the list of parents for the vertex
     */
    private List<HepAlgVertex> getVertexParents( HepAlgVertex vertex ) {
        final List<HepAlgVertex> parents = new ArrayList<>();
        final List<HepAlgVertex> parentVertices = Graphs.predecessorListOf( graph, vertex );

        for ( HepAlgVertex pVertex : parentVertices ) {
            AlgNode parent = pVertex.getCurrentAlg();
            for ( int i = 0; i < parent.getInputs().size(); i++ ) {
                HepAlgVertex child = (HepAlgVertex) parent.getInputs().get( i );
                if ( child == vertex ) {
                    parents.add( pVertex );
                }
            }
        }
        return parents;
    }


    private boolean matchOperands( AlgOptRuleOperand operand, AlgNode alg, List<AlgNode> bindings, Map<AlgNode, List<AlgNode>> nodeChildren ) {
        if ( !operand.matches( alg ) ) {
            return false;
        }
        bindings.add( alg );
        @SuppressWarnings("unchecked")
        List<HepAlgVertex> childAlgs = (List) alg.getInputs();
        switch ( operand.childPolicy ) {
            case ANY:
                return true;
            case UNORDERED:
                // For each operand, at least one child must match. If matchAnyChildren, usually there's just one operand.
                for ( AlgOptRuleOperand childOperand : operand.getChildOperands() ) {
                    boolean match = false;
                    for ( HepAlgVertex childAlg : childAlgs ) {
                        match = matchOperands( childOperand, childAlg.getCurrentAlg(), bindings, nodeChildren );
                        if ( match ) {
                            break;
                        }
                    }
                    if ( !match ) {
                        return false;
                    }
                }
                final List<AlgNode> children = new ArrayList<>( childAlgs.size() );
                for ( HepAlgVertex childAlg : childAlgs ) {
                    children.add( childAlg.getCurrentAlg() );
                }
                nodeChildren.put( alg, children );
                return true;
            default:
                int n = operand.getChildOperands().size();
                if ( childAlgs.size() < n ) {
                    return false;
                }
                for ( Pair<HepAlgVertex, AlgOptRuleOperand> pair : Pair.zip( childAlgs, operand.getChildOperands() ) ) {
                    boolean match = matchOperands( pair.right, pair.left.getCurrentAlg(), bindings, nodeChildren );
                    if ( !match ) {
                        return false;
                    }
                }
                return true;
        }
    }


    private HepAlgVertex applyTransformationResults( HepAlgVertex vertex, HepRuleCall call, AlgTrait<?> parentTrait ) {
        // TODO jvs 5-Apr-2006:  Take the one that gives the best global cost rather than the best local cost.  That requires "tentative" graph edits.

        assert !call.getResults().isEmpty();

        AlgNode bestAlg = null;

        if ( call.getResults().size() == 1 ) {
            // No costing required; skip it to minimize the chance of hitting Algs without cost information.
            bestAlg = call.getResults().get( 0 );
        } else {
            AlgOptCost bestCost = null;
            final AlgMetadataQuery mq = call.getMetadataQuery();
            for ( AlgNode alg : call.getResults() ) {
                AlgOptCost thisCost = getCost( alg, mq );
                if ( LOGGER.isTraceEnabled() ) {
                    // Keep in the isTraceEnabled for the getTupleCount method call
                    LOGGER.trace( "considering {} with cumulative cost={} and rowcount={}", alg, thisCost, mq.getTupleCount( alg ) );
                }
                if ( (bestAlg == null) || thisCost.isLt( bestCost ) ) {
                    bestAlg = alg;
                    bestCost = thisCost;
                }
            }
        }

        ++nTransformations;
        notifyTransformation( call, bestAlg, true );

        // Before we add the result, make a copy of the list of vertex's parents.  We'll need this later during contraction so that we only update the existing parents, not the new parents (otherwise loops can result).
        // Also take care of filtering out parents by traits in case we're dealing with a converter rule.
        final List<HepAlgVertex> allParents = Graphs.predecessorListOf( graph, vertex );
        final List<HepAlgVertex> parents = new ArrayList<>();
        for ( HepAlgVertex parent : allParents ) {
            if ( parentTrait != null ) {
                AlgNode parentAlg = parent.getCurrentAlg();
                if ( parentAlg instanceof Converter ) {
                    // We don't support automatically chaining conversions. Treating a converter as a candidate parent here can cause the "iParentMatch" check below to
                    // throw away a new converter needed in the multi-parent DAG case.
                    continue;
                }
                if ( !parentAlg.getTraitSet().contains( parentTrait ) ) {
                    // This parent does not want the converted result.
                    continue;
                }
            }
            parents.add( parent );
        }

        HepAlgVertex newVertex = addAlgToGraph( bestAlg );

        // There's a chance that newVertex is the same as one of the parents due to common subexpression recognition (e.g. the LogicalProject added by JoinCommuteRule).  In that
        // case, treat the transformation as a nop to avoid creating a loop.
        int iParentMatch = parents.indexOf( newVertex );
        if ( iParentMatch != -1 ) {
            newVertex = parents.get( iParentMatch );
        } else {
            contractVertices( newVertex, vertex, parents );
        }
        if ( getListener() != null ) {
            // Assume listener doesn't want to see garbage.
            collectGarbage();
        }
        notifyTransformation( call, bestAlg, false );
        dumpGraph();
        return newVertex;
    }


    // implement AlgPlanner
    @Override
    public AlgNode register( AlgNode alg, AlgNode equivAlg ) {
        // Ignore; this call is mostly to tell Volcano how to avoid infinite loops.
        return alg;
    }


    @Override
    public void onCopy( AlgNode alg, AlgNode newAlg ) {
        onCopyHook.apply( alg, newAlg );
    }


    // implement AlgPlanner
    @Override
    public AlgNode ensureRegistered( AlgNode alg, AlgNode equivAlg ) {
        return alg;
    }


    // implement AlgPlanner
    @Override
    public boolean isRegistered( AlgNode alg ) {
        return true;
    }


    private HepAlgVertex addAlgToGraph( AlgNode alg ) {
        // Check if a transformation already produced a reference to an existing vertex.
        if ( graph.vertexSet().contains( alg ) ) {
            return (HepAlgVertex) alg;
        }

        // Recursively add children, replacing this algs inputs with corresponding child vertices.
        if ( alg == null ) {
            log.warn( "alg is null" );
        }
        final List<AlgNode> inputs = alg.getInputs();
        final List<AlgNode> newInputs = new ArrayList<>();
        for ( AlgNode input1 : inputs ) {
            HepAlgVertex childVertex = addAlgToGraph( input1 );
            newInputs.add( childVertex );
        }

        if ( !Util.equalShallow( inputs, newInputs ) ) {
            AlgNode oldAlg = alg;
            alg = alg.copy( alg.getTraitSet(), newInputs );
            onCopy( oldAlg, alg );
        }
        // Compute digest first time we add to DAG, otherwise can't get equivVertex for common sub-expression
        alg.recomputeDigest();

        // try to find equivalent alg only if DAG is allowed
        if ( !noDag ) {
            // Now, check if an equivalent vertex already exists in graph.
            String digest = alg.getDigest();
            HepAlgVertex equivVertex = mapDigestToVertex.get( digest );
            if ( equivVertex != null ) {
                // Use existing vertex.
                return equivVertex;
            }
        }

        // No equivalence:  create a new vertex to represent this alg.
        HepAlgVertex newVertex = new HepAlgVertex( alg );
        graph.addVertex( newVertex );
        updateVertex( newVertex, alg );

        for ( AlgNode input : alg.getInputs() ) {
            graph.addEdge( newVertex, (HepAlgVertex) input );
        }

        nTransformations++;
        return newVertex;
    }


    private void contractVertices( HepAlgVertex preservedVertex, HepAlgVertex discardedVertex, List<HepAlgVertex> parents ) {
        if ( preservedVertex == discardedVertex ) {
            // Nop.
            return;
        }

        AlgNode alg = preservedVertex.getCurrentAlg();
        updateVertex( preservedVertex, alg );

        // Update specified parents of discardedVertex.
        for ( HepAlgVertex parent : parents ) {
            AlgNode parentAlg = parent.getCurrentAlg();
            List<AlgNode> inputs = parentAlg.getInputs();
            for ( int i = 0; i < inputs.size(); ++i ) {
                AlgNode child = inputs.get( i );
                if ( child != discardedVertex ) {
                    continue;
                }
                parentAlg.replaceInput( i, preservedVertex );
            }
            graph.removeEdge( parent, discardedVertex );
            graph.addEdge( parent, preservedVertex );
            updateVertex( parent, parentAlg );
        }

        // NOTE:  we don't actually do graph.removeVertex(discardedVertex), because it might still be reachable from preservedVertex. Leave that job for garbage collection.
        if ( discardedVertex == root ) {
            root = preservedVertex;
        }
    }


    private void updateVertex( HepAlgVertex vertex, AlgNode alg ) {
        if ( alg != vertex.getCurrentAlg() ) {
            // REVIEW jvs: We'll do this again later during garbage collection.  Or we could get rid of mark/sweep garbage collection and do it precisely
            // at this point by walking down to all Algs which are only reachable from here.
            notifyDiscard( vertex.getCurrentAlg() );
        }
        String oldDigest = vertex.getCurrentAlg().toString();
        if ( mapDigestToVertex.get( oldDigest ) == vertex ) {
            mapDigestToVertex.remove( oldDigest );
        }
        String newDigest = alg.getDigest();
        // When a transformation happened in one rule apply, support vertex2 replace vertex1, but the current algNode of vertex1 and vertex2 is same, then the digest is also same. but we can't remove vertex2,
        // otherwise the digest will be removed wrongly in the mapDigestToVertex  when collectGC so it must update the digest that map to vertex
        mapDigestToVertex.put( newDigest, vertex );
        if ( alg != vertex.getCurrentAlg() ) {
            vertex.replaceAlg( alg );
        }
        notifyEquivalence( alg, vertex, false );
    }


    private AlgNode buildFinalPlan( HepAlgVertex vertex ) {
        AlgNode alg = vertex.getCurrentAlg();

        notifyChosen( alg );

        // Recursively process children, replacing this Alg's inputs with corresponding child Algs.
        List<AlgNode> inputs = alg.getInputs();
        for ( int i = 0; i < inputs.size(); ++i ) {
            AlgNode child = inputs.get( i );
            if ( !(child instanceof HepAlgVertex) ) {
                // Already replaced.
                continue;
            }
            child = buildFinalPlan( (HepAlgVertex) child );
            alg.replaceInput( i, child );
            alg.recomputeDigest();
        }

        return alg;
    }


    private void collectGarbage() {
        if ( nTransformations == nTransformationsLastGC ) {
            // No modifications have taken place since the last gc, so there can't be any garbage.
            return;
        }
        nTransformationsLastGC = nTransformations;

        LOGGER.trace( "collecting garbage" );

        // Yer basic mark-and-sweep.
        final Set<HepAlgVertex> rootSet = new HashSet<>();
        if ( graph.vertexSet().contains( root ) ) {
            BreadthFirstIterator.reachable( rootSet, graph, root );
        }

        if ( rootSet.size() == graph.vertexSet().size() ) {
            // Everything is reachable:  no garbage to collect.
            return;
        }
        final Set<HepAlgVertex> sweepSet = new HashSet<>();
        for ( HepAlgVertex vertex : graph.vertexSet() ) {
            if ( !rootSet.contains( vertex ) ) {
                sweepSet.add( vertex );
                AlgNode alg = vertex.getCurrentAlg();
                notifyDiscard( alg );
            }
        }
        assert !sweepSet.isEmpty();
        graph.removeAllVertices( sweepSet );
        graphSizeLastGC = graph.vertexSet().size();

        // Clean up digest map too.
        Iterator<Map.Entry<String, HepAlgVertex>> digestIter = mapDigestToVertex.entrySet().iterator();
        while ( digestIter.hasNext() ) {
            HepAlgVertex vertex = digestIter.next().getValue();
            if ( sweepSet.contains( vertex ) ) {
                digestIter.remove();
            }
        }
    }


    private void assertNoCycles() {
        // Verify that the graph is acyclic.
        final CycleDetector<HepAlgVertex, DefaultEdge> cycleDetector = new CycleDetector<>( graph );
        Set<HepAlgVertex> cyclicVertices = cycleDetector.findCycles();
        if ( cyclicVertices.isEmpty() ) {
            return;
        }
        throw new AssertionError( "Query graph cycle detected in HepPlanner: " + cyclicVertices );
    }


    private void dumpGraph() {
        if ( !LOGGER.isTraceEnabled() ) {
            return;
        }

        assertNoCycles();

        final AlgMetadataQuery mq = root.getCluster().getMetadataQuery();
        final StringBuilder sb = new StringBuilder();
        sb.append( "\nBreadth-first from root:  {\n" );
        for ( HepAlgVertex vertex : BreadthFirstIterator.of( graph, root ) ) {
            sb.append( "    " )
                    .append( vertex )
                    .append( " = " );
            AlgNode alg = vertex.getCurrentAlg();
            sb.append( alg )
                    .append( ", rowcount=" )
                    .append( mq.getTupleCount( alg ) )
                    .append( ", cumulative cost=" )
                    .append( getCost( alg, mq ) )
                    .append( '\n' );
        }
        sb.append( "}" );
        LOGGER.trace( sb.toString() );
    }


    // implement AlgPlanner
    @Override
    public void registerMetadataProviders( List<AlgMetadataProvider> list ) {
        list.add( 0, new HepAlgMetadataProvider() );
    }


    // implement AlgPlanner
    @Override
    public long getAlgMetadataTimestamp( AlgNode alg ) {
        // TODO jvs 20-Apr-2006: This is overly conservative.  Better would be to keep a timestamp per HepAlgVertex, and update only affected vertices and all ancestors on each transformation.
        return nTransformations;
    }

}

