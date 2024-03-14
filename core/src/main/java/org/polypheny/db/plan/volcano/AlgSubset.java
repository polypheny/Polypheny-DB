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


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptListener.AlgChosenEvent;
import org.polypheny.db.plan.AlgOptListener.AlgEquivalenceEvent;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Subset of an equivalence class where all algebra expressions have the same physical properties.
 * <p>
 * Physical properties are instances of the {@link AlgTraitSet}, and consist of traits such as calling convention and
 * collation (sort-order).
 * <p>
 * For some traits, a algebra expression can have more than one instance. For example, R can be sorted on both [X]
 * and [Y, Z]. In which case, R would belong to the sub-sets for [X] and [Y, Z]; and also the leading edges [Y] and [].
 *
 * @see AlgNode
 * @see AlgSet
 * @see AlgTrait
 */
public class AlgSubset extends AbstractAlgNode {

    private static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();


    /**
     * cost of best known plan (it may have improved since)
     */
    AlgOptCost bestCost;

    /**
     * The set this subset belongs to.
     */
    @Getter
    final AlgSet set;

    /**
     * best known plan
     */
    @Getter
    AlgNode best;

    /**
     * Timestamp for metadata validity
     */
    long timestamp;

    /**
     * Flag indicating whether this RelSubset's importance was artificially boosted.
     */
    boolean boosted;


    AlgSubset( AlgCluster cluster, AlgSet set, AlgTraitSet traits ) {
        super( cluster, traits );
        this.set = set;
        this.boosted = false;
        assert traits.allSimple();
        computeBestCost( cluster.getPlanner() );
        recomputeDigest();
    }


    /**
     * Computes the best {@link AlgNode} in this subset.
     * <p>
     * Only necessary when a subset is created in a set that has subsets that subsume it. Rationale:
     *
     * <ol>
     * <li>If the are no subsuming subsets, the subset is initially empty.</li>
     * <li>After creation, {@code best} and {@code bestCost} are maintained incrementally by {@link #propagateCostImprovements0} and {@link AlgSet#mergeWith(VolcanoPlanner, AlgSet)}.</li>
     * </ol>
     */
    private void computeBestCost( AlgPlanner planner ) {
        bestCost = planner.getCostFactory().makeInfiniteCost();
        final AlgMetadataQuery mq = getCluster().getMetadataQuery();
        for ( AlgNode alg : getAlgs() ) {
            final AlgOptCost cost = planner.getCost( alg, mq );
            if ( cost.isLt( bestCost ) ) {
                bestCost = cost;
                best = alg;
            }
        }
    }


    @Override
    public boolean containsJoin() {
        return set.alg.containsJoin();
    }


    @Override
    public boolean containsScan() {
        return set.alg.containsScan();
    }


    public AlgNode getOriginal() {
        return set.alg;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        if ( inputs.isEmpty() ) {
            final AlgTraitSet traitSet1 = traitSet.simplify();
            if ( traitSet1.equals( this.traitSet ) ) {
                return this;
            }
            return set.getOrCreateSubset( getCluster(), traitSet1 );
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public String algCompareString() {
        // Compare makes no sense here. Use hashCode() to avoid errors.
        return this.getClass().getSimpleName() + "$" + hashCode() + "&";
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return planner.getCostFactory().makeZeroCost();
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        if ( best != null ) {
            return mq.getTupleCount( best );
        } else {
            return mq.getTupleCount( set.alg );
        }
    }


    @Override
    public void explain( AlgWriter pw ) {
        // Not a typical implementation of "explain". We don't gather terms & values to be printed later. We actually do the work.
        String s = getDescription();
        pw.item( "subset", s );
        final AbstractAlgNode input = (AbstractAlgNode) Util.first( getBest(), getOriginal() );
        if ( input == null ) {
            return;
        }
        input.explainTerms( pw );
        pw.done( input );
    }


    @Override
    protected String computeDigest() {
        StringBuilder digest = new StringBuilder( "Subset#" );
        digest.append( set.id );
        for ( AlgTrait<?> trait : traitSet ) {
            digest.append( '.' ).append( trait );
        }
        return digest.toString();
    }


    @Override
    protected AlgDataType deriveRowType() {
        return set.alg.getTupleType();
    }


    /**
     * Returns the collection of RelNodes one of whose inputs is in this subset.
     */
    Set<AlgNode> getParents() {
        final Set<AlgNode> list = new LinkedHashSet<>();
        for ( AlgNode parent : set.getParentAlgs() ) {
            for ( AlgSubset alg : inputSubsets( parent ) ) {
                if ( alg.set == set && traitSet.satisfies( alg.getTraitSet() ) ) {
                    list.add( parent );
                }
            }
        }
        return list;
    }


    /**
     * Returns the collection of distinct subsets that contain a {@link AlgNode} one of whose inputs is in this subset.
     */
    Set<AlgSubset> getParentSubsets( VolcanoPlanner planner ) {
        final Set<AlgSubset> list = new LinkedHashSet<>();
        for ( AlgNode parent : set.getParentAlgs() ) {
            for ( AlgSubset alg : inputSubsets( parent ) ) {
                if ( alg.set == set && alg.getTraitSet().equals( traitSet ) ) {
                    list.add( planner.getSubset( parent ) );
                }
            }
        }
        return list;
    }


    private static List<AlgSubset> inputSubsets( AlgNode parent ) {
        //noinspection unchecked
        return (List<AlgSubset>) (List) parent.getInputs();
    }


    /**
     * Returns a list of algebra expressions one of whose children is this subset. The elements of the list are distinct.
     */
    public Collection<AlgNode> getParentRels() {
        final Set<AlgNode> list = new LinkedHashSet<>();
        parentLoop:
        for ( AlgNode parent : set.getParentAlgs() ) {
            for ( AlgSubset alg : inputSubsets( parent ) ) {
                if ( alg.set == set && traitSet.satisfies( alg.getTraitSet() ) ) {
                    list.add( parent );
                    continue parentLoop;
                }
            }
        }
        return list;
    }



    /**
     * Adds expression <code>rel</code> to this subset.
     */
    void add( AlgNode alg ) {
        if ( set.algs.contains( alg ) ) {
            return;
        }

        VolcanoPlanner planner = (VolcanoPlanner) alg.getCluster().getPlanner();
        if ( planner.listener != null ) {
            AlgEquivalenceEvent event = new AlgEquivalenceEvent( planner, alg, this, true );
            planner.listener.algEquivalenceFound( event );
        }

        // If this isn't the first alg in the set, it must have compatible row type.
        if ( set.alg != null ) {
            AlgOptUtil.equal( "rowtype of new alg", alg.getTupleType(), "rowtype of set", getTupleType(), Litmus.THROW );
        }
        set.addInternal( alg );
        if ( false ) {
            Set<CorrelationId> variablesSet = AlgOptUtil.getVariablesSet( alg );
            Set<CorrelationId> variablesStopped = alg.getVariablesSet();
            Set<CorrelationId> variablesPropagated = Util.minus( variablesSet, variablesStopped );
            assert set.variablesPropagated.containsAll( variablesPropagated );
            Set<CorrelationId> variablesUsed = AlgOptUtil.getVariablesUsed( alg );
            assert set.variablesUsed.containsAll( variablesUsed );
        }
    }


    /**
     * Recursively builds a tree consisting of the cheapest plan at each node.
     */
    AlgNode buildCheapestPlan( VolcanoPlanner planner ) {
        CheapestPlanReplacer replacer = new CheapestPlanReplacer( planner );
        final AlgNode cheapest = replacer.visit( this, -1, null );

        if ( planner.listener != null ) {
            AlgChosenEvent event = new AlgChosenEvent( planner, null );
            planner.listener.algChosen( event );
        }

        return cheapest;
    }


    /**
     * Checks whether an algexp has made its subset cheaper, and if it so, recursively checks whether that subset's parents have gotten cheaper.
     *
     * @param planner Planner
     * @param mq Metadata query
     * @param alg Relational expression whose cost has improved
     * @param activeSet Set of active subsets, for cycle detection
     */
    void propagateCostImprovements( VolcanoPlanner planner, AlgMetadataQuery mq, AlgNode alg, Set<AlgSubset> activeSet ) {
        for ( AlgSubset subset : set.subsets ) {
            if ( alg.getTraitSet().satisfies( subset.traitSet ) ) {
                subset.propagateCostImprovements0( planner, mq, alg, activeSet );
            }
        }
    }


    void propagateCostImprovements0(
            VolcanoPlanner planner, AlgMetadataQuery mq,
            AlgNode alg, Set<AlgSubset> activeSet ) {
        ++timestamp;

        if ( !activeSet.add( this ) ) {
            // This subset is already in the chain being propagated to. This means that the graph is cyclic, and therefore the cost of this algebra expression - not this subset - must be infinite.
            LOGGER.trace( "cyclic: {}", this );
            return;
        }
        try {
            final AlgOptCost cost = planner.getCost( alg, mq );
            if ( cost.isLt( bestCost ) ) {
                LOGGER.trace( "Subset cost improved: subset [{}] cost was {} now {}", this, bestCost, cost );

                bestCost = cost;
                best = alg;

                // Lower cost means lower importance. Other nodes will change too, but we'll get to them later.
                planner.ruleQueue.recompute( this );
                for ( AlgNode parent : getParents() ) {
                    final AlgSubset parentSubset = planner.getSubset( parent );
                    parentSubset.propagateCostImprovements( planner, mq, parent, activeSet );
                }
                planner.checkForSatisfiedConverters( set, alg );
            }
        } finally {
            activeSet.remove( this );
        }
    }


    public void propagateBoostRemoval( VolcanoPlanner planner ) {
        planner.ruleQueue.recompute( this );

        if ( boosted ) {
            boosted = false;

            for ( AlgSubset parentSubset : getParentSubsets( planner ) ) {
                parentSubset.propagateBoostRemoval( planner );
            }
        }
    }


    @Override
    public void collectVariablesUsed( Set<CorrelationId> variableSet ) {
        variableSet.addAll( set.variablesUsed );
    }


    @Override
    public void collectVariablesSet( Set<CorrelationId> variableSet ) {
        variableSet.addAll( set.variablesPropagated );
    }


    /**
     * Returns the alg nodes in this alg subset. All algs must have the same traits and are logically equivalent.
     *
     * @return all the algs in the subset
     */
    public Iterable<AlgNode> getAlgs() {
        return () -> Linq4j.asEnumerable( set.algs )
                .where( v1 -> v1.getTraitSet().satisfies( traitSet ) )
                .iterator();
    }


    /**
     * As {@link #getAlgs()} but returns a list.
     */
    public List<AlgNode> getAlgList() {
        final List<AlgNode> list = new ArrayList<>();
        for ( AlgNode alg : set.algs ) {
            if ( alg.getTraitSet().satisfies( traitSet ) ) {
                list.add( alg );
            }
        }
        return list;
    }


    /**
     * Visitor which walks over a tree of {@link AlgSet}s, replacing each node with the cheapest implementation of the expression.
     */
    static class CheapestPlanReplacer {

        VolcanoPlanner planner;


        CheapestPlanReplacer( VolcanoPlanner planner ) {
            super();
            this.planner = planner;
        }


        public AlgNode visit( AlgNode p, int ordinal, AlgNode parent ) {
            if ( p instanceof AlgSubset subset ) {
                AlgNode cheapest = subset.best;
                if ( cheapest == null ) {
                    AlgOptCost cost = planner.getCost( p, p.getCluster().getMetadataQuery() );

                    // Dump the planner's expression pool, so we can figure out why we reached impasse.
                    StringWriter sw = new StringWriter();
                    final PrintWriter pw = new PrintWriter( sw );
                    pw.println( "Node [" + subset.getDescription() + "] could not be implemented; planner state:\n" );
                    planner.dump( pw );
                    pw.flush();
                    final String dump = sw.toString();
                    RuntimeException e = new AlgPlanner.CannotPlanException( dump );
                    LOGGER.trace( "Caught exception in class={}, method=visit", getClass().getName(), e );
                    throw e;
                }
                p = cheapest;
            }

            if ( ordinal != -1 ) {
                if ( planner.listener != null ) {
                    AlgChosenEvent event = new AlgChosenEvent( planner, p );
                    planner.listener.algChosen( event );
                }
            }

            List<AlgNode> oldInputs = p.getInputs();
            List<AlgNode> inputs = new ArrayList<>();
            for ( int i = 0; i < oldInputs.size(); i++ ) {
                AlgNode oldInput = oldInputs.get( i );
                AlgNode input = visit( oldInput, i, p );
                inputs.add( input );
            }
            if ( !inputs.equals( oldInputs ) ) {
                final AlgNode pOld = p;
                p = p.copy( p.getTraitSet(), inputs );
                planner.provenances.put( p, new VolcanoPlanner.DirectProvenance( pOld ) );
            }
            return p;
        }

    }

}

