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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptListener.AlgEquivalenceEvent;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * A <code>AlgSet</code> is an equivalence-set of expressions; that is, a set of expressions which have identical semantics. We are generally interested in using the expression which has the lowest cost.
 *
 * All of the expressions in an <code>AlgSet</code> have the same calling convention.
 */
class AlgSet {

    private static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    final List<AlgNode> algs = new ArrayList<>();
    /**
     * Relational expressions that have a subset in this set as a child. This is a multi-set. If multiple relational expressions in this set have the same parent, there will be multiple entries.
     */
    final List<AlgNode> parents = new ArrayList<>();
    final List<AlgSubset> subsets = new ArrayList<>();

    /**
     * List of {@link AbstractConverter} objects which have not yet been satisfied.
     */
    final List<AbstractConverter> abstractConverters = new ArrayList<>();

    /**
     * Set to the superseding set when this is found to be equivalent to another set.
     */
    AlgSet equivalentSet;
    AlgNode alg;

    /**
     * Variables that are set by relational expressions in this set and available for use by parent and child expressions.
     */
    final Set<CorrelationId> variablesPropagated;

    /**
     * Variables that are used by relational expressions in this set.
     */
    final Set<CorrelationId> variablesUsed;
    final int id;

    /**
     * Reentrancy flag.
     */
    boolean inMetadataQuery;


    AlgSet( int id, Set<CorrelationId> variablesPropagated, Set<CorrelationId> variablesUsed ) {
        this.id = id;
        this.variablesPropagated = variablesPropagated;
        this.variablesUsed = variablesUsed;
    }


    /**
     * Returns all of the {@link AlgNode}s which reference {@link AlgNode}s in this set.
     */
    public List<AlgNode> getParentAlgs() {
        return parents;
    }


    /**
     * @return all of the {@link AlgNode}s contained by any subset of this set (does not include the subset objects themselves)
     */
    public List<AlgNode> getAlgsFromAllSubsets() {
        return algs;
    }


    public AlgSubset getSubset( AlgTraitSet traits ) {
        for ( AlgSubset subset : subsets ) {
            if ( subset.getTraitSet().equals( traits ) ) {
                return subset;
            }
        }
        return null;
    }


    /**
     * Removes all references to a specific {@link AlgNode} in both the subsets and their parent relationships.
     */
    void obliterateAlgNode( AlgNode alg ) {
        parents.remove( alg );
    }


    /**
     * Adds a relational expression to a set, with its results available under a particular calling convention. An expression may be in the set several times with
     * different calling conventions (and hence different costs).
     */
    public AlgSubset add( AlgNode alg ) {
        assert equivalentSet == null : "adding to a dead set";
        final AlgTraitSet traitSet = alg.getTraitSet().simplify();
        final AlgSubset subset = getOrCreateSubset( alg.getCluster(), traitSet );
        subset.add( alg );
        return subset;
    }


    private void addAbstractConverters( VolcanoPlanner planner, AlgCluster cluster, AlgSubset subset, boolean subsetToOthers ) {
        // Converters from newly introduced subset to all the remaining one (vice versa), only if we can convert.  No point adding converters if it is not possible.
        for ( AlgSubset other : subsets ) {
            assert other.getTraitSet().size() == subset.getTraitSet().size();
            if ( (other == subset)
                    || (subsetToOthers
                    && !subset.getConvention().useAbstractConvertersForConversion(
                    subset.getTraitSet(), other.getTraitSet() ))
                    || (!subsetToOthers
                    && !other.getConvention().useAbstractConvertersForConversion(
                    other.getTraitSet(), subset.getTraitSet() )) ) {
                continue;
            }

            final ImmutableList<AlgTrait<?>> difference = subset.getTraitSet().difference( other.getTraitSet() );

            boolean addAbstractConverter = true;
            int numTraitNeedConvert = 0;

            for ( AlgTrait<?> curOtherTrait : difference ) {
                AlgTraitDef<?> traitDef = curOtherTrait.getTraitDef();
                AlgTrait<?> curAlgTrait = subset.getTraitSet().getTrait( traitDef );

                assert curAlgTrait.getTraitDef() == traitDef;

                if ( curAlgTrait == null ) {
                    addAbstractConverter = false;
                    break;
                }

                boolean canConvert;
                boolean needConvert;
                if ( subsetToOthers ) {
                    // We can convert from subset to other.  So, add converter with subset as child and traitset as the other's traitset.
                    canConvert = traitDef.canConvertUnchecked( cluster.getPlanner(), curAlgTrait, curOtherTrait, subset );
                    needConvert = !curAlgTrait.satisfies( curOtherTrait );
                } else {
                    // We can convert from others to subset.
                    canConvert = traitDef.canConvertUnchecked( cluster.getPlanner(), curOtherTrait, curAlgTrait, other );
                    needConvert = !curOtherTrait.satisfies( curAlgTrait );
                }

                if ( !canConvert ) {
                    addAbstractConverter = false;
                    break;
                }

                if ( needConvert ) {
                    numTraitNeedConvert++;
                }
            }

            if ( addAbstractConverter && numTraitNeedConvert > 0 ) {
                if ( subsetToOthers ) {
                    final AbstractConverter converter = new AbstractConverter( cluster, subset, null, other.getTraitSet() );
                    planner.register( converter, other );
                } else {
                    final AbstractConverter converter = new AbstractConverter( cluster, other, null, subset.getTraitSet() );
                    planner.register( converter, subset );
                }
            }
        }
    }


    AlgSubset getOrCreateSubset( AlgCluster cluster, AlgTraitSet traits ) {
        AlgSubset subset = getSubset( traits );
        if ( subset == null ) {
            subset = new AlgSubset( cluster, this, traits );

            final VolcanoPlanner planner = (VolcanoPlanner) cluster.getPlanner();

            addAbstractConverters( planner, cluster, subset, true );

            // Need to first add to subset before adding the abstract converters (for others->subset) since otherwise during register() the planner will try to add this subset again.
            subsets.add( subset );

            addAbstractConverters( planner, cluster, subset, false );

            if ( planner.listener != null ) {
                postEquivalenceEvent( planner, subset );
            }
        }
        return subset;
    }


    private void postEquivalenceEvent( VolcanoPlanner planner, AlgNode alg ) {
        AlgEquivalenceEvent event = new AlgEquivalenceEvent( planner, alg, "equivalence class " + id, false );
        planner.listener.algEquivalenceFound( event );
    }


    /**
     * Adds an expression <code>rel</code> to this set, without creating a {@link AlgSubset}. (Called only from {@link AlgSubset#add}.
     *
     * @param alg Relational expression
     */
    void addInternal( AlgNode alg ) {
        if ( !algs.contains( alg ) ) {
            algs.add( alg );
            for ( AlgTrait trait : alg.getTraitSet() ) {
                assert trait == trait.getTraitDef().canonize( trait );
            }

            VolcanoPlanner planner = (VolcanoPlanner) alg.getCluster().getPlanner();
            if ( planner.listener != null ) {
                postEquivalenceEvent( planner, alg );
            }
        }
        if ( this.alg == null ) {
            this.alg = alg;
        } else {
            // Row types must be the same, except for field names.
            AlgOptUtil.verifyTypeEquivalence( this.alg, alg, this );
        }
    }


    /**
     * Merges <code>otherSet</code> into this AlgSet.
     *
     * One generally calls this method after discovering that two algebra expressions are equivalent, and hence the <code>AlgSet</code>s they belong to are equivalent also.
     *
     * After this method completes, <code>otherSet</code> is obsolete, its {@link #equivalentSet} member points to this AlgSet, and this AlgSet is still alive.
     *
     * @param planner Planner
     * @param otherSet AlgSet which is equivalent to this one
     */
    void mergeWith( VolcanoPlanner planner, AlgSet otherSet ) {
        assert this != otherSet;
        assert this.equivalentSet == null;
        assert otherSet.equivalentSet == null;
        LOGGER.trace( "Merge set#{} into set#{}", otherSet.id, id );
        otherSet.equivalentSet = this;

        // remove from table
        boolean existed = planner.allSets.remove( otherSet );
        assert existed : "merging with a dead otherSet";

        // merge subsets
        for ( AlgSubset otherSubset : otherSet.subsets ) {
            planner.ruleQueue.subsetImportances.remove( otherSubset );
            AlgSubset subset = getOrCreateSubset( otherSubset.getCluster(), otherSubset.getTraitSet() );
            if ( otherSubset.bestCost.isLt( subset.bestCost ) ) {
                subset.bestCost = otherSubset.bestCost;
                subset.best = otherSubset.best;
            }
            for ( AlgNode otherAlg : otherSubset.getAlgs() ) {
                planner.reregister( this, otherAlg );
            }
        }

        // Has another set merged with this?
        assert equivalentSet == null;

        // Update all algs which have a child in the other set, to reflect the fact that the child has been renamed.
        //
        // Copy array to prevent ConcurrentModificationException.
        final List<AlgNode> previousParents =
                ImmutableList.copyOf( otherSet.getParentAlgs() );
        for ( AlgNode parentAlg : previousParents ) {
            planner.rename( parentAlg );
        }

        // Renaming may have caused this set to merge with another. If so, this set is now obsolete. There's no need to update the children of this set - indeed, it could be dangerous.
        if ( equivalentSet != null ) {
            return;
        }

        // Make sure the cost changes as a result of merging are propagated.
        final Set<AlgSubset> activeSet = new HashSet<>();
        final AlgMetadataQuery mq = alg.getCluster().getMetadataQuery();
        for ( AlgNode parentRel : getParentAlgs() ) {
            final AlgSubset parentSubset = planner.getSubset( parentRel );
            parentSubset.propagateCostImprovements( planner, mq, parentRel, activeSet );
        }
        assert activeSet.isEmpty();
        assert equivalentSet == null;

        // Each of the relations in the old set now has new parents, so potentially new rules can fire. Check for rule matches, just as if it were newly registered.
        // (This may cause rules which have fired once to fire again.)
        for ( AlgNode alg : algs ) {
            assert planner.getSet( alg ) == this;
            planner.fireRules( alg, true );
        }
    }

}
