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


import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.Litmus;


/**
 * A match of a rule to a particular set of target relational expressions, frozen in time.
 */
class VolcanoRuleMatch extends VolcanoRuleCall {

    private final AlgSet targetSet;
    private AlgSubset targetSubset;
    private String digest;
    private double cachedImportance = Double.NaN;


    /**
     * Creates a <code>VolcanoRuleMatch</code>.
     *
     * @param operand0 Primary operand
     * @param algs List of targets; copied by the constructor, so the client can modify it later
     * @param nodeInputs Map from relational expressions to their inputs
     */
    VolcanoRuleMatch( VolcanoPlanner volcanoPlanner, AlgOptRuleOperand operand0, AlgNode[] algs, Map<AlgNode, List<AlgNode>> nodeInputs ) {
        super( volcanoPlanner, operand0, algs.clone(), nodeInputs );
        assert allNotNull( algs, Litmus.THROW );

        // Try to deduce which subset the result will belong to. Assume -- for now -- that the set is the same as the root relexp.
        targetSet = volcanoPlanner.getSet( algs[0] );
        assert targetSet != null : algs[0].toString() + " isn't in a set";
        digest = computeDigest();
    }


    public String toString() {
        return digest;
    }


    /**
     * Clears the cached importance value of this rule match. The importance will be re-calculated next time {@link #getImportance()} is called.
     */
    void clearCachedImportance() {
        cachedImportance = Double.NaN;
    }


    /**
     * Returns the importance of this rule.
     *
     * Calls {@link #computeImportance()} the first time, thereafter uses a cached value until {@link #clearCachedImportance()} is called.
     *
     * @return importance of this rule; a value between 0 and 1
     */
    double getImportance() {
        if ( Double.isNaN( cachedImportance ) ) {
            cachedImportance = computeImportance();
        }

        return cachedImportance;
    }


    /**
     * Computes the importance of this rule match.
     *
     * @return importance of this rule match
     */
    double computeImportance() {
        assert algs[0] != null;
        AlgSubset subset = volcanoPlanner.getSubset( algs[0] );
        double importance = 0;
        if ( subset != null ) {
            importance = volcanoPlanner.ruleQueue.getImportance( subset );
        }
        final AlgSubset targetSubset = guessSubset();
        if ( (targetSubset != null) && (targetSubset != subset) ) {
            // If this rule will generate a member of an equivalence class which is more important, use that importance.
            final double targetImportance = volcanoPlanner.ruleQueue.getImportance( targetSubset );
            if ( targetImportance > importance ) {
                importance = targetImportance;

                // If the equivalence class is cheaper than the target, bump up the importance of the rule. A converter is an easy way to make the plan cheaper, so we'd hate to miss this opportunity.
                //
                // REVIEW: jhyde: This rule seems to make sense, but is disabled until it has been proven.
                //
                // CHECKSTYLE: IGNORE 3
                if ( (subset != null) && subset.bestCost.isLt( targetSubset.bestCost ) && false ) {
                    importance *= targetSubset.bestCost.divideBy( subset.bestCost );
                    importance = Math.min( importance, 0.99 );
                }
            }
        }

        return importance;
    }


    /**
     * Computes a string describing this rule match. Two rule matches are equivalent if and only if their digests are the same.
     *
     * @return description of this rule match
     */
    private String computeDigest() {
        StringBuilder buf = new StringBuilder( "rule [" + getRule() + "] alg [" );
        for ( int i = 0; i < algs.length; i++ ) {
            if ( i > 0 ) {
                buf.append( ", " );
            }
            buf.append( algs[i].toString() );
        }
        buf.append( "]" );
        return buf.toString();
    }


    /**
     * Recomputes the digest of this VolcanoRuleMatch. It is necessary when sets have merged since the match was created.
     */
    public void recomputeDigest() {
        digest = computeDigest();
    }


    /**
     * Returns a guess as to which subset (that is equivalence class of relational expressions combined with a set of physical traits) the result of this rule will belong to.
     *
     * @return expected subset, or null if we cannot guess
     */
    private AlgSubset guessSubset() {
        if ( targetSubset != null ) {
            return targetSubset;
        }
        final AlgTrait targetTrait = getRule().getOutTrait();
        if ( (targetSet != null) && (targetTrait != null) ) {
            final AlgTraitSet targetTraitSet = algs[0].getTraitSet().replace( targetTrait );

            // Find the subset in the target set which matches the expected/ set of traits. It may not exist yet.
            targetSubset = targetSet.getSubset( targetTraitSet );
            return targetSubset;
        }

        // The target subset doesn't exist yet.
        return null;
    }


    /**
     * Returns whether all elements of a given array are not-null; fails if any are null.
     */
    private static <E> boolean allNotNull( E[] es, Litmus litmus ) {
        for ( E e : es ) {
            if ( e == null ) {
                return litmus.fail( "was null", (Object) es );
            }
        }
        return litmus.succeed();
    }

}

