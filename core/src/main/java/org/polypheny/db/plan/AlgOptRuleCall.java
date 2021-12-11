/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link AlgOptRule} with a set of {@link AlgNode relational expression}s as arguments.
 */
public abstract class AlgOptRuleCall {

    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    /**
     * Generator for {@link #id} values.
     */
    private static int nextId = 0;

    public final int id;
    protected final AlgOptRuleOperand operand0;
    protected Map<AlgNode, List<AlgNode>> nodeInputs;
    public final AlgOptRule rule;
    public final AlgNode[] algs;
    private final AlgOptPlanner planner;
    private final List<AlgNode> parents;


    /**
     * Creates a RelOptRuleCall.
     *
     * @param planner Planner
     * @param operand Root operand
     * @param algs Array of relational expressions which matched each operand
     * @param nodeInputs For each node which matched with {@code matchAnyChildren}=true, a list of the node's inputs
     * @param parents list of parent RelNodes corresponding to the first relational expression in the array argument, if known; otherwise, null
     */
    protected AlgOptRuleCall( AlgOptPlanner planner, AlgOptRuleOperand operand, AlgNode[] algs, Map<AlgNode, List<AlgNode>> nodeInputs, List<AlgNode> parents ) {
        this.id = nextId++;
        this.planner = planner;
        this.operand0 = operand;
        this.nodeInputs = nodeInputs;
        this.rule = operand.getRule();
        this.algs = algs;
        this.parents = parents;
        assert algs.length == rule.operands.size();
    }


    protected AlgOptRuleCall( AlgOptPlanner planner, AlgOptRuleOperand operand, AlgNode[] algs, Map<AlgNode, List<AlgNode>> nodeInputs ) {
        this( planner, operand, algs, nodeInputs, null );
    }


    /**
     * Returns the root operand matched by this rule.
     *
     * @return root operand
     */
    public AlgOptRuleOperand getOperand0() {
        return operand0;
    }


    /**
     * Returns the invoked planner rule.
     *
     * @return planner rule
     */
    public AlgOptRule getRule() {
        return rule;
    }


    /**
     * Returns a list of matched relational expressions.
     *
     * @return matched relational expressions
     * @see #alg(int)
     */
    public List<AlgNode> getAlgList() {
        return ImmutableList.copyOf( algs );
    }


    /**
     * Retrieves the {@code ordinal}th matched relational expression. This corresponds to the {@code ordinal}th operand of the rule.
     *
     * @param ordinal Ordinal
     * @param <T> Type
     * @return Relational expression
     */
    public <T extends AlgNode> T alg( int ordinal ) {
        //noinspection unchecked
        return (T) algs[ordinal];
    }


    /**
     * Returns the children of a given relational expression node matched in a rule.
     *
     * If the policy of the operand which caused the match is not {@link AlgOptRuleOperandChildPolicy#ANY}, the children will have their
     * own operands and therefore be easily available in the array returned by the {@link #getAlgList()} method, so this method returns null.
     *
     * This method is for {@link AlgOptRuleOperandChildPolicy#ANY}, which is generally used when a node can have a variable number of
     * children, and hence where the matched children are not retrievable by any other means.
     *
     * @param alg Relational expression
     * @return Children of relational expression
     */
    public List<AlgNode> getChildRels( AlgNode alg ) {
        return nodeInputs.get( alg );
    }


    /**
     * Assigns the input relational expressions of a given relational expression, as seen by this particular call. Is only called when the operand is {@link AlgOptRule#any()}.
     */
    protected void setChildRels( AlgNode alg, List<AlgNode> inputs ) {
        if ( nodeInputs.isEmpty() ) {
            nodeInputs = new HashMap<>();
        }
        nodeInputs.put( alg, inputs );
    }


    /**
     * Returns the planner.
     *
     * @return planner
     */
    public AlgOptPlanner getPlanner() {
        return planner;
    }


    /**
     * Returns the current RelMetadataQuery, to be used for instance by {@link AlgOptRule#onMatch(AlgOptRuleCall)}.
     */
    public AlgMetadataQuery getMetadataQuery() {
        return alg( 0 ).getCluster().getMetadataQuery();
    }


    /**
     * @return list of parents of the first relational expression
     */
    public List<AlgNode> getParents() {
        return parents;
    }


    /**
     * Registers that a rule has produced an equivalent relational expression.
     *
     * Called by the rule whenever it finds a match. The implementation of this method guarantees that the original relational expression (that is, <code>this.rels[0]</code>)
     * has its traits propagated to the new relational expression (<code>rel</code>) and its unregistered children. Any trait not specifically set in the RelTraitSet returned by
     * <code>alg.getTraits()</code> will be copied from <code>this.rels[0].getTraitSet()</code>.
     *
     * @param alg Relational expression equivalent to the root relational expression of the rule call, {@code call.rels(0)}
     * @param equiv Map of other equivalences
     */
    public abstract void transformTo( AlgNode alg, Map<AlgNode, AlgNode> equiv );


    /**
     * Registers that a rule has produced an equivalent relational expression, but no other equivalences.
     *
     * @param alg Relational expression equivalent to the root relational expression of the rule call, {@code call.rels(0)}
     */
    public final void transformTo( AlgNode alg ) {
        transformTo( alg, ImmutableMap.of() );
    }


    /**
     * Creates a {@link AlgBuilder} to be used by code within the call. The {@link AlgOptRule#algBuilderFactory} argument contains policies such as what
     * implementation of {@link Filter} to create.
     */
    public AlgBuilder builder() {
        return rule.algBuilderFactory.create( alg( 0 ).getCluster(), null );
    }

}

