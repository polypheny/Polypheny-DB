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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link AlgOptRule} with a set of {@link AlgNode algebra expression}s as arguments.
 */
public abstract class AlgOptRuleCall {

    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    /**
     * Generator for {@link #id} values.
     */
    private static int nextId = 0;

    public final int id;

    @Getter
    protected final AlgOptRuleOperand operand0;
    protected Map<AlgNode, List<AlgNode>> nodeInputs;

    @Getter
    public final AlgOptRule rule;
    public final AlgNode[] algs;

    @Getter
    private final AlgPlanner planner;

    @Getter
    private final List<AlgNode> parents;


    /**
     * Creates a RelOptRuleCall.
     *
     * @param planner Planner
     * @param operand Root operand
     * @param algs Collection of algebra expressions which matched each operand
     * @param nodeInputs For each node which matched with {@code matchAnyChildren}=true, a list of the node's inputs
     * @param parents list of parent AlgNodes corresponding to the first algebra expression in the array argument, if known; otherwise, null
     */
    protected AlgOptRuleCall( AlgPlanner planner, AlgOptRuleOperand operand, AlgNode[] algs, Map<AlgNode, List<AlgNode>> nodeInputs, List<AlgNode> parents ) {
        this.id = nextId++;
        this.planner = planner;
        this.operand0 = operand;
        this.nodeInputs = nodeInputs;
        this.rule = operand.getRule();
        this.algs = algs;
        this.parents = parents;
        assert algs.length == rule.operands.size();
    }


    protected AlgOptRuleCall( AlgPlanner planner, AlgOptRuleOperand operand, AlgNode[] algs, Map<AlgNode, List<AlgNode>> nodeInputs ) {
        this( planner, operand, algs, nodeInputs, null );
    }


    /**
     * Retrieves the {@code ordinal}th matched algebra expression. This corresponds to the {@code ordinal}th operand of the rule.
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
     * Returns the children of a given algebra expression node matched in a rule.
     * <p>
     * If the policy of the operand which caused the match is not {@link AlgOptRuleOperandChildPolicy#ANY}, the children will have their
     * own operands, so this method returns null.
     * <p>
     * This method is for {@link AlgOptRuleOperandChildPolicy#ANY}, which is generally used when a node can have a variable number of
     * children, and hence where the matched children are not retrievable by any other means.
     *
     * @param alg Relational expression
     * @return Children of algebra expression
     */
    public List<AlgNode> getChildAlgs( AlgNode alg ) {
        return nodeInputs.get( alg );
    }


    /**
     * Assigns the input algebra expressions of a given algebra expression, as seen by this particular call. Is only called when the operand is {@link AlgOptRule#any()}.
     */
    protected void setChildAlgs( AlgNode alg, List<AlgNode> inputs ) {
        if ( nodeInputs.isEmpty() ) {
            nodeInputs = new HashMap<>();
        }
        nodeInputs.put( alg, inputs );
    }


    /**
     * Returns the current RelMetadataQuery, to be used for instance by {@link AlgOptRule#onMatch(AlgOptRuleCall)}.
     */
    public AlgMetadataQuery getMetadataQuery() {
        return alg( 0 ).getCluster().getMetadataQuery();
    }


    /**
     * Registers that a rule has produced an equivalent algebraic expression.
     * <p>
     * Called by the rule whenever it finds a match. The implementation of this method guarantees that the original algebraic expression (that is, <code>this.algs[0]</code>)
     * has its traits propagated to the new algebraic expression (<code>alg</code>) and its unregistered children. Any trait not specifically set in the AlgTraitSet returned by
     * <code>alg.getTraits()</code> will be copied from <code>this.algs[0].getTraitSet()</code>.
     *
     * @param alg Algebraic expression equivalent to the root algebraic expression of the rule call, {@code call.algs(0)}
     * @param equiv Map of other equivalences
     */
    public abstract void transformTo( AlgNode alg, Map<AlgNode, AlgNode> equiv );


    /**
     * Registers that a rule has produced an equivalent algebraic expression, but no other equivalences.
     *
     * @param alg Algebraic expression equivalent to the root algebraic expression of the rule call, {@code call.algs(0)}
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

