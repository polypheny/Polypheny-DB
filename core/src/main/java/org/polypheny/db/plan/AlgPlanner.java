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


import java.util.List;
import java.util.regex.Pattern;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.CachingAlgMetadataProvider;
import org.polypheny.db.plan.volcano.VolcanoPlannerPhase;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * A <code>AlgOptPlanner</code> is a query optimizer: it transforms an algebra expression into a semantically equivalent algebra expression, according to
 * a given set of rules and a cost model.
 */
public interface AlgPlanner {

    Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();


    /**
     * Sets the root node of this query.
     *
     * @param alg Algebra expression
     */
    void setRoot( AlgNode alg );

    /**
     * Returns the root node of this query.
     *
     * @return Root node
     */
    AlgNode getRoot();

    /**
     * Registers an alg trait definition. If the {@link AlgTraitDef} has already been registered, does nothing.
     *
     * @return whether the AlgTraitDef was added, as per {@link java.util.Collection#add}
     */
    boolean addAlgTraitDef( AlgTraitDef<?> algTraitDef );

    /**
     * Clear all the registered AlgTraitDef.
     */
    void clearAlgTraitDefs();

    /**
     * Returns the list of active trait types.
     */
    List<AlgTraitDef<?>> getAlgTraitDefs();

    /**
     * Removes all internal state, including all registered rules.
     */
    void clear();

    /**
     * Returns the list of all registered rules.
     */
    List<AlgOptRule> getRules();

    /**
     * Registers a rule.
     * <p>
     * If the rule has already been registered, does nothing. This method determines if the given rule is a {@link ConverterRule} and pass the
     * ConverterRule to all {@link #addAlgTraitDef(AlgTraitDef) registered} AlgTraitDef instances.
     *
     * @return whether the rule was added, as per {@link java.util.Collection#add}
     */
    boolean addRule( AlgOptRule rule );

    boolean addRule( AlgOptRule operand, VolcanoPlannerPhase phase );

    /**
     * Removes a rule.
     *
     * @return true if the rule was present, as per {@link java.util.Collection#remove(Object)}
     */
    boolean removeRule( AlgOptRule rule );

    /**
     * Provides the Context created when this planner was constructed.
     *
     * @return Never null; either an externally defined context, or a dummy context that returns null for each requested interface
     */
    Context getContext();

    /**
     * Sets the exclusion filter to use for this planner. Rules which match the given pattern will not be fired regardless of whether or when they are added to the planner.
     *
     * @param exclusionFilter pattern to match for exclusion; null to disable filtering
     */
    void setRuleDescExclusionFilter( Pattern exclusionFilter );


    /**
     * Changes an algebraic expression to an equivalent one with a different set of traits.
     *
     * @param alg Algebra expression (may or may not have been registered; must not have the desired traits)
     * @param toTraits Trait set to convert the algebra expression to
     * @return Algbera expression with desired traits. Never null, but may be abstract
     */
    AlgNode changeTraits( AlgNode alg, AlgTraitSet toTraits );

    /**
     * Negotiates an appropriate planner to deal with distributed queries. The idea is that the schemas decide among themselves which has the most knowledge. Right now, the local planner retains control.
     */
    AlgPlanner chooseDelegate();

    /**
     * Finds the most efficient expression to implement this query.
     *
     * @throws CannotPlanException if cannot find a plan
     */
    AlgNode findBestExp();

    /**
     * Returns the factory that creates {@link AlgOptCost}s.
     */
    AlgOptCostFactory getCostFactory();

    /**
     * Computes the cost of a AlgNode. In most cases, this just dispatches to {@link AlgMetadataQuery#getCumulativeCost}.
     *
     * @param alg Algebra expression of interest
     * @param mq Metadata query
     * @return estimated cost
     */
    AlgOptCost getCost( AlgNode alg, AlgMetadataQuery mq );


    /**
     * Registers an algebra expression in the expression bank.
     * <p>
     * After it has been registered, you may not modify it.
     * <p>
     * The expression must not already have been registered. If you are not sure whether it has been registered, call {@link #ensureRegistered(AlgNode, AlgNode)}.
     *
     * @param alg Algebraic expression to register (must not already be registered)
     * @param equivAlg Algebraic expression it is equivalent to (may be null)
     * @return the same expression, or an equivalent existing expression
     */
    AlgNode register( AlgNode alg, AlgNode equivAlg );

    /**
     * Registers an algebra expression if it is not already registered.
     * <p>
     * If {@code equivAlg} is specified, {@code alg} is placed in the same equivalence set. It is OK if {@code equivAlg} has different traits;
     * {@code alg} will end up in a different subset of the same set.
     * <p>
     * It is OK if {@code alg} is a subset.
     *
     * @param alg Algebraic expression to register
     * @param equivAlg Algebraic expression it is equivalent to (may be null)
     * @return Registered algebra expression
     */
    AlgNode ensureRegistered( AlgNode alg, AlgNode equivAlg );

    /**
     * Determines whether an algebra expression has been registered.
     *
     * @param alg expression to test
     * @return whether alg has been registered
     */
    boolean isRegistered( AlgNode alg );

    void addRuleDuringRuntime( AlgOptRule operands );

    /**
     * Adds a listener to this planner.
     *
     * @param newListener new listener to be notified of events
     */
    void addListener( AlgOptListener newListener );

    /**
     * Gives this planner a chance to register one or more {@link AlgMetadataProvider}s in the chain which will be used to answer metadata queries.
     * <p>
     * Planners which use their own algebra expressions internally to represent concepts such as equivalence classes will generally need to supply corresponding metadata providers.
     *
     * @param list receives planner's custom providers, if any
     */
    void registerMetadataProviders( List<AlgMetadataProvider> list );

    /**
     * Gets a timestamp for a given algs metadata. This timestamp is used by {@link CachingAlgMetadataProvider} to decide whether cached metadata has gone stale.
     *
     * @param alg alg of interest
     * @return timestamp of last change which might affect metadata derivation
     */
    long getAlgMetadataTimestamp( AlgNode alg );

    /**
     * Sets the importance of an algebra expression.
     * <p>
     * An important use of this method is when a {@link AlgOptRule} has created an algebra expression which is indisputably better than the original algebra expression.
     * The rule set the original algebra expression's importance to zero, to reduce the search space. Pending rule calls are cancelled, and future rules will not fire.
     *
     * @param alg Algebra expression
     * @param importance Importance
     */
    void setImportance( AlgNode alg, double importance );

    /**
     * Registers a class of AlgNode. If this class of {@link AlgNode} has been seen before, does nothing.
     *
     * @param node Algebra expression
     */
    void registerClass( AlgNode node );

    /**
     * Creates an empty trait set. It contains all registered traits, and the default values of any traits that have them.
     * <p>
     * The empty trait set acts as the prototype (a kind of factory) for all subsequently created trait sets.
     *
     * @return Empty trait set
     */
    AlgTraitSet emptyTraitSet();

    /**
     * Sets the object that can execute scalar expressions.
     */
    void setExecutor( RexExecutor executor );

    /**
     * Returns the executor used to evaluate constant expressions.
     */
    RexExecutor getExecutor();

    /**
     * Called when an algebraic expression is copied to a similar expression.
     */
    void onCopy( AlgNode alg, AlgNode newAlg );

    /**
     * Thrown by {@link AlgPlanner#findBestExp()}.
     */
    class CannotPlanException extends RuntimeException {

        public CannotPlanException( String message ) {
            super( message );
        }

    }

}
