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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.CachingRelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutor;
import ch.unibas.dmi.dbis.polyphenydb.util.CancelFlag;
import ch.unibas.dmi.dbis.polyphenydb.util.trace.PolyphenyDbTrace;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;


/**
 * A <code>RelOptPlanner</code> is a query optimizer: it transforms a relational expression into a semantically equivalent relational expression, according to
 * a given set of rules and a cost model.
 */
public interface RelOptPlanner {

    Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();


    /**
     * Sets the root node of this query.
     *
     * @param rel Relational expression
     */
    void setRoot( RelNode rel );

    /**
     * Returns the root node of this query.
     *
     * @return Root node
     */
    RelNode getRoot();

    /**
     * Registers a rel trait definition. If the {@link RelTraitDef} has already been registered, does nothing.
     *
     * @return whether the RelTraitDef was added, as per {@link java.util.Collection#add}
     */
    boolean addRelTraitDef( RelTraitDef relTraitDef );

    /**
     * Clear all the registered RelTraitDef.
     */
    void clearRelTraitDefs();

    /**
     * Returns the list of active trait types.
     */
    List<RelTraitDef> getRelTraitDefs();

    /**
     * Removes all internal state, including all registered rules, materialized views, and lattices.
     */
    void clear();

    /**
     * Returns the list of all registered rules.
     */
    List<RelOptRule> getRules();

    /**
     * Registers a rule.
     *
     * If the rule has already been registered, does nothing. This method determines if the given rule is a {@link ConverterRule} and pass the
     * ConverterRule to all {@link #addRelTraitDef(RelTraitDef) registered} RelTraitDef instances.
     *
     * @return whether the rule was added, as per {@link java.util.Collection#add}
     */
    boolean addRule( RelOptRule rule );

    /**
     * Removes a rule.
     *
     * @return true if the rule was present, as per {@link java.util.Collection#remove(Object)}
     */
    boolean removeRule( RelOptRule rule );

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
     * Does nothing.
     *
     * @param cancelFlag flag which the planner should periodically check
     * @deprecated Previously, this method installed the cancellation-checking flag for this planner, but is now deprecated. Now, you should add a {@link CancelFlag} to the {@link Context} passed to the constructor.
     */
    @Deprecated
    // to be removed before 2.0
    void setCancelFlag( CancelFlag cancelFlag );

    /**
     * Changes a relational expression to an equivalent one with a different set of traits.
     *
     * @param rel Relational expression (may or may not have been registered; must not have the desired traits)
     * @param toTraits Trait set to convert the relational expression to
     * @return Relational expression with desired traits. Never null, but may be abstract
     */
    RelNode changeTraits( RelNode rel, RelTraitSet toTraits );

    /**
     * Negotiates an appropriate planner to deal with distributed queries. The idea is that the schemas decide among themselves which has the most knowledge. Right now, the local planner retains control.
     */
    RelOptPlanner chooseDelegate();

    /**
     * Defines a pair of relational expressions that are equivalent.
     *
     * Typically {@code tableRel} is a {@link LogicalTableScan} representing a table that is a materialized view and {@code queryRel} is the SQL expression that populates that view.
     * The intention is that {@code tableRel} is cheaper to evaluate and therefore if the query being optimized uses (or can be rewritten to use) {@code queryRel} as a sub-expression then it can be optimized by
     * using {@code tableRel} instead.
     */
    void addMaterialization( RelOptMaterialization materialization );

    /**
     * Returns the materializations that have been registered with the planner.
     */
    List<RelOptMaterialization> getMaterializations();

    /**
     * Defines a lattice.
     *
     * The lattice may have materializations; it is not necessary to call {@link #addMaterialization} for these; they are registered implicitly.
     */
    void addLattice( RelOptLattice lattice );

    /**
     * Retrieves a lattice, given its star table.
     */
    RelOptLattice getLattice( RelOptTable table );

    /**
     * Finds the most efficient expression to implement this query.
     *
     * @throws CannotPlanException if cannot find a plan
     */
    RelNode findBestExp();

    /**
     * Returns the factory that creates {@link RelOptCost}s.
     */
    RelOptCostFactory getCostFactory();

    /**
     * Computes the cost of a RelNode. In most cases, this just dispatches to {@link RelMetadataQuery#getCumulativeCost}.
     *
     * @param rel Relational expression of interest
     * @param mq Metadata query
     * @return estimated cost
     */
    RelOptCost getCost( RelNode rel, RelMetadataQuery mq );

    /**
     * @deprecated Use {@link #getCost(RelNode, RelMetadataQuery)} or, better, call {@link RelMetadataQuery#getCumulativeCost(RelNode)}.
     */
    @Deprecated
    // to be removed before 2.0
    RelOptCost getCost( RelNode rel );

    /**
     * Registers a relational expression in the expression bank.
     *
     * After it has been registered, you may not modify it.
     *
     * The expression must not already have been registered. If you are not sure whether it has been registered, call {@link #ensureRegistered(RelNode, RelNode)}.
     *
     * @param rel Relational expression to register (must not already be registered)
     * @param equivRel Relational expression it is equivalent to (may be null)
     * @return the same expression, or an equivalent existing expression
     */
    RelNode register( RelNode rel, RelNode equivRel );

    /**
     * Registers a relational expression if it is not already registered.
     *
     * If {@code equivRel} is specified, {@code rel} is placed in the same equivalence set. It is OK if {@code equivRel} has different traits;
     * {@code rel} will end up in a different subset of the same set.
     *
     * It is OK if {@code rel} is a subset.
     *
     * @param rel Relational expression to register
     * @param equivRel Relational expression it is equivalent to (may be null)
     * @return Registered relational expression
     */
    RelNode ensureRegistered( RelNode rel, RelNode equivRel );

    /**
     * Determines whether a relational expression has been registered.
     *
     * @param rel expression to test
     * @return whether rel has been registered
     */
    boolean isRegistered( RelNode rel );

    /**
     * Tells this planner that a schema exists. This is the schema's chance to tell the planner about all of the special transformation rules.
     */
    void registerSchema( RelOptSchema schema );

    /**
     * Adds a listener to this planner.
     *
     * @param newListener new listener to be notified of events
     */
    void addListener( RelOptListener newListener );

    /**
     * Gives this planner a chance to register one or more {@link RelMetadataProvider}s in the chain which will be used to answer metadata queries.
     *
     * Planners which use their own relational expressions internally to represent concepts such as equivalence classes will generally need to supply corresponding metadata providers.
     *
     * @param list receives planner's custom providers, if any
     */
    void registerMetadataProviders( List<RelMetadataProvider> list );

    /**
     * Gets a timestamp for a given rel's metadata. This timestamp is used by {@link CachingRelMetadataProvider} to decide whether cached metadata has gone stale.
     *
     * @param rel rel of interest
     * @return timestamp of last change which might affect metadata derivation
     */
    long getRelMetadataTimestamp( RelNode rel );

    /**
     * Sets the importance of a relational expression.
     *
     * An important use of this method is when a {@link RelOptRule} has created a relational expression which is indisputably better than the original relational expression.
     * The rule set the original relational expression's importance to zero, to reduce the search space. Pending rule calls are cancelled, and future rules will not fire.
     *
     * @param rel Relational expression
     * @param importance Importance
     */
    void setImportance( RelNode rel, double importance );

    /**
     * Registers a class of RelNode. If this class of RelNode has been seen before, does nothing.
     *
     * @param node Relational expression
     */
    void registerClass( RelNode node );

    /**
     * Creates an empty trait set. It contains all registered traits, and the default values of any traits that have them.
     *
     * The empty trait set acts as the prototype (a kind of factory) for all subsequently created trait sets.
     *
     * @return Empty trait set
     */
    RelTraitSet emptyTraitSet();

    /**
     * Sets the object that can execute scalar expressions.
     */
    void setExecutor( RexExecutor executor );

    /**
     * Returns the executor used to evaluate constant expressions.
     */
    RexExecutor getExecutor();

    /**
     * Called when a relational expression is copied to a similar expression.
     */
    void onCopy( RelNode rel, RelNode newRel );

    /**
     * @deprecated Use {@link RexExecutor}
     */
    @Deprecated
            // to be removed before 2.0
    interface Executor extends RexExecutor {

    }


    /**
     * Thrown by {@link ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner#findBestExp()}.
     */
    class CannotPlanException extends RuntimeException {

        public CannotPlanException( String message ) {
            super( message );
        }
    }
}
