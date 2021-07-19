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

package org.polypheny.db.rel;


import java.util.List;
import java.util.Set;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelImplementor;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptNode;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.core.Correlate;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.rel.metadata.Metadata;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Litmus;


/**
 * A <code>RelNode</code> is a relational expression.
 *
 * Relational expressions process data, so their names are typically verbs: Sort, Join, Project, Filter, Scan, Sample.
 *
 * A relational expression is not a scalar expression; see {@link org.polypheny.db.sql.SqlNode} and {@link RexNode}.
 *
 * If this type of relational expression has some particular planner rules, it should implement the <em>public static</em> method {@link AbstractRelNode#register}.
 *
 * When a relational expression comes to be implemented, the system allocates a {@link RelImplementor} to manage the process. Every implementable relational expression
 * has a {@link RelTraitSet} describing its physical attributes. The RelTraitSet always contains a {@link Convention} describing how the expression passes data to its consuming
 * relational expression, but may contain other traits, including some applied externally. Because traits can be applied externally, implementations of RelNode should never assume the size or contents of
 * their trait set (beyond those traits configured by the RelNode itself).
 *
 * For each calling-convention, there is a corresponding sub-interface of RelNode. For example,
 * {@code org.polypheny.db.adapter.enumerable.EnumerableRel}
 * has operations to manage the conversion to a graph of
 * {@code org.polypheny.db.adapter.enumerable.EnumerableConvention}
 * calling-convention, and it interacts with a
 * {@code EnumerableRelImplementor}.
 *
 * A relational expression is only required to implement its calling-convention's interface when it is actually implemented, that is, converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement their convention's interface.
 *
 * Every relational expression must derive from {@link AbstractRelNode}. (Why have the <code>RelNode</code> interface, then? We need a root interface, because an interface can only derive from an interface.)
 */
public interface RelNode extends RelOptNode, Cloneable {

    /**
     * Returns a list of this relational expression's child expressions. (These are scalar expressions, and so do not include the relational inputs that are returned by {@link #getInputs}.
     *
     * The caller should treat the list as unmodifiable; typical implementations will return an immutable list. If there are no child expressions, returns an empty list, not <code>null</code>.
     *
     * @return List of this relational expression's child expressions
     * @see #accept(RexShuttle)
     * @deprecated use #accept(org.polypheny.db.rex.RexShuttle)
     */
    @Deprecated
    // to be removed before 2.0
    List<RexNode> getChildExps();

    /**
     * Return the CallingConvention trait from this RelNode's {@link #getTraitSet() trait set}.
     *
     * @return this RelNode's CallingConvention
     */
    Convention getConvention();

    /**
     * Returns the name of the variable which is to be implicitly set at runtime each time a row is returned from the first input of this relational expression; or null if there is no variable.
     *
     * @return Name of correlating variable, or null
     */
    String getCorrelVariable();

    /**
     * Returns the <code>i</code><sup>th</sup> input relational expression.
     *
     * @param i Ordinal of input
     * @return <code>i</code><sup>th</sup> input
     */
    RelNode getInput( int i );

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    @Override
    RelDataType getRowType();

    /**
     * Returns the type of the rows expected for an input. Defaults to {@link #getRowType}.
     *
     * @param ordinalInParent input's 0-based ordinal with respect to this parent rel
     * @return expected row type
     */
    RelDataType getExpectedInputRowType( int ordinalInParent );

    /**
     * Returns an array of this relational expression's inputs. If there are no inputs, returns an empty list, not {@code null}.
     *
     * @return Array of this relational expression's inputs
     */
    @Override
    List<RelNode> getInputs();

    /**
     * Returns an estimate of the number of rows this relational expression will return.
     *
     * Don't call this method directly. Instead, use {@link RelMetadataQuery#getRowCount}, which gives plugins a chance to override the rel's default ideas about row count.
     *
     * @param mq Metadata query
     * @return Estimate of the number of rows this relational expression will return
     */
    double estimateRowCount( RelMetadataQuery mq );

    /**
     * Returns the names of variables that are set in this relational expression but also used and therefore not available to parents of this relational expression.
     *
     * Note: only {@link Correlate} should set variables.
     *
     * Note: {@link #getVariablesSet()} is equivalent but returns {@link CorrelationId} rather than their names. It is preferable except for calling old methods that require a set of strings.
     *
     * @return Names of variables which are set in this relational expression
     * @deprecated Use {@link #getVariablesSet()} and {@link CorrelationId#names(Set)}
     */
    @Deprecated
    // to be removed before 2.0
    Set<String> getVariablesStopped();

    /**
     * Returns the variables that are set in this relational expression but also used and therefore not available to parents of this relational expression.
     *
     * Note: only {@link Correlate} should set variables.
     *
     * @return Names of variables which are set in this relational expression
     */
    Set<CorrelationId> getVariablesSet();

    /**
     * Collects variables known to be used by this expression or its descendants. By default, no such information is available and must be derived by analyzing sub-expressions, but some optimizer implementations
     * may insert special expressions which remember such information.
     *
     * @param variableSet receives variables used
     */
    void collectVariablesUsed( Set<CorrelationId> variableSet );

    /**
     * Collects variables set by this expression.
     * TODO: is this required?
     *
     * @param variableSet receives variables known to be set by
     */
    void collectVariablesSet( Set<CorrelationId> variableSet );

    /**
     * Interacts with the {@link RelVisitor} in a {@link Glossary#VISITOR_PATTERN visitor pattern} to traverse the tree of relational expressions.
     *
     * @param visitor Visitor that will traverse the tree of relational expressions
     */
    void childrenAccept( RelVisitor visitor );

    /**
     * Returns the cost of this plan (not including children). The base implementation throws an error; derived classes should override.
     *
     * NOTE: Don't call this method directly. Instead, use {@link RelMetadataQuery#getNonCumulativeCost}, which gives plugins a chance to override the rel's default ideas about cost.
     *
     * @param planner Planner for cost calculation
     * @param mq Metadata query
     * @return Cost of this plan (not including children)
     */
    RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq );

    /**
     * Returns a metadata interface.
     *
     * @param <M> Type of metadata being requested
     * @param metadataClass Metadata interface
     * @param mq Metadata query
     * @return Metadata object that supplies the desired metadata (never null, although if the information is not present the metadata object may return null from all methods)
     */
    <M extends Metadata> M metadata( Class<M> metadataClass, RelMetadataQuery mq );

    /**
     * Describes the inputs and attributes of this relational expression.
     * Each node should call {@code super.explain}, then call the
     * {@link org.polypheny.db.rel.externalize.RelWriterImpl#input(String, RelNode)}
     * and
     * {@link org.polypheny.db.rel.externalize.RelWriterImpl#item(String, Object)}
     * methods for each input and attribute.
     *
     * @param pw Plan writer
     */
    void explain( RelWriter pw );

    /**
     * Receives notification that this expression is about to be registered. The implementation of this method must at least register all child expressions.
     *
     * @param planner Planner that plans this relational node
     * @return Relational expression that should be used by the planner
     */
    RelNode onRegister( RelOptPlanner planner );

    /**
     * Computes the digest, assigns it, and returns it. For planner use only.
     *
     * @return Digest of this relational expression
     */
    String recomputeDigest();

    /**
     * Replaces the <code>ordinalInParent</code><sup>th</sup> input. You must override this method if you override {@link #getInputs}.
     *
     * @param ordinalInParent Position of the child input, 0 is the first
     * @param p New node that should be put at position {@code ordinalInParent}
     */
    void replaceInput( int ordinalInParent, RelNode p );

    /**
     * If this relational expression represents an access to a table, returns that table, otherwise returns null.
     *
     * @return If this relational expression represents an access to a table, returns that table, otherwise returns null
     */
    RelOptTable getTable();

    /**
     * Returns the name of this relational expression's class, sans package name, for use in explain. For example, for a
     * <code> org.polypheny.db.rel.ArrayRel.ArrayReader</code>, this method returns "ArrayReader".
     *
     * @return Name of this relational expression's class, sans package name, for use in explain
     */
    String getRelTypeName();

    /**
     * Returns whether this relational expression is valid.
     *
     * If assertions are enabled, this method is typically called with <code>litmus</code> = <code>THROW</code>, as follows:
     *
     * <blockquote>
     * <pre>assert rel.isValid(Litmus.THROW)</pre>
     * </blockquote>
     *
     * This signals that the method can throw an {@link AssertionError} if it is not valid.
     *
     * @param litmus What to do if invalid
     * @param context Context for validity checking
     * @return Whether relational expression is valid
     * @throws AssertionError if this relational expression is invalid and litmus is THROW
     */
    boolean isValid( Litmus litmus, Context context );

    /**
     * Creates a copy of this relational expression, perhaps changing traits and inputs.
     *
     * Sub-classes with other important attributes are encouraged to create variants of this method with more parameters.
     *
     * @param traitSet Trait set
     * @param inputs Inputs
     * @return Copy of this relational expression, substituting traits and inputs
     */
    RelNode copy( RelTraitSet traitSet, List<RelNode> inputs );

    /**
     * Registers any special rules specific to this kind of relational expression.
     *
     * The planner calls this method this first time that it sees a relational expression of this class. The derived class should call {@link org.polypheny.db.plan.RelOptPlanner#addRule} for each rule,
     * and then call {@code super.register}.
     *
     * @param planner Planner to be used to register additional relational expressions
     */
    void register( RelOptPlanner planner );

    /**
     * Accepts a visit from a shuttle.
     *
     * @param shuttle Shuttle
     * @return A copy of this node incorporating changes made by the shuttle to this node's children
     */
    RelNode accept( RelShuttle shuttle );

    /**
     * Accepts a visit from a shuttle. If the shuttle updates expression, then a copy of the relation should be created.
     *
     * @param shuttle Shuttle
     * @return A copy of this node incorporating changes made by the shuttle to this node's children
     */
    RelNode accept( RexShuttle shuttle );

    /**
     * Returns a string which allows to compare rel plans.
     */
    String relCompareString();

    /**
     * For optimized trees. Returns whether the involved operators support implementation caching. Default is true.
     * Only override if you need to set this to false.
     */
    default boolean isImplementationCacheable() {
        boolean isCacheable = true;
        for ( RelNode child : getInputs() ) {
            isCacheable &= child.isImplementationCacheable();
        }
        return isCacheable;
    }

    /**
     * To check if a RelNode includes a ViewTableScan
     */
    default boolean hasView() {
        return false;
    }

    /**
     * expands node
     * if a part of RelNode is a LogicalViewTableScan it is replaced
     * else recursively hands call down if view in deeper level
     */
    default void tryExpandView( RelNode input ) {
        if ( input instanceof LogicalViewTableScan ) {
            input = ((LogicalViewTableScan) input).expandViewNode();
        } else {
            input.tryExpandView( input );
        }
    }

    default RelNode tryParentExpandView( RelNode input ) {
        if ( input instanceof LogicalViewTableScan ) {
            return ((LogicalViewTableScan) input).expandViewNode();
        } else {
            input.tryExpandView( input );
            return input;
        }
    }

    /**
     * Context of a relational expression, for purposes of checking validity.
     */
    interface Context {

        Set<CorrelationId> correlationIds();

    }

}

