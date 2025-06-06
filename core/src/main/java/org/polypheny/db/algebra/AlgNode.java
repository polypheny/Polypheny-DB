/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.core.Correlate;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.externalize.AlgWriterImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.Metadata;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgMetadata.GlobalStats;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgImplementor;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptNode;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Litmus;


/**
 * A <code>AlgNode</code> is a relational expression.
 * <p>
 * Relational expressions process data, so their names are typically verbs: Sort, Join, Project, Filter, Scan, Sample.
 * <p>
 * A relational expression is not a scalar expression; see {@link AlgNode} and {@link RexNode}.
 * <p>
 * If this type of relational expression has some particular planner rules, it should implement the <em>public static</em> method {@link AbstractAlgNode#register}.
 * <p>
 * When a relational expression comes to be implemented, the system allocates a {@link AlgImplementor} to manage the process. Every implementable relational expression
 * has a {@link AlgTraitSet} describing its physical attributes. The RelTraitSet always contains a {@link Convention} describing how the expression passes data to its consuming
 * relational expression, but may contain other traits, including some applied externally. Because traits can be applied externally, implementations of {@link AlgNode} should never assume the size or contents of
 * their trait set (beyond those traits configured by the {@link AlgNode} itself).
 * <p>
 * For each calling-convention, there is a corresponding sub-interface of AlgNode. For example,
 * {@code EnumerableAlg}
 * has operations to manage the conversion to a graph of
 * {@code EnumerableConvention}
 * calling-convention, and it interacts with a
 * {@code EnumerableAlgImplementor}.
 * <p>
 * A relational expression is only required to implement its calling-convention's interface when it is actually implemented, that is, converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement their convention's interface.
 * <p>
 * Every relational expression must derive from {@link AbstractAlgNode}. (Why have the <code>AlgNode</code> interface, then? We need a root interface, because an interface can only derive from an interface.)
 **/
public interface AlgNode extends AlgOptNode, Cloneable {

    /**
     * Returns a list of this relational expression's child expressions. (These are scalar expressions, and so do not include the relational inputs that are returned by {@link #getInputs}.
     * <p>
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
     * Return the CallingConvention trait from this AlgNode's {@link #getTraitSet() trait set}.
     *
     * @return this AlgNode's CallingConvention
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
    AlgNode getInput( int i );

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    @Override
    AlgDataType getTupleType();


    /**
     * Returns the type of the rows expected for an input. Defaults to {@link #getTupleType}.
     *
     * @param ordinalInParent input's 0-based ordinal with respect to this parent rel
     * @return expected row type
     */
    AlgDataType getExpectedInputRowType( int ordinalInParent );

    /**
     * Returns an array of this relational expression's inputs. If there are no inputs, returns an empty list, not {@code null}.
     *
     * @return collection of this relational expression's inputs
     */
    @Override
    List<AlgNode> getInputs();

    /**
     * Returns an estimate of the number of rows this relational expression will return.
     * <p>
     * Don't call this method directly. Instead, use {@link AlgMetadataQuery#getTupleCount}, which gives plugins a chance to override the rel's default ideas about row count.
     *
     * @param mq Metadata query
     * @return Estimate of the number of rows this relational expression will return
     */
    double estimateTupleCount( AlgMetadataQuery mq );

    /**
     * Returns the names of variables that are set in this relational expression but also used and therefore not available to parents of this relational expression.
     * <p>
     * Note: only {@link Correlate} should set variables.
     * <p>
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
     * <p>
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
     * Interacts with the {@link AlgVisitor} in a {@link Glossary#VISITOR_PATTERN visitor pattern} to traverse the tree of relational expressions.
     *
     * @param visitor Visitor that will traverse the tree of relational expressions
     */
    void childrenAccept( AlgVisitor visitor );

    /**
     * Returns the cost of this plan (not including children). The base implementation throws an error; derived classes should override.
     * <p>
     * NOTE: Don't call this method directly. Instead, use {@link AlgMetadataQuery#getNonCumulativeCost}, which gives plugins a chance to override the alg's default ideas about cost.
     *
     * @param planner Planner for cost calculation
     * @param mq Metadata query
     * @return Cost of this plan (not including children)
     */
    AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq );

    /**
     * Returns a metadata interface.
     *
     * @param <M> Type of metadata being requested
     * @param metadataClass Metadata interface
     * @param mq Metadata query
     * @return Metadata object that supplies the desired metadata (never null, although if the information is not present the metadata object may return null from all methods)
     */
    <M extends Metadata> M metadata( Class<M> metadataClass, AlgMetadataQuery mq );

    /**
     * Describes the inputs and attributes of this relational expression.
     * Each node should call {@code super.explain}, then call the
     * {@link AlgWriterImpl#input(String, AlgNode)}
     * and
     * {@link AlgWriterImpl#item(String, Object)}
     * methods for each input and attribute.
     *
     * @param pw Plan writer
     */
    void explain( AlgWriter pw );

    /**
     * Recursively constructs a string representation of the tree rooted at this AlgNode using the provided StringBuilder.
     * The basic structure of PolyAlgebra is {@code OPERATOR[attributes](children)}.
     *
     * @param sb StringBuilder for building the representation
     * @param prefix Prefix to be added in front of each operator (resulting in operators having a visual indentation) or null if not desired
     */
    void buildPolyAlgebra( StringBuilder sb, String prefix );

    default void buildPolyAlgebra( StringBuilder sb ) {
        buildPolyAlgebra( sb, null );
    }

    default String buildPolyAlgebra( String prefix ) {
        StringBuilder sb = new StringBuilder();
        buildPolyAlgebra( sb, prefix );
        return sb.toString();
    }

    /**
     * Recursively constructs a JSON object structure that represents the tree rooted at this AlgNode using the provided ObjectMapper.
     *
     * @param mapper the ObjectMapper used for creating JsonNodes.
     * @param gs the GlobalStats object containing the global maximums for stats or null if no metadata should be included
     * @return a ObjectNode representing the AlgNode tree rooted at this node.
     */
    ObjectNode serializePolyAlgebra( ObjectMapper mapper, GlobalStats gs );

    /**
     * Serialize this node without generating metadata.
     *
     * @param mapper the ObjectMapper used for creating JsonNodes.
     * @return a ObjectNode representing the AlgNode tree rooted at this node.
     */
    default ObjectNode serializePolyAlgebra( ObjectMapper mapper ) {
        return serializePolyAlgebra( mapper, null );
    }

    /**
     * Retrieves the PolyAlgDeclaration for this AlgNode implementation.
     * This declaration is read only and can thus be a static object.
     *
     * @return PolyAlgDeclaration for this AlgNode implementation
     */
    PolyAlgDeclaration getPolyAlgDeclaration();

    /**
     * Binds the arguments defining the state of this instance to the corresponding parameters. This is done for the purpose of creating the PolyAlgebra representation.
     * The returned PolyAlgArgs contains every {@link org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg} relevant for this AlgNode instance.
     * The corresponding {@link org.polypheny.db.algebra.polyalg.PolyAlgDeclaration} must be identical to {@code getPolyAlgDeclaration()}.
     * Its parameters can be used to retrieve corresponding argument values.
     *
     * @return PolyAlgArgs that maps parameters of the declaration to {@link org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg} that wraps the corresponding attribute
     */
    PolyAlgArgs bindArguments();

    /**
     * Receives notification that this expression is about to be registered. The implementation of this method must at least register all child expressions.
     *
     * @param planner Planner that plans this relational node
     * @return Relational expression that should be used by the planner
     */
    AlgNode onRegister( AlgPlanner planner );

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
    void replaceInput( int ordinalInParent, AlgNode p );

    /**
     * If this relational expression represents an access to a table, returns that table, otherwise returns null.
     *
     * @return If this relational expression represents an access to a table, returns that table, otherwise returns null
     */
    Entity getEntity();


    /**
     * If this relational expression represents an access to a table, returns wether the access modifies any data in that table.
     * If this relational expression does not represent an access this returns false.
     *
     * @return If this relational expression represents an access to a table, returns wether the access modifies any data in that table.
     */
    boolean isDataModifying();


    /**
     * Returns the name of this relational expression's class, sans package name, for use in explain. For example, for a
     * <code> org.polypheny.db.alg.ArrayRel.ArrayReader</code>, this method returns "ArrayReader".
     *
     * @return Name of this relational expression's class, sans package name, for use in explain
     */
    String getAlgTypeName();

    /**
     * Returns whether this relational expression is valid.
     * <p>
     * If assertions are enabled, this method is typically called with <code>litmus</code> = <code>THROW</code>, as follows:
     *
     * <blockquote>
     * <pre>assert alg.isValid(Litmus.THROW)</pre>
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
     * <p>
     * Sub-classes with other important attributes are encouraged to create variants of this method with more parameters.
     *
     * @param traitSet Trait set
     * @param inputs Inputs
     * @return Copy of this relational expression, substituting traits and inputs
     */
    AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs );

    /**
     * Registers any special rules specific to this kind of relational expression.
     * <p>
     * The planner calls this method this first time that it sees a relational expression of this class. The derived class should call {@link AlgPlanner#addRule} for each rule,
     * and then call {@code super.register}.
     *
     * @param planner Planner to be used to register additional relational expressions
     */
    void register( AlgPlanner planner );

    /**
     * Accepts a visit from a shuttle.
     *
     * @param shuttle Shuttle
     * @return A copy of this node incorporating changes made by the shuttle to this node's children
     */
    AlgNode accept( AlgShuttle shuttle );


    /**
     * Accepts a visit from a shuttle. If the shuttle updates expression, then a copy of the relation should be created.
     *
     * @param shuttle Shuttle
     * @return A copy of this node incorporating changes made by the shuttle to this node's children
     */
    AlgNode accept( RexShuttle shuttle );


    /**
     * Returns a string which allows to compare alg plans.
     */
    String algCompareString();

    /**
     * For optimized trees. Returns whether the involved operators support implementation caching. Default is true.
     * Only override if you need to set this to false.
     */
    default boolean isImplementationCacheable() {
        boolean isCacheable = true;
        for ( AlgNode child : getInputs() ) {
            isCacheable &= child.isImplementationCacheable();
        }
        return isCacheable;
    }

    /**
     * Expands node
     * If a part of AlgNode is a LogicalViewScan it is replaced
     * Else recursively hands call down if view in deeper level
     */
    default AlgNode unfoldView( @Nullable AlgNode parent, int index, AlgCluster cluster ) {
        int i = 0;
        for ( AlgNode node : getInputs() ) {
            node.unfoldView( this, i++, cluster );
        }
        return this;
    }

    default DataModel getModel() {
        return Objects.requireNonNullElse( getTraitSet().getTrait( ModelTraitDef.INSTANCE ), ModelTrait.RELATIONAL ).dataModel();
    }

    default boolean containsView() {
        return getInputs().stream().anyMatch( AlgNode::containsView );
    }

    default void replaceCluster( AlgCluster cluster ) {
        // empty on purpose
    }

    default boolean isCrossModel() {
        return getInputs().stream().anyMatch( AlgNode::isCrossModel );
    }

    default boolean containsJoin() {
        return getInputs().stream().anyMatch( AlgNode::containsJoin );
    }

    default boolean containsScan() {
        return getInputs().stream().anyMatch( AlgNode::containsScan );
    }

    default boolean containsEntity() {
        if ( getEntity() != null ) {
            return true;
        }
        return getInputs().stream().anyMatch( AlgNode::containsEntity );
    }

    default Set<Entity> getEntities() {
        if ( getEntity() != null ) {
            return Set.of( getEntity() );
        }
        return getInputs().stream().map( AlgNode::getEntities ).reduce( ( a, b ) -> Stream.concat( a.stream(), b.stream() ).collect( Collectors.toSet() ) ).orElse( Set.of() );
    }

    /**
     * Context of an algebra expression, for purposes of checking validity.
     */
    interface Context {

        Set<CorrelationId> correlationIds();

    }

}
