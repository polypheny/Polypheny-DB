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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.externalize.AlgWriterImpl;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.Metadata;
import org.polypheny.db.algebra.metadata.MetadataFactory;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexElementRef;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexRangeRef;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexTableIndexRef;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Base class for every algebraic expression ({@link AlgNode}).
 */
@SuperBuilder(toBuilder = true)
public abstract class AbstractAlgNode implements AlgNode {

    /**
     * Generator for {@link #id} values.
     */
    private static final AtomicInteger NEXT_ID = new AtomicInteger( 0 );

    private static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();


    /**
     * Description, consists of id plus digest.
     */
    private String desc;

    /**
     * Cached type of this relational expression.
     */
    protected AlgDataType rowType;

    /**
     * A short description of this expression's type, inputs, and other properties. The string uniquely identifies
     * the node; another node is equivalent if and only if it has the same value.
     * Computed by {@link #computeDigest}, assigned by {@link #onRegister}, returned by {@link #getDigest()}.
     *
     * @see #desc
     */
    @Getter
    protected String digest;

    // Setter is used to set the cluster in Views
    @Setter
    @Getter
    private transient AlgCluster cluster;

    /**
     * unique id of this object -- for debugging
     */
    @Getter
    protected final int id;

    /**
     * The AlgTraitSet that describes the traits of this AlgNode.
     * Setter is used to set the cluster in Views
     */
    @Setter
    @Getter
    protected AlgTraitSet traitSet;


    /**
     * Creates an <code>AbstractAlgNode</code>.
     */
    public AbstractAlgNode( AlgCluster cluster, AlgTraitSet traitSet ) {
        super();
        assert cluster != null;
        this.cluster = cluster;
        this.traitSet = traitSet;
        this.id = NEXT_ID.getAndIncrement();
        this.digest = getAlgTypeName() + "#" + id;
        this.desc = digest;
        LOGGER.trace( "new {}", digest );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        // Note that empty set equals empty set, so expressions with zero inputs do not generally need to implement their own copy method.
        if ( getInputs().equals( inputs ) && traitSet == getTraitSet() ) {
            return this;
        }
        throw new AssertionError( "Algebra expression should override copy. Class=[" + getClass() + "]; traits=[" + getTraitSet() + "]; desired traits=[" + traitSet + "]" );
    }


    public static <T> T sole( List<T> collection ) {
        assert collection.size() == 1;
        return collection.get( 0 );
    }


    @Override
    public List<RexNode> getChildExps() {
        return ImmutableList.of();
    }


    @Override
    public final Convention getConvention() {
        return traitSet.getTrait( ConventionTraitDef.INSTANCE );
    }


    @Override
    public String getCorrelVariable() {
        return null;
    }


    @Override
    public AlgNode getInput( int i ) {
        List<AlgNode> inputs = getInputs();
        return inputs.get( i );
    }


    @Override
    public void register( AlgPlanner planner ) {
        Util.discard( planner );
    }


    @Override
    public final String getAlgTypeName() {
        String className = getClass().getName();
        int i = className.lastIndexOf( "$" );
        if ( i >= 0 ) {
            return className.substring( i + 1 );
        }
        i = className.lastIndexOf( "." );
        if ( i >= 0 ) {
            return className.substring( i + 1 );
        }
        return className;
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        return litmus.succeed();
    }


    @Override
    public final AlgDataType getTupleType() {
        if ( rowType == null ) {
            rowType = deriveRowType();
            assert rowType != null : this;
        }
        return rowType;
    }


    protected AlgDataType deriveRowType() {
        // This method is only called if rowType is null, so you don't NEED to implement it if rowType is always set.
        throw new UnsupportedOperationException( "The rowType could not be derived." );
    }


    @Override
    public AlgDataType getExpectedInputRowType( int ordinalInParent ) {
        return getTupleType();
    }


    @Override
    public List<AlgNode> getInputs() {
        return Collections.emptyList();
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return 1.0;
    }


    @Override
    public final Set<String> getVariablesStopped() {
        return CorrelationId.names( getVariablesSet() );
    }


    @Override
    public ImmutableSet<CorrelationId> getVariablesSet() {
        return ImmutableSet.of();
    }


    @Override
    public void collectVariablesUsed( Set<CorrelationId> variableSet ) {
        // for default case, nothing to do
    }


    @Override
    public void collectVariablesSet( Set<CorrelationId> variableSet ) {
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        List<AlgNode> inputs = getInputs();
        for ( int i = 0; i < inputs.size(); i++ ) {
            visitor.visit( inputs.get( i ), i, this );
        }
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        // Call fall-back method. Specific logical types (such as LogicalProject and LogicalJoin) have their own RelShuttle.visit methods.
        return shuttle.visit( this );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        return this;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // by default, assume cost is proportional to number of rows
        double tupleCount = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( tupleCount, tupleCount, 0 );
    }


    @Override
    public final <M extends Metadata> M metadata( Class<M> metadataClass, AlgMetadataQuery mq ) {
        final MetadataFactory factory = cluster.getMetadataFactory();
        final M metadata = factory.query( this, mq, metadataClass );
        assert metadata != null : "no provider found (alg=" + this + ", m=" + metadataClass + "); a backstop provider is recommended";
        // Usually the metadata belongs to the alg that created it. AlgSubset and HepAlgVertex are notable exceptions, so
        // disable the assert. It's not worth the performance hit to override this method for them.
        return metadata;
    }


    @Override
    public void explain( AlgWriter pw ) {
        explainTerms( pw ).done( this );
    }


    /**
     * Describes the inputs and attributes of this expression.
     * Each node should call {@code super.explainTerms}, then call the {@link AlgWriterImpl#input(String, AlgNode)} and
     * {@link AlgWriterImpl#item(String, Object)} methods for each input and attribute.
     *
     * @param pw Plan writer
     * @return Plan writer for fluent-explain pattern
     */
    public AlgWriter explainTerms( AlgWriter pw ) {
        ModelTrait trait = getTraitSet().getTrait( ModelTraitDef.INSTANCE );
        if ( trait != null ) {
            return pw.item( "model", trait.dataModel().name() );
        }
        return pw;
    }


    @Override
    public void buildPolyAlgebra( StringBuilder sb, String prefix ) {
        final String INDENT = " ";
        String nextPrefix = prefix == null ? null : prefix + INDENT;
        boolean makeFieldsUnique = !(this instanceof SetOp); // set operations like UNION require duplicate field names
        List<String> inputFieldNames = makeFieldsUnique ?
                PolyAlgUtils.uniquifiedInputFieldNames( this ) :
                PolyAlgUtils.getInputFieldNamesList( this );

        PolyAlgDeclaration decl = getPolyAlgDeclaration();
        sb.append( prefix == null ? "" : prefix ).append( decl.opName );
        sb.append( collectAttributes().serializeArguments( this, inputFieldNames ) );

        int size = getInputs().size();
        if ( size == 0 ) {
            return;  // skip parentheses for leaves
        }

        sb.append( "(\n" );
        int inputIdx = 0;
        for ( AlgNode child : getInputs() ) {
            List<String> projections = makeFieldsUnique ?
                    PolyAlgUtils.getAuxProjections( child, inputFieldNames, inputIdx ) :
                    List.of();
            inputIdx += child.getTupleType().getFieldCount();

            if ( projections == null ) {
                child.buildPolyAlgebra( sb, nextPrefix );
            } else {
                sb.append( nextPrefix )
                        .append( PolyAlgRegistry.getDeclaration( LogicalRelProject.class ).opName ).append( "#" )  // TODO: select Project depending on data model, logical / physical
                        .append( PolyAlgUtils.joinMultiValuedWithBrackets( projections ) )
                        .append( "(\n" );
                child.buildPolyAlgebra( sb, nextPrefix == null ? null : nextPrefix + INDENT );
                sb.append( ")" );
            }

            size--;
            if ( size > 0 ) {
                sb.append( ", \n" );
            }
        }
        sb.append( ")" );
    }


    /**
     * If a declaration should be shared by multiple implementations,
     * this method must be redefined.
     * Otherwise, this implementation should cover most cases.
     *
     * @return The declaration associated with the runtime class of the instance.
     */
    @Override
    public PolyAlgDeclaration getPolyAlgDeclaration() {
        return PolyAlgRegistry.getDeclaration( getClass(), getModel(), getInputs().size() );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        // TODO: Move implementation from abstract class into child classes
        return new PolyAlgArgs( getPolyAlgDeclaration() );
    }


    @Override
    public AlgNode onRegister( AlgPlanner planner ) {
        List<AlgNode> oldInputs = getInputs();
        List<AlgNode> inputs = new ArrayList<>( oldInputs.size() );
        for ( final AlgNode input : oldInputs ) {
            AlgNode e = planner.ensureRegistered( input, null );
            assert e == input || AlgOptUtil.equal(
                    "tupletype of alg before registration",
                    input.getTupleType(),
                    "tupletype of alg after registration",
                    e.getTupleType(),
                    Litmus.THROW );
            inputs.add( e );
        }
        AlgNode r = this;
        if ( !Util.equalShallow( oldInputs, inputs ) ) {
            r = copy( getTraitSet(), inputs );
        }
        r.recomputeDigest();
        assert r.isValid( Litmus.THROW, null );
        return r;
    }


    @Override
    public String recomputeDigest() {
        String tempDigest = computeDigest();
        assert tempDigest != null : "computeDigest() should be non-null";

        this.desc = "alg#" + id + ":" + tempDigest;
        this.digest = tempDigest;
        return this.digest;
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode p ) {
        throw new UnsupportedOperationException( "replaceInput called on " + this );
    }


    public String toString() {
        return desc;
    }


    @Override
    public final String getDescription() {
        return desc;
    }


    @Override
    public Entity getEntity() {
        return null;
    }


    @Override
    public void replaceCluster( AlgCluster cluster ) {
        for ( AlgNode input : getInputs() ) {
            input.replaceCluster( cluster );
        }
        setCluster( cluster );
    }


    /**
     * Computes the digest. Does not modify this object.
     *
     * @return Digest
     */
    protected String computeDigest() {
        StringWriter sw = new StringWriter();
        AlgWriter pw =
                new AlgWriterImpl( new PrintWriter( sw ), ExplainLevel.DIGEST_ATTRIBUTES, false ) {
                    @Override
                    protected void explain_( AlgNode alg, List<Pair<String, Object>> values ) {
                        pw.write( getAlgTypeName() );

                        for ( AlgTrait<?> trait : traitSet ) {
                            pw.write( "." );
                            pw.write( trait.toString() );
                        }

                        pw.write( "(" );
                        int j = 0;
                        for ( Pair<String, Object> value : values ) {
                            if ( j++ > 0 ) {
                                pw.write( "," );
                            }
                            pw.write( value.left );
                            pw.write( "=" );
                            if ( value.right instanceof AlgNode input ) {
                                pw.write( input.getAlgTypeName() );
                                pw.write( "#" );
                                pw.write( Integer.toString( input.getId() ) );
                            } else {
                                pw.write( String.valueOf( value.right ) );
                            }
                        }
                        pw.write( ")" );
                    }
                };
        explain( pw );
        return sw.toString();
    }


    public static class AlgComparatorBuilder extends AlgShuttleImpl implements RexVisitor<String> {


        @Override
        public String visitIndexRef( RexIndexRef inputRef ) {
            return "$" + inputRef.getIndex();
        }


        @Override
        public String visitLocalRef( RexLocalRef localRef ) {
            return "l$" + localRef.getIndex();
        }


        @Override
        public String visitLiteral( RexLiteral literal ) {
            return literal.value == null ? "null" : literal.value.toJson();
        }


        @Override
        public String visitCall( RexCall call ) {
            return call.op.getName() + "(" + call.getOperands().stream().map( operand -> operand.accept( this ) ).reduce( ( s, s2 ) -> s + "," + s2 ).orElse( "" ) + ")";
        }


        @Override
        public String visitOver( RexOver over ) {
            return "$over";
        }


        @Override
        public String visitCorrelVariable( RexCorrelVariable correlVariable ) {
            return "$" + correlVariable.id;
        }


        @Override
        public String visitDynamicParam( RexDynamicParam dynamicParam ) {
            return "$" + dynamicParam.getIndex() + "(" + dynamicParam.getType().getFullTypeString() + ")";
        }


        @Override
        public String visitRangeRef( RexRangeRef rangeRef ) {
            return "$" + rangeRef.getOffset() + "[" + rangeRef.getOffset() + ".." + rangeRef.getOffset() + "]";
        }


        @Override
        public String visitFieldAccess( RexFieldAccess fieldAccess ) {
            return fieldAccess.toString();
        }


        @Override
        public String visitSubQuery( RexSubQuery subQuery ) {
            return "$subquery[" + subQuery.alg.accept( this ) + "]";
        }


        @Override
        public String visitTableInputRef( RexTableIndexRef fieldRef ) {
            return "$table" + fieldRef.getIndex();
        }


        @Override
        public String visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
            return "$pattern" + fieldRef.getIndex();
        }


        @Override
        public String visitNameRef( RexNameRef nameRef ) {
            return "$name" + nameRef.getName();
        }


        @Override
        public String visitElementRef( RexElementRef rexElementRef ) {
            return "$element(" + rexElementRef.getCollectionRef().accept( this ) + ")";
        }

    }


}

