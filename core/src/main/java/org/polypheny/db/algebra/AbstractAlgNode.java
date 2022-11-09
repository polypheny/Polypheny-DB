/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.externalize.AlgWriterImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.Metadata;
import org.polypheny.db.algebra.metadata.MetadataFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.ModelTraitDef;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Base class for every relational expression ({@link AlgNode}).
 */
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
     * A short description of this relational expression's type, inputs, and other properties. The string uniquely identifies
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
    private transient AlgOptCluster cluster;

    /**
     * unique id of this object -- for debugging
     */
    @Getter
    protected final int id;

    /**
     * The RelTraitSet that describes the traits of this AlgNode.
     * Setter is used to set the cluster in Views
     */
    @Setter
    @Getter
    protected AlgTraitSet traitSet;


    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public AbstractAlgNode( AlgOptCluster cluster, AlgTraitSet traitSet ) {
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
        // Note that empty set equals empty set, so relational expressions with zero inputs do not generally need to implement their own copy method.
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
    public void register( AlgOptPlanner planner ) {
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
    public final AlgDataType getRowType() {
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
        return getRowType();
    }


    @Override
    public List<AlgNode> getInputs() {
        return Collections.emptyList();
    }


    @Override
    public double estimateRowCount( AlgMetadataQuery mq ) {
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
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        // by default, assume cost is proportional to number of rows
        double rowCount = mq.getRowCount( this );
        double bytesPerRow = 1;
        return planner.getCostFactory().makeCost( rowCount, rowCount, 0 );
    }


    @Override
    public final <M extends Metadata> M metadata( Class<M> metadataClass, AlgMetadataQuery mq ) {
        final MetadataFactory factory = cluster.getMetadataFactory();
        final M metadata = factory.query( this, mq, metadataClass );
        assert metadata != null : "no provider found (rel=" + this + ", m=" + metadataClass + "); a backstop provider is recommended";
        // Usually the metadata belongs to the alg that created it. RelSubset and HepAlgVertex are notable exceptions, so
        // disable the assert. It's not worth the performance hit to override this method for them.
        //   assert metadata.rel() == this : "someone else's metadata";
        return metadata;
    }


    @Override
    public void explain( AlgWriter pw ) {
        explainTerms( pw ).done( this );
    }


    /**
     * Describes the inputs and attributes of this relational expression.
     * Each node should call {@code super.explainTerms}, then call the {@link AlgWriterImpl#input(String, AlgNode)} and
     * {@link AlgWriterImpl#item(String, Object)} methods for each input and attribute.
     *
     * @param pw Plan writer
     * @return Plan writer for fluent-explain pattern
     */
    public AlgWriter explainTerms( AlgWriter pw ) {
        ModelTrait trait = getTraitSet().getTrait( ModelTraitDef.INSTANCE );
        if ( trait != null ) {
            return pw.item( "model", trait.getDataModel().name() );
        }
        return pw;
    }


    @Override
    public AlgNode onRegister( AlgOptPlanner planner ) {
        List<AlgNode> oldInputs = getInputs();
        List<AlgNode> inputs = new ArrayList<>( oldInputs.size() );
        for ( final AlgNode input : oldInputs ) {
            AlgNode e = planner.ensureRegistered( input, null );
            if ( e != input ) {
                // TODO: change 'equal' to 'eq', which is stronger.
                assert AlgOptUtil.equal(
                        "rowtype of alg before registration",
                        input.getRowType(),
                        "rowtype of alg after registration",
                        e.getRowType(),
                        Litmus.THROW );
            }
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
    public AlgOptTable getTable() {
        return null;
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

                        for ( AlgTrait trait : traitSet ) {
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
                            if ( value.right instanceof AlgNode ) {
                                AlgNode input = (AlgNode) value.right;
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

}

