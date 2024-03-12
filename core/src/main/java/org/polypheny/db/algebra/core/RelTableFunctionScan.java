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

package org.polypheny.db.algebra.core;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.metadata.AlgColumnMapping;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;


/**
 * Relational expression that calls a table-valued function.
 *
 * The function returns a result set.
 * It can appear as a leaf in a query tree, or can be applied to relational inputs.
 *
 * @see LogicalRelTableFunctionScan
 */
public abstract class RelTableFunctionScan extends AbstractAlgNode {

    private final RexNode rexCall;

    private final Type elementType;

    private ImmutableList<AlgNode> inputs;

    protected final ImmutableSet<AlgColumnMapping> columnMappings;


    /**
     * Creates a <code>TableFunctionScan</code>.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param inputs 0 or more relational inputs
     * @param rexCall Function invocation expression
     * @param elementType Element type of the collection that will implement this table
     * @param rowType Row type produced by function
     * @param columnMappings Column mappings associated with this function
     */
    protected RelTableFunctionScan( AlgCluster cluster, AlgTraitSet traits, List<AlgNode> inputs, RexNode rexCall, Type elementType, AlgDataType rowType, Set<AlgColumnMapping> columnMappings ) {
        super( cluster, traits );
        this.rexCall = rexCall;
        this.elementType = elementType;
        this.rowType = rowType;
        this.inputs = ImmutableList.copyOf( inputs );
        this.columnMappings = columnMappings == null ? null : ImmutableSet.copyOf( columnMappings );
    }


    @Override
    public final RelTableFunctionScan copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    /**
     * Copies this relational expression, substituting traits and inputs.
     *
     * @param traitSet Traits
     * @param inputs 0 or more relational inputs
     * @param rexCall Function invocation expression
     * @param elementType Element type of the collection that will implement this table
     * @param rowType Row type produced by function
     * @param columnMappings Column mappings associated with this function
     * @return Copy of this relational expression, substituting traits and inputs
     */
    public abstract RelTableFunctionScan copy( AlgTraitSet traitSet, List<AlgNode> inputs, RexNode rexCall, Type elementType, AlgDataType rowType, Set<AlgColumnMapping> columnMappings );


    @Override
    public List<AlgNode> getInputs() {
        return inputs;
    }


    @Override
    public List<RexNode> getChildExps() {
        return ImmutableList.of( rexCall );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        RexNode rexCall = shuttle.apply( this.rexCall );
        if ( rexCall == this.rexCall ) {
            return this;
        }
        return copy( traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode p ) {
        final List<AlgNode> newInputs = new ArrayList<>( inputs );
        newInputs.set( ordinalInParent, p );
        inputs = ImmutableList.copyOf( newInputs );
        recomputeDigest();
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        // Calculate result as the sum of the input row count estimates, assuming there are any, otherwise use the superclass default. So for a no-input UDX, behave like an AbstractAlgNode;
        // for a one-input UDX, behave like a SingleRel; for a multi-input UDX, behave like UNION ALL.
        // TODO jvs 10-Sep-2007: UDX-supplied costing metadata.
        if ( inputs.size() == 0 ) {
            return super.estimateTupleCount( mq );
        }
        double nRows = 0.0;
        for ( AlgNode input : inputs ) {
            Double d = mq.getTupleCount( input );
            if ( d != null ) {
                nRows += d;
            }
        }
        return nRows;
    }


    /**
     * Returns function invocation expression.
     *
     * Within this rexCall, instances of {@link RexIndexRef} refer to entire input {@link AlgNode}s rather than their fields.
     *
     * @return function invocation expression
     */
    public RexNode getCall() {
        return rexCall;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        super.explainTerms( pw );
        for ( Ord<AlgNode> ord : Ord.zip( inputs ) ) {
            pw.input( "input#" + ord.i, ord.e );
        }
        pw.item( "invocation", rexCall ).item( "rowType", rowType );
        if ( elementType != null ) {
            pw.item( "elementType", elementType );
        }
        return pw;
    }


    /**
     * Returns set of mappings known for this table function, or null if unknown (not the same as empty!).
     *
     * @return set of mappings known for this table function, or null if unknown (not the same as empty!)
     */
    public Set<AlgColumnMapping> getColumnMappings() {
        return columnMappings;
    }


    /**
     * Returns element type of the collection that will implement this table.
     *
     * @return element type of the collection that will implement this table
     */
    public Type getElementType() {
        return elementType;
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                getInputs().stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "$" +
                (getCall() != null ? getCall().hashCode() : "") + "$" +
                rowType.toString() + "&";
    }

}

