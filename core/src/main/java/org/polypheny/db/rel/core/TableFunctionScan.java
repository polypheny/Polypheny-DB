/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableFunctionScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelColumnMapping;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexShuttle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;


/**
 * Relational expression that calls a table-valued function.
 *
 * The function returns a result set.
 * It can appear as a leaf in a query tree, or can be applied to relational inputs.
 *
 * @see LogicalTableFunctionScan
 */
public abstract class TableFunctionScan extends AbstractRelNode {

    private final RexNode rexCall;

    private final Type elementType;

    private ImmutableList<RelNode> inputs;

    protected final ImmutableSet<RelColumnMapping> columnMappings;


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
    protected TableFunctionScan( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings ) {
        super( cluster, traits );
        this.rexCall = rexCall;
        this.elementType = elementType;
        this.rowType = rowType;
        this.inputs = ImmutableList.copyOf( inputs );
        this.columnMappings = columnMappings == null ? null : ImmutableSet.copyOf( columnMappings );
    }


    /**
     * Creates a TableFunctionScan by parsing serialized output.
     */
    protected TableFunctionScan( RelInput input ) {
        this(
                input.getCluster(),
                input.getTraitSet(),
                input.getInputs(),
                input.getExpression( "invocation" ),
                (Type) input.get( "elementType" ),
                input.getRowType( "rowType" ),
                ImmutableSet.of() );
    }


    @Override
    public final TableFunctionScan copy( RelTraitSet traitSet, List<RelNode> inputs ) {
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
    public abstract TableFunctionScan copy( RelTraitSet traitSet, List<RelNode> inputs, RexNode rexCall, Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings );


    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }


    @Override
    public List<RexNode> getChildExps() {
        return ImmutableList.of( rexCall );
    }


    @Override
    public RelNode accept( RexShuttle shuttle ) {
        RexNode rexCall = shuttle.apply( this.rexCall );
        if ( rexCall == this.rexCall ) {
            return this;
        }
        return copy( traitSet, inputs, rexCall, elementType, rowType, columnMappings );
    }


    @Override
    public void replaceInput( int ordinalInParent, RelNode p ) {
        final List<RelNode> newInputs = new ArrayList<>( inputs );
        newInputs.set( ordinalInParent, p );
        inputs = ImmutableList.copyOf( newInputs );
        recomputeDigest();
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        // Calculate result as the sum of the input row count estimates, assuming there are any, otherwise use the superclass default. So for a no-input UDX, behave like an AbstractRelNode;
        // for a one-input UDX, behave like a SingleRel; for a multi-input UDX, behave like UNION ALL.
        // TODO jvs 10-Sep-2007: UDX-supplied costing metadata.
        if ( inputs.size() == 0 ) {
            return super.estimateRowCount( mq );
        }
        double nRows = 0.0;
        for ( RelNode input : inputs ) {
            Double d = mq.getRowCount( input );
            if ( d != null ) {
                nRows += d;
            }
        }
        return nRows;
    }


    /**
     * Returns function invocation expression.
     *
     * Within this rexCall, instances of {@link RexInputRef} refer to entire input {@link RelNode}s rather than their fields.
     *
     * @return function invocation expression
     */
    public RexNode getCall() {
        return rexCall;
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        super.explainTerms( pw );
        for ( Ord<RelNode> ord : Ord.zip( inputs ) ) {
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
    public Set<RelColumnMapping> getColumnMappings() {
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
}

