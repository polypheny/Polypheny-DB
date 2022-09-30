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

package org.polypheny.db.algebra.core;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;


/**
 * Relational operator that returns the contents of a table.
 */
public abstract class Scan extends AbstractAlgNode {

    /**
     * The table definition.
     */
    protected final AlgOptTable table;


    protected Scan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table ) {
        super( cluster, traitSet );
        this.table = table;
        if ( table.getRelOptSchema() != null ) {
            cluster.getPlanner().registerSchema( table.getRelOptSchema() );
        }
    }


    /**
     * Creates a Scan by parsing serialized output.
     */
    protected Scan( AlgInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getTable( "table" ) );
    }


    @Override
    public double estimateRowCount( AlgMetadataQuery mq ) {
        return table.getRowCount();
    }


    @Override
    public AlgOptTable getTable() {
        return table;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        double dRows = table.getRowCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public AlgDataType deriveRowType() {
        return table.getRowType();
    }


    /**
     * Returns an identity projection for the given table.
     */
    public static ImmutableIntList identity( AlgOptTable table ) {
        return ImmutableIntList.identity( table.getRowType().getFieldCount() );
    }


    /**
     * Returns an identity projection.
     */
    public ImmutableIntList identity() {
        return identity( table );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "table", table.getQualifiedName() );
    }


    /**
     * Projects a subset of the fields of the table, and also asks for "extra" fields that were not included in the table's official type.
     *
     * The default implementation assumes that tables cannot do either of these operations, therefore it adds a {@link Project} that projects {@code NULL} values for the extra fields,
     * using the {@link AlgBuilder#project(Iterable)} method.
     *
     * Sub-classes, representing table types that have these capabilities, should override.
     *
     * @param fieldsUsed Bitmap of the fields desired by the consumer
     * @param extraFields Extra fields, not advertised in the table's row-type, wanted by the consumer
     * @param algBuilder Builder used to create a Project
     * @return Relational expression that projects the desired fields
     */
    public AlgNode project( ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields, AlgBuilder algBuilder ) {
        final int fieldCount = getRowType().getFieldCount();
        if ( fieldsUsed.equals( ImmutableBitSet.range( fieldCount ) ) && extraFields.isEmpty() ) {
            return this;
        }
        final List<RexNode> exprList = new ArrayList<>();
        final List<String> nameList = new ArrayList<>();
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final List<AlgDataTypeField> fields = getRowType().getFieldList();

        // Project the subset of fields.
        for ( int i : fieldsUsed ) {
            AlgDataTypeField field = fields.get( i );
            exprList.add( rexBuilder.makeInputRef( this, i ) );
            nameList.add( field.getName() );
        }

        // Project nulls for the extra fields. (Maybe a sub-class table has extra fields, but we don't.)
        for ( AlgDataTypeField extraField : extraFields ) {
            exprList.add( rexBuilder.ensureType( extraField.getType(), rexBuilder.constantNull(), true ) );
            nameList.add( extraField.getName() );
        }

        return algBuilder.push( this ).project( exprList, nameList ).build();
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                String.join( ".", table.getQualifiedName() ) + "&";
    }

}
