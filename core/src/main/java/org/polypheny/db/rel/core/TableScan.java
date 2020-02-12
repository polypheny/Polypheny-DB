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
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Relational operator that returns the contents of a table.
 */
public abstract class TableScan extends AbstractRelNode {

    /**
     * The table definition.
     */
    protected final RelOptTable table;


    protected TableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table ) {
        super( cluster, traitSet );
        this.table = table;
        if ( table.getRelOptSchema() != null ) {
            cluster.getPlanner().registerSchema( table.getRelOptSchema() );
        }
    }


    /**
     * Creates a TableScan by parsing serialized output.
     */
    protected TableScan( RelInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getTable( "table" ) );
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return table.getRowCount();
    }


    @Override
    public RelOptTable getTable() {
        return table;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double dRows = table.getRowCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public RelDataType deriveRowType() {
        return table.getRowType();
    }


    /**
     * Returns an identity projection for the given table.
     */
    public static ImmutableIntList identity( RelOptTable table ) {
        return ImmutableIntList.identity( table.getRowType().getFieldCount() );
    }


    /**
     * Returns an identity projection.
     */
    public ImmutableIntList identity() {
        return identity( table );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).item( "table", table.getQualifiedName() );
    }


    /**
     * Projects a subset of the fields of the table, and also asks for "extra" fields that were not included in the table's official type.
     *
     * The default implementation assumes that tables cannot do either of these operations, therefore it adds a {@link Project} that projects {@code NULL} values for the extra fields,
     * using the {@link RelBuilder#project(Iterable)} method.
     *
     * Sub-classes, representing table types that have these capabilities, should override.
     *
     * @param fieldsUsed Bitmap of the fields desired by the consumer
     * @param extraFields Extra fields, not advertised in the table's row-type, wanted by the consumer
     * @param relBuilder Builder used to create a Project
     * @return Relational expression that projects the desired fields
     */
    public RelNode project( ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields, RelBuilder relBuilder ) {
        final int fieldCount = getRowType().getFieldCount();
        if ( fieldsUsed.equals( ImmutableBitSet.range( fieldCount ) ) && extraFields.isEmpty() ) {
            return this;
        }
        final List<RexNode> exprList = new ArrayList<>();
        final List<String> nameList = new ArrayList<>();
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final List<RelDataTypeField> fields = getRowType().getFieldList();

        // Project the subset of fields.
        for ( int i : fieldsUsed ) {
            RelDataTypeField field = fields.get( i );
            exprList.add( rexBuilder.makeInputRef( this, i ) );
            nameList.add( field.getName() );
        }

        // Project nulls for the extra fields. (Maybe a sub-class table has extra fields, but we don't.)
        for ( RelDataTypeField extraField : extraFields ) {
            exprList.add( rexBuilder.ensureType( extraField.getType(), rexBuilder.constantNull(), true ) );
            nameList.add( extraField.getName() );
        }

        return relBuilder.push( this ).project( exprList, nameList ).build();
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}
