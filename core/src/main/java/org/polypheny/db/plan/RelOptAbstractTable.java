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

package org.polypheny.db.plan;


import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelDistribution;
import org.polypheny.db.rel.RelDistributions;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelReferentialConstraint;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Partial implementation of {@link RelOptTable}.
 */
public abstract class RelOptAbstractTable implements RelOptTable {

    protected final RelOptSchema schema;
    protected final RelDataType rowType;
    protected final String name;


    protected RelOptAbstractTable( RelOptSchema schema, String name, RelDataType rowType ) {
        this.schema = schema;
        this.name = name;
        this.rowType = rowType;
    }


    public String getName() {
        return name;
    }


    @Override
    public List<String> getQualifiedName() {
        return ImmutableList.of( name );
    }


    @Override
    public double getRowCount() {
        return 100;
    }


    @Override
    public RelDataType getRowType() {
        return rowType;
    }


    @Override
    public RelOptSchema getRelOptSchema() {
        return schema;
    }


    // Override to define collations.
    @Override
    public List<RelCollation> getCollationList() {
        return Collections.emptyList();
    }


    @Override
    public RelDistribution getDistribution() {
        return RelDistributions.BROADCAST_DISTRIBUTED;
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        return clazz.isInstance( this )
                ? clazz.cast( this )
                : null;
    }


    // Override to define keys
    @Override
    public boolean isKey( ImmutableBitSet columns ) {
        return false;
    }


    // Override to define foreign keys
    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return Collections.emptyList();
    }


    @Override
    public RelNode toRel( ToRelContext context ) {
        return LogicalTableScan.create( context.getCluster(), this );
    }


    @Override
    public Expression getExpression( Class clazz ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public RelOptTable extend( List<RelDataTypeField> extendedFields ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return RelOptTableImpl.columnStrategies( this );
    }

}

