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

package org.polypheny.db.plan;


import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgDistributions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Partial implementation of {@link AlgOptTable}.
 */
public abstract class AlgOptAbstractTable implements AlgOptTable {

    protected final AlgOptSchema schema;
    protected final AlgDataType rowType;
    protected final String name;


    protected AlgOptAbstractTable( AlgOptSchema schema, String name, AlgDataType rowType ) {
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
    public AlgDataType getRowType() {
        return rowType;
    }


    @Override
    public AlgOptSchema getRelOptSchema() {
        return schema;
    }


    // Override to define collations.
    @Override
    public List<AlgCollation> getCollationList() {
        return Collections.emptyList();
    }


    @Override
    public AlgDistribution getDistribution() {
        return AlgDistributions.BROADCAST_DISTRIBUTED;
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
    public List<AlgReferentialConstraint> getReferentialConstraints() {
        return Collections.emptyList();
    }


    @Override
    public AlgNode toAlg( ToAlgContext context ) {
        return LogicalScan.create( context.getCluster(), this );
    }


    @Override
    public Expression getExpression( Class clazz ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public AlgOptTable extend( List<AlgDataTypeField> extendedFields ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return AlgOptTableImpl.columnStrategies( this );
    }

}

