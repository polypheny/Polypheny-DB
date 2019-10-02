/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.prepare.RelOptTableImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributions;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelReferentialConstraint;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
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

