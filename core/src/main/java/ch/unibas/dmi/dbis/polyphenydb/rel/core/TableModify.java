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

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeUtil;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;


/**
 * Relational expression that modifies a table.
 *
 * It is similar to {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan}, but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *
 * <ul>
 * <li>For {@code INSERT}, those rows are the new values;
 * <li>for {@code DELETE}, the old values;
 * <li>for {@code UPDATE}, all old values plus updated new values.
 * </ul>
 */
public abstract class TableModify extends SingleRel {

    /**
     * Enumeration of supported modification operations.
     */
    public enum Operation {
        INSERT, UPDATE, DELETE, MERGE
    }


    /**
     * The connection to the optimizing session.
     */
    protected Prepare.CatalogReader catalogReader;

    /**
     * The table definition.
     */
    protected final RelOptTable table;
    private final Operation operation;
    private final List<String> updateColumnList;
    private final List<RexNode> sourceExpressionList;
    private RelDataType inputRowType;
    private final boolean flattened;


    /**
     * Creates a {@code TableModify}.
     *
     * The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param table Target table to modify
     * @param catalogReader accessor to the table metadata.
     * @param input Sub-query or filter condition
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    protected TableModify( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traitSet, input );
        this.table = table;
        this.catalogReader = catalogReader;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
        if ( operation == Operation.UPDATE ) {
            Objects.requireNonNull( updateColumnList );
            Objects.requireNonNull( sourceExpressionList );
            Preconditions.checkArgument( sourceExpressionList.size() == updateColumnList.size() );
        } else {
            Preconditions.checkArgument( updateColumnList == null );
            Preconditions.checkArgument( sourceExpressionList == null );
        }
        if ( table.getRelOptSchema() != null ) {
            cluster.getPlanner().registerSchema( table.getRelOptSchema() );
        }
        this.flattened = flattened;
    }


    public Prepare.CatalogReader getCatalogReader() {
        return catalogReader;
    }


    @Override
    public RelOptTable getTable() {
        return table;
    }


    public List<String> getUpdateColumnList() {
        return updateColumnList;
    }


    public List<RexNode> getSourceExpressionList() {
        return sourceExpressionList;
    }


    public boolean isFlattened() {
        return flattened;
    }


    public Operation getOperation() {
        return operation;
    }


    public boolean isInsert() {
        return operation == Operation.INSERT;
    }


    public boolean isUpdate() {
        return operation == Operation.UPDATE;
    }


    public boolean isDelete() {
        return operation == Operation.DELETE;
    }


    public boolean isMerge() {
        return operation == Operation.MERGE;
    }


    @Override
    public RelDataType deriveRowType() {
        return RelOptUtil.createDmlRowType( SqlKind.INSERT, getCluster().getTypeFactory() );
    }


    @Override
    public RelDataType getExpectedInputRowType( int ordinalInParent ) {
        assert ordinalInParent == 0;

        if ( inputRowType != null ) {
            return inputRowType;
        }

        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        final RelDataType rowType = table.getRowType();
        switch ( operation ) {
            case UPDATE:
                inputRowType =
                        typeFactory.createJoinType(
                                rowType,
                                getCatalogReader().createTypeFromProjection( rowType, updateColumnList ) );
                break;
            case MERGE:
                inputRowType =
                        typeFactory.createJoinType(
                                typeFactory.createJoinType( rowType, rowType ),
                                getCatalogReader().createTypeFromProjection( rowType, updateColumnList ) );
                break;
            default:
                inputRowType = rowType;
                break;
        }

        if ( flattened ) {
            inputRowType = SqlTypeUtil.flattenRecordType( typeFactory, inputRowType, null );
        }

        return inputRowType;
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw )
                .item( "table", table.getQualifiedName() )
                .item( "operation", getOperation() )
                .itemIf( "updateColumnList", updateColumnList, updateColumnList != null )
                .itemIf( "sourceExpressionList", sourceExpressionList, sourceExpressionList != null )
                .item( "flattened", flattened );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // REVIEW jvs 21-Apr-2006:  Just for now...
        double rowCount = mq.getRowCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }
}

