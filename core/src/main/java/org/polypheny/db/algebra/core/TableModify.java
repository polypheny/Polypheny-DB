/*
 * Copyright 2019-2021 The Polypheny Project
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


import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Relational expression that modifies a table.
 *
 * It is similar to {@link org.polypheny.db.algebra.core.TableScan}, but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *
 * <ul>
 * <li>For {@code INSERT}, those rows are the new values;
 * <li>for {@code DELETE}, the old values;
 * <li>for {@code UPDATE}, all old values plus updated new values.
 * </ul>
 */
public abstract class TableModify extends SingleAlg {

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
    @Getter
    protected final AlgOptTable table;
    @Getter
    private final Operation operation;
    @Getter
    private final List<String> updateColumnList;
    @Getter
    private final List<RexNode> sourceExpressionList;
    private AlgDataType inputRowType;
    @Getter
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
    protected TableModify( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, Prepare.CatalogReader catalogReader, AlgNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
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
    public AlgDataType deriveRowType() {
        return AlgOptUtil.createDmlRowType( Kind.INSERT, getCluster().getTypeFactory() );
    }


    @Override
    public AlgDataType getExpectedInputRowType( int ordinalInParent ) {
        assert ordinalInParent == 0;

        if ( inputRowType != null ) {
            return inputRowType;
        }

        final AlgDataTypeFactory typeFactory = getCluster().getTypeFactory();
        final AlgDataType rowType = table.getRowType();
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
            inputRowType = PolyTypeUtil.flattenRecordType( typeFactory, inputRowType, null );
        }

        return inputRowType;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "table", table.getQualifiedName() )
                .item( "operation", getOperation() )
                .itemIf( "updateColumnList", updateColumnList, updateColumnList != null )
                .itemIf( "sourceExpressionList", sourceExpressionList, sourceExpressionList != null )
                .item( "flattened", flattened );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        // REVIEW jvs: Just for now...
        double rowCount = mq.getRowCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                String.join( ".", table.getQualifiedName() ) + "$" +
                (getInputs() != null ? getInputs().stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "$" : "") +
                getOperation().name() + "$" +
                (getUpdateColumnList() != null ? String.join( "$", getUpdateColumnList() ) + "$" : "") +
                (getSourceExpressionList() != null ? getSourceExpressionList().stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                isFlattened() + "&";
    }

}

