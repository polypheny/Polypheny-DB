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
 */

package org.polypheny.db.algebra.core.relational;


import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Relational expression that modifies a table.
 *
 * It is similar to {@link RelScan}, but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *
 * <ul>
 * <li>For {@code INSERT}, those rows are the new values;
 * <li>for {@code DELETE}, the old values;
 * <li>for {@code UPDATE}, all old values plus updated new values.
 * </ul>
 */
public abstract class RelModify<E extends Entity> extends Modify<E> implements RelAlg {


    /**
     * The table definition.
     */
    @Getter
    private final Operation operation;
    @Getter
    private final List<String> updateColumns;
    @Getter
    private final List<? extends RexNode> sourceExpressions;
    private AlgDataType inputRowType;
    @Getter
    private final boolean flattened;


    /**
     * Creates a {@code Modify}.
     *
     * The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param table Target table to modify
     * @param input Sub-query or filter condition
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumns List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressions List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    protected RelModify(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            E table,
            AlgNode input,
            Operation operation,
            List<String> updateColumns,
            List<? extends RexNode> sourceExpressions,
            boolean flattened ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), table, input );
        this.operation = operation;
        this.updateColumns = updateColumns;
        this.sourceExpressions = sourceExpressions;
        if ( operation == Operation.UPDATE ) {
            Objects.requireNonNull( updateColumns );
            Objects.requireNonNull( sourceExpressions );
            Preconditions.checkArgument( sourceExpressions.size() == updateColumns.size() );
        } else {
            Preconditions.checkArgument( updateColumns == null );
            Preconditions.checkArgument( sourceExpressions == null );
        }
        this.flattened = flattened;
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
        final AlgDataType rowType = entity.getTupleType();
        switch ( operation ) {
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
                .item( "entity", entity.id )
                .item( "layer", entity.getLayer() )
                .item( "operation", getOperation() )
                .itemIf( "updateColumns", updateColumns, updateColumns != null )
                .itemIf( "sourceExpressions", sourceExpressions, sourceExpressions != null )
                .item( "flattened", flattened );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // REVIEW jvs: Just for now...
        double rowCount = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                "." + entity.id + "$" +
                (getInputs() != null ? getInputs().stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "$" : "") +
                getOperation().name() + "$" +
                (getUpdateColumns() != null ? getUpdateColumns().stream().map( c -> "c" ).collect( Collectors.joining( "$" ) ) + "$" : "") +
                (getSourceExpressions() != null ? getSourceExpressions().stream().map( RexNode::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                isFlattened() + "&";
    }


}

