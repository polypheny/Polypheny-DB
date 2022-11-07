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
 */

package org.polypheny.db.adapter.neo4j.rules.relational;

import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;

public class NeoModify extends Modify implements NeoRelAlg {

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
     * @param catalogReader accessor to the table metadata.
     * @param input Sub-query or filter condition
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    public NeoModify( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, CatalogReader catalogReader, AlgNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public void implement( NeoRelationalImplementor implementor ) {
        assert getTable() != null;
        implementor.setTable( getTable() );
        implementor.setDml( true );

        implementor.visitChild( 0, getInput() );

        switch ( getOperation() ) {
            case INSERT:
                handleInsert( implementor );
                break;
            case UPDATE:
                handleUpdate( implementor );
                break;
            case DELETE:
                handleDelete( implementor );
                break;
            case MERGE:
                throw new UnsupportedOperationException( "Merge is not supported" );
        }
    }


    private void handleDelete( NeoRelationalImplementor implementor ) {
        implementor.addDelete();
    }


    private void handleUpdate( NeoRelationalImplementor implementor ) {
        implementor.addUpdate( this );
    }


    private void handleInsert( NeoRelationalImplementor implementor ) {
        // insert
        if ( !implementor.isPrepared() ) {
            implementor.addCreate();
        } else {
            implementor.addPreparedValues();
        }
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoModify(
                inputs.get( 0 ).getCluster(),
                traitSet,
                table,
                catalogReader,
                inputs.get( 0 ),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }

}
