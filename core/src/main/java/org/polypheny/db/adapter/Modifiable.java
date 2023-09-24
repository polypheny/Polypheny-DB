/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.types.ModifiableCollection;
import org.polypheny.db.schema.types.ModifiableGraph;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.tools.AlgBuilder;

public interface Modifiable extends Scannable {


    default AlgNode getModify( long allocId, Modify<?> modify, AlgBuilder builder ) {
        if ( modify.getEntity().unwrap( AllocationTable.class ) != null ) {
            return getRelModify( allocId, (RelModify<?>) modify, builder );
        } else if ( modify.getEntity().unwrap( AllocationCollection.class ) != null ) {
            return getDocModify( allocId, (DocumentModify<?>) modify, builder );
        } else if ( modify.getEntity().unwrap( AllocationGraph.class ) != null ) {
            return getGraphModify( allocId, (LpgModify<?>) modify, builder );
        }
        throw new NotImplementedException();
    }

    default AlgNode getRelModify( long allocId, RelModify<?> modify, AlgBuilder builder ) {
        PhysicalEntity table = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        if ( table.unwrap( ModifiableTable.class ) == null ) {
            return null;
        }
        return table.unwrap( ModifiableTable.class ).toModificationAlg(
                modify.getCluster(),
                modify.getTraitSet(),
                table,
                modify.getInput(),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList() );
    }

    default AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        if ( entity.unwrap( ModifiableCollection.class ) == null ) {
            return null;
        }
        return entity.unwrap( ModifiableCollection.class ).toModificationAlg(
                modify.getCluster(),
                modify.getTraitSet(),
                entity,
                modify.getInput(),
                modify.operation,
                modify.updates,
                modify.renames,
                modify.removes );
    }

    default AlgNode getGraphModify( long allocId, LpgModify<?> modify, AlgBuilder builder ) {
        PhysicalEntity entity = getCatalog().getPhysicalsFromAllocs( allocId ).get( 0 );
        if ( entity.unwrap( ModifiableGraph.class ) == null ) {
            return null;
        }
        return entity.unwrap( ModifiableGraph.class ).toModificationAlg(
                modify.getCluster(),
                modify.getTraitSet(),
                entity,
                modify.getInput(),
                modify.operation,
                modify.ids,
                modify.operations );
    }

    void addColumn( Context context, long allocId, LogicalColumn column );


    void dropColumn( Context context, long allocId, long columnId );

    default String addIndex( Context context, LogicalIndex logicalIndex, List<AllocationTable> allocations ) {
        return allocations.stream().map( a -> addIndex( context, logicalIndex, a ) ).collect( Collectors.toList() ).get( 0 );
    }

    String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation );

    default void dropIndex( Context context, LogicalIndex logicalIndex, List<Long> allocIds ) {
        for ( Long allocId : allocIds ) {
            dropIndex( context, logicalIndex, allocId );
        }
    }

    void dropIndex( Context context, LogicalIndex logicalIndex, long allocId );

    void updateColumnType( Context context, long allocId, LogicalColumn column );


}
