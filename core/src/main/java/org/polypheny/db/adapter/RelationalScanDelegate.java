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
import lombok.AllArgsConstructor;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.catalogs.RelStoreCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Pair;

@AllArgsConstructor
public class RelationalScanDelegate implements Scannable {

    public final Scannable scannable;

    public final RelStoreCatalog catalog;


    @Override
    public AlgNode getRelScan( long allocId, AlgBuilder builder ) {
        Pair<AllocationEntity, List<Long>> relations = catalog.getAllocRelations().get( allocId );
        return builder.scan( catalog.getTable( relations.right.get( 0 ) ) ).build();
    }


    @Override
    public AlgNode getGraphScan( long allocId, AlgBuilder builder ) {
        builder.clear();
        PhysicalTable node = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
        PhysicalTable nProps = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 1 ) );
        PhysicalTable edge = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 2 ) );
        PhysicalTable eProps = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 3 ) );

        builder.scan( node );
        builder.scan( nProps );
        builder.scan( edge );
        builder.scan( eProps );

        builder.transform( ModelTrait.GRAPH, GraphType.of(), false );

        return builder.build();
    }


    @Override
    public AlgNode getDocumentScan( long allocId, AlgBuilder builder ) {
        builder.clear();
        PhysicalTable table = catalog.getTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
        builder.scan( table );
        AlgDataType rowType = DocumentType.ofId();
        builder.transform( ModelTrait.DOCUMENT, rowType, false );
        return builder.build();
    }


    @Override
    public AlgNode getScan( long allocId, AlgBuilder builder ) {
        Pair<AllocationEntity, List<Long>> alloc = catalog.getAllocRelations().get( allocId );
        if ( alloc.left.unwrap( AllocationTable.class ) != null ) {
            return getRelScan( allocId, builder );
        } else if ( alloc.left.unwrap( AllocationCollection.class ) != null ) {
            return getDocumentScan( allocId, builder );
        } else if ( alloc.left.unwrap( AllocationGraph.class ) != null ) {
            return getGraphScan( allocId, builder );
        } else {
            throw new GenericRuntimeException( "This should not happen" );
        }
    }


    @Override
    public void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        scannable.createTable( context, logical, allocation );
    }


    @Override
    public void refreshTable( long allocId ) {
        scannable.refreshTable( allocId );
    }


    @Override
    public void refreshGraph( long allocId ) {
        scannable.refreshTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
        scannable.refreshTable( catalog.getAllocRelations().get( allocId ).getValue().get( 1 ) );
        scannable.refreshTable( catalog.getAllocRelations().get( allocId ).getValue().get( 2 ) );
        scannable.refreshTable( catalog.getAllocRelations().get( allocId ).getValue().get( 3 ) );
    }


    @Override
    public void refreshCollection( long allocId ) {
        scannable.refreshTable( catalog.getAllocRelations().get( allocId ).getValue().get( 0 ) );
    }

}
