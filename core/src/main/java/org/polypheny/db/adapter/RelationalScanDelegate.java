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

package org.polypheny.db.adapter;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;

@AllArgsConstructor
public class RelationalScanDelegate implements Scannable {

    public final Scannable scannable;

    @Getter
    public final RelAdapterCatalog catalog;


    @Override
    public AlgNode getGraphScan( long allocId, AlgBuilder builder ) {
        return Scannable.getGraphScanSubstitute( scannable, allocId, builder );
    }


    @Override
    public AlgNode getDocumentScan( long allocId, AlgBuilder builder ) {
        return Scannable.getDocumentScanSubstitute( scannable, allocId, builder );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        return scannable.createTable( context, logical, allocation );
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        scannable.restoreTable( alloc, entities, context );
    }


    @Override
    public void restoreGraph( AllocationGraph alloc, List<PhysicalEntity> entities, Context context ) {
        Scannable.restoreGraphSubstitute( scannable, alloc, entities, context );
    }


    @Override
    public void restoreCollection( AllocationCollection alloc, List<PhysicalEntity> entities, Context context ) {
        Scannable.restoreCollectionSubstitute( scannable, alloc, entities, context );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        scannable.dropTable( context, allocId );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        return Scannable.createGraphSubstitute( scannable, context, logical, allocation );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        Scannable.dropGraphSubstitute( scannable, context, allocation );
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        return Scannable.createCollectionSubstitute( scannable, context, logical, allocation );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        Scannable.dropCollectionSubstitute( scannable, context, allocation );
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        scannable.renameLogicalColumn( id, newColumnName );
    }

}
