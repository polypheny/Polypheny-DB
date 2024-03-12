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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;

public class RelationalModifyDelegate extends RelationalScanDelegate implements Modifiable {


    private final Modifiable modifiable;


    public RelationalModifyDelegate( Modifiable modifiable, RelAdapterCatalog catalog ) {
        super( modifiable, catalog );
        this.modifiable = modifiable;
    }


    @Override
    public AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        return Modifiable.getDocModifySubstitute( modifiable, allocId, modify, builder );
    }


    @Override
    public AlgNode getGraphModify( long allocId, LpgModify<?> alg, AlgBuilder builder ) {
        return Modifiable.getGraphModifySubstitute( modifiable, allocId, alg, builder );
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn column ) {
        modifiable.addColumn( context, allocId, column );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        modifiable.dropColumn( context, allocId, columnId );
    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        return modifiable.addIndex( context, index, allocation );
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {
        modifiable.dropIndex( context, index, allocId );
    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn column ) {
        modifiable.updateColumnType( context, allocId, column );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        return modifiable.createTable( context, logical, allocation );
    }


    @Override
    public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        return Scannable.createGraphSubstitute( modifiable, context, logical, allocation );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        Modifiable.dropGraphSubstitute( modifiable, allocation.id );
    }


    @Override
    public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        PhysicalTable physical = Scannable.createSubstitutionTable( modifiable, context, logical, allocation, "_doc_", List.of(
                new ColumnContext( DocumentType.DOCUMENT_ID, null, PolyType.TEXT, false ),
                new ColumnContext( DocumentType.DOCUMENT_DATA, null, PolyType.TEXT, true ) ), 1 );
        catalog.addPhysical( allocation, physical );

        return List.of( physical );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        Scannable.dropCollectionSubstitute( modifiable, context, allocation );
    }



}
