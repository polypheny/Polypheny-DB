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

import lombok.Getter;
import org.polypheny.db.catalog.catalogs.GraphStoreCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.prepare.Context;

public class GraphScanDelegate implements Scannable {

    protected final Scannable scannable;
    @Getter
    protected final GraphStoreCatalog catalog;


    public GraphScanDelegate( Scannable scannable, GraphStoreCatalog catalog ) {
        this.scannable = scannable;
        this.catalog = catalog;
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
        scannable.refreshGraph( allocId );
    }


    @Override
    public void refreshCollection( long allocId ) {
        Scannable.refreshCollectionSubstitution( scannable, allocId );
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        scannable.dropTable( context, allocId );
    }


    @Override
    public void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
        scannable.createGraph( context, logical, allocation );
    }


    @Override
    public void dropGraph( Context context, AllocationGraph allocation ) {
        scannable.dropGraph( context, allocation );
    }


    @Override
    public void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
        Scannable.createCollectionSubstitute( scannable, context, logical, allocation );
    }


    @Override
    public void dropCollection( Context context, AllocationCollection allocation ) {
        Scannable.dropCollectionSubstitute( scannable, context, allocation );
    }

}
