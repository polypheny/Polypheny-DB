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

import org.polypheny.db.catalog.catalogs.GraphAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;

public class GraphModifyDelegate extends GraphScanDelegate implements Modifiable {


    private final Modifiable modifiable;


    public GraphModifyDelegate( Modifiable modifiable, GraphAdapterCatalog catalog ) {
        super( modifiable, catalog );
        this.modifiable = modifiable;
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn column ) {
        throw new GenericRuntimeException( "Should be overwritten." );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        throw new GenericRuntimeException( "Should be overwritten." );
    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        throw new GenericRuntimeException( "Should be overwritten." );
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {
        throw new GenericRuntimeException( "Should be overwritten." );
    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn column ) {
        throw new GenericRuntimeException( "Should be overwritten." );
    }


}
