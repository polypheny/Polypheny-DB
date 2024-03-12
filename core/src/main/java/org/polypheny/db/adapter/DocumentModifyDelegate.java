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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;

public class DocumentModifyDelegate extends DocumentScanDelegate implements Modifiable {

    private final Modifiable modifiable;


    public DocumentModifyDelegate( Modifiable modifiable, DocAdapterCatalog catalog ) {
        super( modifiable, catalog );
        this.modifiable = modifiable;
    }


    @Override
    public AlgNode getGraphModify( long allocId, LpgModify<?> modify, AlgBuilder builder ) {
        // todo long term add a more natural way to do this
        return Modifiable.getGraphModifySubstitute( modifiable, allocId, modify, builder );
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

}
