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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.catalog.catalogs.GraphAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;

@Slf4j
public class GraphModifyDelegate extends GraphScanDelegate implements Modifiable {


    private final Modifiable modifiable;


    public GraphModifyDelegate( Modifiable modifiable, GraphAdapterCatalog catalog ) {
        super( modifiable, catalog );
        this.modifiable = modifiable;
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn column ) {
        log.warn( "Should be overwritten by callee." );
        modifiable.addColumn( context, allocId, column );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        log.warn( "Should be overwritten by callee." );
        modifiable.dropColumn( context, allocId, columnId );
    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        log.warn( "Should be overwritten by callee." );
        return modifiable.addIndex( context, index, allocation );
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {
        modifiable.dropIndex( context, index, allocId );
    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn column ) {
        log.warn( "Should be overwritten by callee." );
        modifiable.updateColumnType( context, allocId, column );
    }


    @Override
    public AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
        return Modifiable.getDocModifySubstitute( modifiable, allocId, modify, builder );
    }

}
