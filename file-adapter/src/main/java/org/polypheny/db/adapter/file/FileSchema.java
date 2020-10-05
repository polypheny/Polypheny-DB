/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.file;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;


public class FileSchema extends AbstractSchema {

    @Getter
    private final String schemaName;
    private final Map<String, FileTable> tableMap = new HashMap<>();
    private final FileStore store;

    public FileSchema( String schemaName, FileStore store ) {
        super();
        this.schemaName = schemaName;
        this.store = store;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return new HashMap<>( tableMap );
    }

    public Table createFileTable(  CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        ArrayList<Long> columnIds = new ArrayList<>();
        ArrayList<PolyType> columnTypes = new ArrayList<>();
        ArrayList<String> columnNames = new ArrayList<>();
        for( CatalogColumnPlacement p: columnPlacementsOnStore ) {
            CatalogColumn catalogColumn;
            catalogColumn = Catalog.getInstance().getColumn( p.columnId );
            if( p.storeId == store.getStoreId() ) {
                columnIds.add( p.columnId );
                //todo arrayType
                columnTypes.add( catalogColumn.type );
                columnNames.add( catalogColumn.name );
            }
        }
        FileTable table = new FileTable( store.getRootDir(), schemaName, catalogTable.id, columnIds, columnTypes, columnNames, store );
        tableMap.put( catalogTable.name, table );
        return table;
    }
}
