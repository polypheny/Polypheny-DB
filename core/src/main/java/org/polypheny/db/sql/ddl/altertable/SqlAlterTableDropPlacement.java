/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name DROP PLACEMENT ON STORE storeName} statement.
 */
public class SqlAlterTableDropPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier storeName;


    public SqlAlterTableDropPlacement( SqlParserPos pos, SqlIdentifier table, SqlIdentifier storeName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = Objects.requireNonNull( storeName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "DROP" );
        writer.keyword( "PLACEMENT" );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        try {
            // Check whether this placement exists
            if ( !catalogTable.placementsByStore.containsKey( storeInstance.getStoreId() ) ) {
                throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.placementDoesNotExist( catalogTable.name, storeName.getSimple() ) );
            }
            // Check whether the store supports schema changes
            if ( storeInstance.isSchemaReadOnly() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.storeIsSchemaReadOnly( storeName.getSimple() ) );
            }
            // Check if there are is another placement for every column on this store
            for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacementsOnStore( storeInstance.getStoreId(), catalogTable.id ) ) {
                List<CatalogColumnPlacement> existingPlacements = Catalog.getInstance().getColumnPlacements( placement.columnId );
                if ( existingPlacements.size() < 2 ) {
                    throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.onlyOnePlacementLeft() );
                }
            }
            // Drop all indexes on this store
            try {
                for ( CatalogIndex index : Catalog.getInstance().getIndexes( catalogTable.id, false ) ) {
                    if ( index.location == storeInstance.getStoreId() ) {
                        if ( index.location == 0 ) {
                            // Delete polystore index
                            IndexManager.getInstance().deleteIndex( index );
                        } else {
                            // Delete index on store
                            StoreManager.getInstance().getStore( index.location ).dropIndex( context, index );
                        }
                        // Delete index in catalog
                        Catalog.getInstance().deleteIndex( index.id );
                    }
                }
            } catch ( GenericCatalogException e ) {
                throw new PolyphenyDbContextException( "Exception while dropping indexes on data store.", e );
            }
            // Physically delete the data from the store
            storeInstance.dropTable( context, catalogTable );
            // Inform routing
            statement.getRouter().dropPlacements( Catalog.getInstance().getColumnPlacementsOnStore( storeInstance.getStoreId(), catalogTable.id ) );
            // Delete placement in the catalog
            List<CatalogColumnPlacement> placements = Catalog.getInstance().getColumnPlacementsOnStore( storeInstance.getStoreId(), catalogTable.id );
            for ( CatalogColumnPlacement placement : placements ) {
                Catalog.getInstance().deleteColumnPlacement( storeInstance.getStoreId(), placement.columnId );
            }
            // Remove All
            Catalog.getInstance().deletePartitionsOnDataPlacement( storeInstance.getStoreId(), catalogTable.id );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }

}

