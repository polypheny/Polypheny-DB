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

package org.polypheny.db.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT (columnList) ON STORE storeName} statement.
 */
public class SqlAlterTableModifyPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnList;
    private final SqlIdentifier storeName;


    public SqlAlterTableModifyPlacement( SqlParserPos pos, SqlIdentifier table, SqlNodeList columnList, SqlIdentifier storeName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnList = Objects.requireNonNull( columnList );
        this.storeName = Objects.requireNonNull( storeName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnList, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "PLACEMENT" );
        columnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        List<Long> columnIds = new LinkedList<>();
        for ( SqlNode node : columnList.getList() ) {
            CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, (SqlIdentifier) node );
            columnIds.add( catalogColumn.id );
        }
        Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        try {
            // Check whether this placement already exists
            if ( !catalogTable.placementsByStore.containsKey( storeInstance.getStoreId() ) ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
            }
            // Check whether the store supports schema changes
            if ( storeInstance.isSchemaReadOnly() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.storeIsSchemaReadOnly( storeName.getSimple() ) );
            }
            // Which columns to remove
            for ( CatalogColumnPlacement placement : Catalog.getInstance().getColumnPlacementsOnStore( storeInstance.getStoreId(), catalogTable.id ) ) {
                if ( !columnIds.contains( placement.columnId ) ) {
                    // Check whether the column is a primary key column
                    CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
                    if ( primaryKey.columnIds.contains( placement.columnId ) ) {
                        // Check if the placement type is manual. If so, change to automatic
                        if ( placement.placementType == PlacementType.MANUAL ) {
                            // Make placement manual
                            Catalog.getInstance().updateColumnPlacementType(
                                    storeInstance.getStoreId(),
                                    placement.columnId,
                                    PlacementType.AUTOMATIC );
                        }
                    } else {
                        // It is not a primary key. Remove the column
                        // Check if there are is another placement for this column
                        List<CatalogColumnPlacement> existingPlacements = Catalog.getInstance().getColumnPlacements( placement.columnId );
                        if ( existingPlacements.size() < 2 ) {
                            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.onlyOnePlacementLeft() );
                        }
                        // Drop Column on store
                        storeInstance.dropColumn( context, Catalog.getInstance().getColumnPlacement( storeInstance.getStoreId(), placement.columnId ) );
                        // Drop column placement
                        Catalog.getInstance().deleteColumnPlacement( storeInstance.getStoreId(), placement.columnId );
                    }
                }
            }
            // Which columns to add
            for ( long cid : columnIds ) {
                if ( Catalog.getInstance().checkIfExistsColumnPlacement( storeInstance.getStoreId(), cid ) ) {
                    CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( storeInstance.getStoreId(), cid );
                    if ( placement.placementType == PlacementType.AUTOMATIC ) {
                        // Make placement manual
                        Catalog.getInstance().updateColumnPlacementType( storeInstance.getStoreId(), cid, PlacementType.MANUAL );
                    }
                } else {
                    // Create column placement
                    Catalog.getInstance().addColumnPlacement(
                            storeInstance.getStoreId(),
                            cid,
                            PlacementType.MANUAL,
                            null,
                            null,
                            null );
                    // Add column on store
                    storeInstance.addColumn( context, catalogTable, Catalog.getInstance().getColumn( cid ) );
                    // !!!!!!!!!!!!!!!!!!!!!!!!
                    // TODO: Now we should also copy the data
                }
            }
        } catch ( GenericCatalogException | UnknownKeyException | UnknownColumnPlacementException | UnknownColumnException e ) {
            throw new RuntimeException( e );
        }

    }
}

