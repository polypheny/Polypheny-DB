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

import com.google.common.collect.ImmutableList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name ADD PLACEMENT [(columnList)] ON STORE storeName} statement.
 */
public class SqlAlterTableAddPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlNodeList columnList;
    private final SqlIdentifier storeName;


    public SqlAlterTableAddPlacement( SqlParserPos pos, SqlIdentifier table, SqlNodeList columnList, SqlIdentifier storeName ) {
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
        writer.keyword( "ADD" );
        writer.keyword( "PLACEMENT" );
        columnList.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        List<Long> columnIds = new LinkedList<>();
        for ( SqlNode node : columnList.getList() ) {
            CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, (SqlIdentifier) node );
            columnIds.add( catalogColumn.id );
        }
        List<CatalogColumn> addedColumns = new LinkedList<>();
        Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        try {
            // Check whether this placement already exists
            for ( int storeId : catalogTable.placementsByStore.keySet() ) {
                if ( storeId == storeInstance.getStoreId() ) {
                    throw SqlUtil.newContextException(
                            storeName.getParserPosition(),
                            RESOURCE.placementAlreadyExists( catalogTable.name, storeName.getSimple() ) );
                }
            }
            // Check whether the store supports schema changes
            if ( storeInstance.isSchemaReadOnly() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.storeIsSchemaReadOnly( storeName.getSimple() ) );
            }
            // Check whether the list is empty (this is a short hand for a full placement)
            if ( columnIds.size() == 0 ) {
                columnIds = ImmutableList.copyOf( catalogTable.columnIds );
            }
            // Create column placements
            for ( long cid : columnIds ) {
                Catalog.getInstance().addColumnPlacement(
                        storeInstance.getStoreId(),
                        cid,
                        PlacementType.MANUAL,
                        null,
                        null,
                        null );
                addedColumns.add( Catalog.getInstance().getColumn( cid ) );
            }
            //Check if placement includes primary key columns
            CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
            for ( long cid : primaryKey.columnIds ) {
                if ( !columnIds.contains( cid ) ) {
                    Catalog.getInstance().addColumnPlacement(
                            storeInstance.getStoreId(),
                            cid,
                            PlacementType.AUTOMATIC,
                            null,
                            null,
                            null );
                    addedColumns.add( Catalog.getInstance().getColumn( cid ) );
                }
            }
            // Create table on store
            storeInstance.createTable( context, catalogTable );
            // Copy data to the newly added placements
            DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
            dataMigrator.copyData( statement.getTransaction(), Catalog.getInstance().getStore( storeInstance.getStoreId() ), addedColumns );
        } catch ( GenericCatalogException | UnknownStoreException e ) {
            throw new RuntimeException( e );
        }
    }

}

