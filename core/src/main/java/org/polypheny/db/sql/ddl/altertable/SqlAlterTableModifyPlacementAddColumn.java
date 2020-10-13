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
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT ADD COLUMN columnName ON STORE storeName} statement.
 */
public class SqlAlterTableModifyPlacementAddColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier columnName;
    private final SqlIdentifier storeName;


    public SqlAlterTableModifyPlacementAddColumn( SqlParserPos pos, SqlIdentifier table, SqlIdentifier columnName, SqlIdentifier storeName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnName = Objects.requireNonNull( columnName );
        this.storeName = Objects.requireNonNull( storeName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, columnName, storeName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "PLACEMENT" );
        writer.keyword( "ADD" );
        writer.keyword( "COLUMN" );
        columnName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "ON" );
        writer.keyword( "STORE" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        CatalogColumn catalogColumn = getCatalogColumn( catalogTable.id, columnName );
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
            // Make sure that this store does not contain a placement of this column
            if ( Catalog.getInstance().checkIfExistsColumnPlacement( storeInstance.getStoreId(), catalogColumn.id ) ) {
                CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( storeInstance.getStoreId(), catalogColumn.id );
                if ( placement.placementType == PlacementType.AUTOMATIC ) {
                    // Make placement manual
                    Catalog.getInstance().updateColumnPlacementType(
                            storeInstance.getStoreId(),
                            catalogColumn.id,
                            PlacementType.MANUAL );
                } else {
                    throw SqlUtil.newContextException(
                            storeName.getParserPosition(),
                            RESOURCE.placementAlreadyExists( catalogTable.name, storeName.getSimple() ) );
                }
            } else {
                // Create column placement
                Catalog.getInstance().addColumnPlacement(
                        storeInstance.getStoreId(),
                        catalogColumn.id,
                        PlacementType.MANUAL,
                        null,
                        null,
                        null,
                        null );
                // Add column on store
                storeInstance.addColumn( context, catalogTable, catalogColumn );
                // Copy the data to the newly added column placements
                DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
                dataMigrator.copyData( statement.getTransaction(), Catalog.getInstance().getStore( storeInstance.getStoreId() ), ImmutableList.of( catalogColumn ) );
            }
        } catch ( GenericCatalogException | UnknownColumnPlacementException | UnknownStoreException e ) {
            throw new RuntimeException( e );
        }
    }

}

