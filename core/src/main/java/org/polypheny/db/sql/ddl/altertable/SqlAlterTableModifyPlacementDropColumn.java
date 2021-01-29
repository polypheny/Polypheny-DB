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

import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT DROP COLUMN columnName ON STORE storeName} statement.
 */
public class SqlAlterTableModifyPlacementDropColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier columnName;
    private final SqlIdentifier storeName;


    public SqlAlterTableModifyPlacementDropColumn( SqlParserPos pos, SqlIdentifier table, SqlIdentifier columnName, SqlIdentifier storeName ) {
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
        writer.keyword( "DROP" );
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
        DataStore storeInstance = getDataStoreInstance( storeName );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        // Check whether this placement already exists
        if ( !catalogTable.placementsByAdapter.containsKey( storeInstance.getAdapterId() ) ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
        }
        // Check whether this store actually contains a placement of this column
        if ( !Catalog.getInstance().checkIfExistsColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id ) ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
        }
        // Check whether there are any indexes located on the store requiring this column
        for ( CatalogIndex index : Catalog.getInstance().getIndexes( catalogTable.id, false ) ) {
            if ( index.location == storeInstance.getAdapterId() && index.key.columnIds.contains( catalogColumn.id ) ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.indexPreventsRemovalOfPlacement( index.name, catalogColumn.name ) );
            }
        }
        // Check if there are is another placement for this column
        List<CatalogColumnPlacement> existingPlacements = Catalog.getInstance().getColumnPlacements( catalogColumn.id );
        if ( existingPlacements.size() < 2 ) {
            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.onlyOnePlacementLeft() );
        }
        // Check whether the column to drop is a primary key
        CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
        if ( primaryKey.columnIds.contains( catalogColumn.id ) ) {
            throw SqlUtil.newContextException(
                    storeName.getParserPosition(),
                    RESOURCE.placementIsPrimaryKey( catalogColumn.name ) );
        }
        // Drop Column on store
        storeInstance.dropColumn( context, Catalog.getInstance().getColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id ) );
        // Drop column placement
        Catalog.getInstance().deleteColumnPlacement( storeInstance.getAdapterId(), catalogColumn.id );
    }

}

