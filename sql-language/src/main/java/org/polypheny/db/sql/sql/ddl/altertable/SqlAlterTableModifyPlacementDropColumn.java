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

package org.polypheny.db.sql.sql.ddl.altertable;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.IndexPreventsRemovalException;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.PlacementIsPrimaryException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlIdentifier;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.sql.sql.ddl.SqlAlterTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY PLACEMENT DROP COLUMN columnName ON STORE storeName} statement.
 */
public class SqlAlterTableModifyPlacementDropColumn extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier columnName;
    private final SqlIdentifier storeName;


    public SqlAlterTableModifyPlacementDropColumn( ParserPos pos, SqlIdentifier table, SqlIdentifier columnName, SqlIdentifier storeName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.columnName = Objects.requireNonNull( columnName );
        this.storeName = Objects.requireNonNull( storeName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table, columnName, storeName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
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
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        DataStore storeInstance = getDataStoreInstance( storeName );

        if ( catalogTable.tableType != TableType.TABLE ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogTable.name + " is not a table." );
        }

        try {
            DdlManager.getInstance().dropColumnPlacement(
                    catalogTable,
                    columnName.getSimple(),
                    storeInstance,
                    statement );
        } catch ( UnknownAdapterException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.unknownAdapter( storeName.getSimple() ) );
        } catch ( PlacementNotExistsException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.placementDoesNotExist( storeName.getSimple(), catalogTable.name ) );
        } catch ( IndexPreventsRemovalException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.indexPreventsRemovalOfPlacement( e.getIndexName(), columnName.getSimple() ) );
        } catch ( LastPlacementException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.onlyOnePlacementLeft() );
        } catch ( PlacementIsPrimaryException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.placementIsPrimaryKey( columnName.getSimple() ) );
        } catch ( ColumnNotExistsException e ) {
            throw CoreUtil.newContextException(
                    columnName.getPos(),
                    RESOURCE.columnNotFoundInTable( e.columnName, e.tableName )
            );
        }
    }

}

