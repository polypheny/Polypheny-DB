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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl.altertable;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.PlacementType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code ALTER TABLE name ADD PLACEMENT} statement.
 */
public class SqlAlterTableAddPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier storeName;


    public SqlAlterTableAddPlacement( SqlParserPos pos, SqlIdentifier table, SqlIdentifier storeName ) {
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
        writer.keyword( "ADD" );
        writer.keyword( "PLACEMENT" );
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogCombinedTable combinedTable = getCatalogCombinedTable( context, transaction, table );
        Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        try {
            // Check whether this placement already exists
            for ( int storeId : combinedTable.getColumnPlacementsByStore().keySet() ) {
                if ( storeId == storeInstance.getStoreId() ) {
                    throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.placementAlreadyExists( storeName.getSimple(), combinedTable.getTable().name ) );
                }
            }
            // Create column placements
            for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
                transaction.getCatalog().addColumnPlacement( storeInstance.getStoreId(), catalogColumn.id, PlacementType.MANUAL, null, null, null );
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO MV: Now we need to actually create the placement on the store and get the physical schema, table and column name
                //
            }
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }

}

