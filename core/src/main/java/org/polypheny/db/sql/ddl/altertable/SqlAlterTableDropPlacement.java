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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name DROP PLACEMENT} statement.
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
        storeName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        Store storeInstance = StoreManager.getInstance().getStore( storeName.getSimple() );
        if ( storeInstance == null ) {
            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.unknownStoreName( storeName.getSimple() ) );
        }
        try {
            // Check if there are at least to placements for each column of this table
            for ( List<CatalogColumnPlacement> placements : catalogTable.columnIds.stream().map( id -> Catalog.getInstance().getColumnPlacements( id ) ).collect( Collectors.toList() ) ) {
                if ( placements.size() < 2 ) {
                    throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.onlyOnePlacementLeft() );
                }
            }
            // Check whether the store supports schema changes
            if ( storeInstance.isSchemaReadOnly() ) {
                throw SqlUtil.newContextException(
                        storeName.getParserPosition(),
                        RESOURCE.storeIsSchemaReadOnly( storeName.getSimple() ) );
            }
            // Check whether this placement exists
            Map<Integer, ImmutableList<Long>> placements = catalogTable.placementsByStore;
            if ( placements.containsKey( storeInstance.getStoreId() ) ) {
                // Physically delete the data from the store
                storeInstance.dropTable( context, catalogTable );
                // Inform routing
                transaction.getRouter().dropPlacements( Catalog.getInstance().getColumnPlacementsOnStore( storeInstance.getStoreId(), catalogTable.id ) );
                // Delete placement in the catalog
                for ( long columnId : placements.get( storeInstance.getStoreId() ) ) {
                    Catalog.getInstance().deleteColumnPlacement( storeInstance.getStoreId(), columnId );
                }
                return;
            }

            throw SqlUtil.newContextException( storeName.getParserPosition(), RESOURCE.placementDoesNotExist( catalogTable.name, storeName.getSimple() ) );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }

}

