/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.ddl.DdlManager;
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


public class SqlAlterTableRefreshPlacement extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier storeName;
    private final boolean allPlacements;


    public SqlAlterTableRefreshPlacement(
            ParserPos pos,
            SqlIdentifier table,
            SqlIdentifier storeName,
            boolean allPlacements ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.storeName = storeName;
        this.allPlacements = allPlacements;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( table );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( table );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "REFRESH" );

        if ( allPlacements ) {
            writer.keyword( "ALL" );
            writer.keyword( "PLACEMENTS" );
        } else {
            writer.keyword( "PLACEMENT" );
            writer.keyword( "ON" );
            writer.keyword( "STORE" );
            storeName.unparse( writer, leftPrec, rightPrec );
        }

    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        CatalogEntity catalogEntity = getCatalogTable( context, table );

        if ( catalogEntity.entityType != EntityType.ENTITY ) {
            throw new RuntimeException( "Not possible to use ALTER TABLE because " + catalogEntity.name + " is not a table." );
        }

        List<DataStore> dataStores = new ArrayList<>();

        // Add all eagerly replicated stores
        if ( allPlacements ) {
            Catalog.getInstance().getDataPlacementsByReplicationStrategy( catalogEntity.id, ReplicationStrategy.LAZY ).forEach( dp ->
                    dataStores.add( getDataStoreInstance( dp.adapterId ) )
            );
        } else {
            DataStore storeInstance = getDataStoreInstance( storeName );
            if ( Catalog.getInstance().checkIfExistsDataPlacement( storeInstance.getAdapterId(), catalogEntity.id ) ) {

                if ( Catalog.getInstance().getDataPlacement( storeInstance.getAdapterId(), catalogEntity.id ).replicationStrategy.equals( ReplicationStrategy.EAGER ) ) {
                    throw new RuntimeException( "The placement on store '" + storeName + "' is updated EAGERLY. Nothing to refresh here." );
                }
                dataStores.add( storeInstance );
            }
        }

        try {
            DdlManager.getInstance().refreshDataPlacements( catalogEntity, dataStores, statement );
        } catch ( UnknownAdapterException e ) {
            throw CoreUtil.newContextException(
                    storeName.getPos(),
                    RESOURCE.unknownAdapter( storeName.getSimple() ) );
        }
    }

}
