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

package org.polypheny.db.schema;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedSchema;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.BuiltInMethod;


public class PolySchemaBuilder {

    private final static PolySchemaBuilder INSTANCE = new PolySchemaBuilder();


    public static PolySchemaBuilder getInstance() {
        return INSTANCE;
    }


    public AbstractPolyphenyDbSchema getCurrent( Transaction transaction ) {
        return update( transaction );
    }


    public AbstractPolyphenyDbSchema update( Transaction transaction ) {
        final AbstractPolyphenyDbSchema polyphenyDbSchema;
        final Schema schema = new RootSchema();
        if ( false ) {
            polyphenyDbSchema = new CachingPolyphenyDbSchema( null, schema, "" );
        } else {
            polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "" );
        }

        SchemaPlus rootSchema = polyphenyDbSchema.plus();

        // Build schema
        Catalog catalog = transaction.getCatalog();
        List<CatalogSchema> schemas;
        try {
            schemas = catalog.getSchemas( 0, null );
            // combinedDatabase = transaction.getCatalog().getCombinedDatabase( 0 );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( "Something went wrong while retrieving the current schema from the catalog.", e );
        }
        List<CatalogTable> catalogTables = null;
        // TODO DL refactor, perhaps for such occasion still combined object?
        try {
            for ( CatalogSchema catalogSchema : schemas ) {
                Map<String, Table> tableMap = new HashMap<>();
                SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), catalogSchema.name ).plus();
                // Create schema on stores
                for ( Store store : StoreManager.getInstance().getStores().values() ) {
                    store.createNewSchema( transaction, rootSchema, catalogSchema.name );
                }
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO MV: This assumes that there are only "complete" placements of tables and no vertical portioning at all.
                //

                catalogTables = catalog.getTables( catalogSchema.id, null );

                for ( CatalogTable catalogTable : catalogTables ) {
                    Map<Integer, List<CatalogColumnPlacement>> placements = new HashMap<>();
                    List<CatalogStore> stores = catalog.getStores();

                    for ( CatalogStore store : stores ) {
                        placements.put( store.id, catalog.getColumnPlacementsOnStore( store.id ) );
                    }

                    Map<Long, List<CatalogColumnPlacement>> placementsColumn = new HashMap<>();
                    List<CatalogColumn> columns = catalog.getColumns( catalogTable.id );
                    for ( CatalogColumn column : columns ) {
                        placementsColumn.put(column.id, catalog.getColumnPlacementByColumn( column.id ));
                    }

                    CatalogCombinedTable combinedTable = new CatalogCombinedTable( catalogTable, catalog.getColumns( catalogTable.id ), catalogSchema, catalog.getDatabase( 0 ), catalog.getUser( catalogTable.ownerName ), placements, placementsColumn, catalog.getKeys().stream().filter( k -> k.tableId == catalogTable.id ).collect( Collectors.toList()) );
                    int storeId = combinedTable.getColumnPlacementsByStore().keySet().iterator().next(); // TODO MV: This looks inefficient
                    Store store = StoreManager.getInstance().getStore( storeId );
                    Table table = store.createTableSchema( combinedTable );
                    s.add( catalogTable.name, table );
                    tableMap.put( catalogTable.name, table );
                }
                rootSchema.add( catalogSchema.name, s );
                tableMap.forEach( rootSchema.getSubSchema( catalogSchema.name )::add );
                CatalogDatabase catalogDatabase = catalog.getDatabase( 0 );
                if ( catalogDatabase.defaultSchemaId == catalogSchema.id ) {
                    tableMap.forEach( rootSchema::add );
                }
            }
        } catch ( GenericCatalogException | UnknownDatabaseException | UnknownCollationException | UnknownTypeException | UnknownUserException e ) {
            e.printStackTrace();
        }

        polyphenyDbSchema.getSubSchemaMap().forEach( ( schemaName, s ) -> s.setSchema( StoreManager.getInstance().getStore( 0 ).getCurrentSchema() ) );

        return polyphenyDbSchema;
    }


    /**
     * Schema that has no parents.
     */
    private static class RootSchema extends AbstractSchema {

        RootSchema() {
            super();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method );
        }
    }

}
