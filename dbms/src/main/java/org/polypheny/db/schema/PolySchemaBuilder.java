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


import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.DataContext;
import org.polypheny.db.Store;
import org.polypheny.db.StoreManager;
import org.polypheny.db.Transaction;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedSchema;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.schema.impl.AbstractSchema;
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
        CatalogCombinedDatabase combinedDatabase;
        try {
            combinedDatabase = transaction.getCatalog().getCombinedDatabase( 0 );
        } catch ( GenericCatalogException | UnknownSchemaException | UnknownTableException e ) {
            throw new RuntimeException( "Something went wrong while retrieving the current schema from the catalog.", e );
        }

        for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
            Map<String, Table> tableMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), combinedSchema.getSchema().name ).plus();
            // Create schema on stores
            for ( Store store : StoreManager.getInstance().getStores().values() ) {
                store.createNewSchema( transaction, rootSchema, combinedSchema.getSchema().name );
            }
            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // TODO MV: This assumes that there are only "complete" placements of tables and no vertical portioning at all.
            //
            for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                int storeId = combinedTable.getColumnPlacementsByStore().keySet().iterator().next(); // TODO MV: This looks inefficient
                Store store = StoreManager.getInstance().getStore( storeId );
                Table table = store.createTableSchema( combinedTable );
                s.add( combinedTable.getTable().name, table );
                tableMap.put( combinedTable.getTable().name, table );
            }
            rootSchema.add( combinedSchema.getSchema().name, s );
            tableMap.forEach( rootSchema.getSubSchema( combinedSchema.getSchema().name )::add );
            if ( combinedDatabase.getDefaultSchema() != null && combinedSchema.getSchema().id == combinedDatabase.getDefaultSchema().id ) {
                tableMap.forEach( rootSchema::add );
            }
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
