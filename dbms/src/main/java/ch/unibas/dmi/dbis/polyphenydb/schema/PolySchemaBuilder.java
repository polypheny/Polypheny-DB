/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.schema;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;


public class PolySchemaBuilder {

    private final static PolySchemaBuilder INSTANCE = new PolySchemaBuilder();

    private Map<PolyXid, AbstractPolyphenyDbSchema> cache = new HashMap<>();


    public static PolySchemaBuilder getInstance() {
        return INSTANCE;
    }


    public AbstractPolyphenyDbSchema getCurrent( Transaction transaction ) {
        /*if ( !cache.containsKey( transaction.getXid() ) ) {
            cache.put( transaction.getXid(), update( transaction ) );
        }
        return cache.get( transaction.getXid() ); */
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
            for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                int storeId = combinedTable.getPlacements().get( 0 ).storeId;
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
