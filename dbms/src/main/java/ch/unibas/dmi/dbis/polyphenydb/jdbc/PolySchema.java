package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;


public class PolySchema {

    private final static PolySchema INSTANCE = new PolySchema();

    private Map<PolyXid, PolyphenyDbSchema> cache = new HashMap<>();


    public static PolySchema getInstance() {
        return INSTANCE;
    }


    public PolyphenyDbSchema getCurrent( PolyXid xid ) {
        if ( !cache.containsValue( xid ) ) {
            cache.put( xid, update( xid ) );
        }
        return cache.get( xid );
    }


    public PolyphenyDbSchema update( PolyXid xid ) {
        final PolyphenyDbSchema polyphenyDbSchema;
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
            combinedDatabase = CatalogManagerImpl.getInstance().getCombinedDatabase( xid, 0 );
        } catch ( GenericCatalogException | UnknownSchemaException | UnknownTableException e ) {
            throw new RuntimeException( "Something went wrong while retrieving the current schema from the catalog.", e );
        }

        for ( CatalogCombinedSchema combinedSchema : combinedDatabase.getSchemas() ) {
            Map<String, Table> tableMap = new HashMap<>();
            SchemaPlus s = new SimplePolyphenyDbSchema( polyphenyDbSchema, new AbstractSchema(), combinedSchema.getSchema().name ).plus();
            for ( CatalogCombinedTable combinedTable : combinedSchema.getTables() ) {
                int storeId = combinedTable.getPlacements().get( 0 ).storeId;
                Store store = StoreManager.getInstance().getStore( storeId );
                store.createNewSchema( rootSchema, combinedSchema.getSchema().name );
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
