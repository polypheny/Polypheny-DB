package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.DbmsMeta.RootSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;


public class PolySchema {

    private final static PolySchema INSTANCE = new PolySchema();

    private SchemaPlus current = null;


    public static PolySchema getInstance() {
        return INSTANCE;
    }


    public SchemaPlus getCurrent() {
        if ( current == null ) {
            current = update();
        }
        return current;
    }


    public SchemaPlus update() {
        final PolyphenyDbSchema polyphenyDbSchema;
        final Schema schema = new RootSchema();
        if ( false ) {
            polyphenyDbSchema = new CachingPolyphenyDbSchema( null, schema, "" );
        } else {
            polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "" );
        }

        final SchemaPlus rootSchema = polyphenyDbSchema.plus();

        // Build schema
        StoreManager.getInstance().getStores().forEach( ( uniqueName, store ) -> {
            store.getTables( rootSchema ).forEach( rootSchema::add );
        } );

        return rootSchema;
    }


}
