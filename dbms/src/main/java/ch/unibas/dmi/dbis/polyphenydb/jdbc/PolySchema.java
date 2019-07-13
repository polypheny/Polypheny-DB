package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.StoreManager;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
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

    private PolyphenyDbSchema current = null;


    public static PolySchema getInstance() {
        return INSTANCE;
    }


    public PolyphenyDbSchema getCurrent() {
        if ( current == null ) {
            current = update();
        }
        return current;
    }


    public PolyphenyDbSchema update() {
        final PolyphenyDbSchema polyphenyDbSchema;
        final Schema schema = new RootSchema();
        if ( false ) {
            polyphenyDbSchema = new CachingPolyphenyDbSchema( null, schema, "" );
        } else {
            polyphenyDbSchema = new SimplePolyphenyDbSchema( null, schema, "" );
        }

        SchemaPlus rootSchema = polyphenyDbSchema.plus();

        // Build schema
        //final Expression expression = polyphenyDbSchema.plus().getExpression( null, "" );
        StoreManager.getInstance().getStores().forEach( ( uniqueName, store ) -> {
            Schema s = store.getSchema( rootSchema );
            rootSchema.add( uniqueName, s );

            Map<String, Table> tableMap = null;
            if ( uniqueName.equals( "HSQLDB" ) ) {
                tableMap = new HashMap<>( ((JdbcSchema) s).getTableMap() );
            } else if ( uniqueName.equals( "CSV" ) ) {
                tableMap = new HashMap<>( ((CsvSchema) s).getTableMap() );
            }
            tableMap.forEach( polyphenyDbSchema.getSubSchema( uniqueName, false )::add );
        } );

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
