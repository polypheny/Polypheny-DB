package ch.unibas.dmi.dbis.polyphenydb.adapter.hsqldb;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect.DatabaseProduct;
import org.apache.commons.dbcp2.BasicDataSource;


public class HsqldbStore implements Store {


    private final JdbcSchema schema;


    public HsqldbStore() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
        dataSource.setUrl( "jdbc:hsqldb:mem:testdb" );
        dataSource.setUsername( "sa" );
        dataSource.setPassword( "" );
        schema = new JdbcSchema( dataSource, DatabaseProduct.HSQLDB.getDialect(), null, "test", null );
    }


    @Override
    public void addDatabase( String databaseName ) {

    }


    @Override
    public void addSchema( String schemaName, String databaseName ) {

    }


    @Override
    public void addTable( String tableName, String schemaName, String databaseName ) {

    }


    @Override
    public Schema getSchema() {
        return schema;
    }
}
