package ch.unibas.dmi.dbis.polyphenydb.adapter.hsqldb;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbcp2.BasicDataSource;


public class HsqldbStore implements Store {


    private final BasicDataSource dataSource;
    private JdbcSchema currentJdbcSchema;


    public HsqldbStore() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
        dataSource.setUrl( "jdbc:hsqldb:mem:testdb" );
        dataSource.setUsername( "sa" );
        dataSource.setPassword( "" );

        try {
            addDefaultSchema( dataSource );
        } catch ( SQLException e ) {
            e.printStackTrace();
        }

        this.dataSource = dataSource;


    }


    private void addDefaultSchema( BasicDataSource dataSource ) throws SQLException {
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.executeUpdate( "CREATE TABLE \"test\"(id INTEGER, name VARCHAR(20))" );
        statement.executeUpdate( "INSERT INTO \"test\"(id, name) VALUES (1, 'bob')" );
        connection.commit();
        statement.close();
        connection.close();
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        //return new JdbcSchema( dataSource, DatabaseProduct.HSQLDB.getDialect(), new JdbcConvention( DatabaseProduct.HSQLDB.getDialect(), expression, "myjdbcconvention" ), "testdb", null, combinedSchema );
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, dataSource, null, null );
    }


    @Override
    public Table createTableSchema( CatalogCombinedTable combinedTable ) {
        return currentJdbcSchema.createJdbcTable( combinedTable );
    }
}
