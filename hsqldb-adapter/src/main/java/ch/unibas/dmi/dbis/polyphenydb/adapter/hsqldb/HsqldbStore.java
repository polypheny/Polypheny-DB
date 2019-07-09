package ch.unibas.dmi.dbis.polyphenydb.adapter.hsqldb;


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect.DatabaseProduct;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.commons.dbcp2.BasicDataSource;


public class HsqldbStore implements Store {


    private final BasicDataSource dataSource;


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
        statement.executeUpdate( "CREATE TABLE test(id INTEGER, name VARCHAR(20))" );
        statement.executeUpdate( "INSERT INTO test(id, name) VALUES (1, 'bob')" );
        connection.commit();
        statement.close();
//        connection.close();
    }


    @Override
    public Map<String, Table> getTables( SchemaPlus schemaPlus ) {
        final Expression expression = schemaPlus.getExpression( null, "" );
        final JdbcSchema schema = new JdbcSchema( dataSource, DatabaseProduct.HSQLDB.getDialect(), new JdbcConvention( DatabaseProduct.HSQLDB.getDialect(), expression, "myjdbcconvention" ), "test", null );

        final Map<String, Table> map = new HashMap<>();
        map.put( "test", new JdbcTable( schema, "testdb", null, "test", TableType.TABLE ) );
        return map;
    }
}
