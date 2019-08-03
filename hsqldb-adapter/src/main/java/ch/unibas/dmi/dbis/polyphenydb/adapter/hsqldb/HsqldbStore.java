package ch.unibas.dmi.dbis.polyphenydb.adapter.hsqldb;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HsqldbStore implements Store {

    private static final Logger LOG = LoggerFactory.getLogger( HsqldbStore.class );

    private final BasicDataSource dataSource;
    private JdbcSchema currentJdbcSchema;


    public HsqldbStore() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
        dataSource.setUrl( "jdbc:hsqldb:mem:testdb" );
        dataSource.setUsername( "sa" );
        dataSource.setPassword( "" );

        // TODO: Change when implementing transaction support
        dataSource.setDefaultAutoCommit( true );

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
        statement.executeUpdate( "CREATE TABLE \"test\"(\"id\" INTEGER, \"name\" VARCHAR(20))" );
        statement.executeUpdate( "INSERT INTO \"test\"(\"id\", \"name\") VALUES (1, 'bob')" );
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


    @Override
    public Schema getCurrentSchema() {
        return currentJdbcSchema;
    }


    @Override
    public void createTable( Context context, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE TABLE " ).append( currentJdbcSchema.dialect.quoteIdentifier( combinedTable.getTable().name ) ).append( " ( " );
        boolean first = true;
        for ( CatalogColumn column : combinedTable.getColumns() ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( currentJdbcSchema.dialect.quoteIdentifier( column.name ) ).append( " " );
            builder.append( getTypeString( column.type ) );
            if ( column.precision != null ) {
                builder.append( "(" ).append( column.precision );
                if ( column.length != null ) {
                    builder.append( "," ).append( column.length );
                }
                builder.append( ")" );
            }

        }
        builder.append( " )" );
        try {
            dataSource.getConnection().createStatement().executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropTable( CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "DROP TABLE " ).append( currentJdbcSchema.dialect.quoteIdentifier( combinedTable.getTable().name ) );
        try {
            dataSource.getConnection().createStatement().executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        // TODO: implement
        LOG.warn( "Not implemented yet" );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        // TODO: implement
        LOG.warn( "Not implemented yet" );
    }


    @Override
    public void truncate( Transaction transaction, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "TRUNCATE TABLE " ).append( currentJdbcSchema.dialect.quoteIdentifier( combinedTable.getTable().name ) );
        try {
            dataSource.getConnection().createStatement().executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    private String getTypeString( PolySqlType polySqlType ) {
        switch ( polySqlType ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                return "VARBINARY";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case REAL:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
            case DECIMAL:
                return "DECIMAL";
            case MONEY:
                return "MONEY";
            case VARCHAR:
                return "VARCHAR";
            case TEXT:
                return "TEXT";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
        }
        throw new RuntimeException( "Unknown type: " + polySqlType.name() );
    }

}
