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
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialectFactoryImpl;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HsqldbStore implements Store {

    private static final Logger LOG = LoggerFactory.getLogger( HsqldbStore.class );

    private final BasicDataSource dataSource;
    private JdbcSchema currentJdbcSchema;
    private SqlDialect dialect;


    public HsqldbStore() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
        dataSource.setUrl( "jdbc:hsqldb:mem:testdb" );
        dataSource.setUsername( "sa" );
        dataSource.setPassword( "" );

        // TODO: Change when implementing transaction support
        dataSource.setDefaultAutoCommit( true );

        this.dataSource = dataSource;
        dialect = JdbcSchema.createDialect( SqlDialectFactoryImpl.INSTANCE, dataSource );
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
        builder.append( "CREATE TABLE " ).append( dialect.quoteIdentifier( combinedTable.getTable().name ) ).append( " ( " );
        boolean first = true;
        for ( CatalogColumn column : combinedTable.getColumns() ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( column.name ) ).append( " " );
            builder.append( getTypeString( column.type ) );
            if ( column.length != null ) {
                builder.append( "(" ).append( column.length );
                if ( column.scale != null ) {
                    builder.append( "," ).append( column.scale );
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
        builder.append( "DROP TABLE " ).append( dialect.quoteIdentifier( combinedTable.getTable().name ) );
        try {
            dataSource.getConnection().createStatement().executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void addColumn( CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( catalogTable.getTable().name ) );
        builder.append( " ADD " ).append( dialect.quoteIdentifier( catalogColumn.name ) ).append( " " );
        builder.append( catalogColumn.type.name() );
        if ( catalogColumn.length != null ) {
            builder.append( "(" );
            builder.append( catalogColumn.length );
            if ( catalogColumn.scale != null ) {
                builder.append( "," ).append( catalogColumn.scale );
            }
            builder.append( ")" );
        }
        if ( catalogColumn.nullable ) {
            builder.append( " NULL" );
        } else {
            builder.append( " NOT NULL" );
        }
        if ( catalogColumn.position <= catalogTable.getColumns().size() ) {
            String beforeColumnName = catalogTable.getColumns().get( catalogColumn.position - 1 ).name;
            builder.append( " BEFORE " ).append( dialect.quoteIdentifier( beforeColumnName ) );
        }
        try {
            dataSource.getConnection().createStatement().executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void dropColumn( CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( catalogTable.getTable().name ) );
        builder.append( " DROP " ).append( dialect.quoteIdentifier( catalogColumn.name ) );
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
        builder.append( "TRUNCATE TABLE " ).append( dialect.quoteIdentifier( combinedTable.getTable().name ) );
        try {
            dataSource.getConnection().createStatement().executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void updateColumnType( CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( catalogColumn.tableName ) ).append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( catalogColumn.name ) ).append( " " ).append( catalogColumn.type );
        if ( catalogColumn.length != null ) {
            builder.append( "(" );
            builder.append( catalogColumn.length );
            if ( catalogColumn.scale != null ) {
                builder.append( "," ).append( catalogColumn.scale );
            }
            builder.append( ")" );
        }
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
            case VARCHAR:
                return "VARCHAR";
            case TEXT:
                throw new RuntimeException( "Unsupported datatype: " + polySqlType.name() );
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
