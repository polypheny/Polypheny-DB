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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.stores;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcPhysicalNameProvider;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialectFactoryImpl;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

// TODO(jan): General PostgresqlStore todo list:
//   - Implement better logging.
//   - Check all the functions whether they are properly adjusted to Postgres.
//   - Link to Postgres documentation.

@Slf4j
public class PostgresqlStore extends AbstractJdbcStore {

    public static final String ADAPTER_NAME = "PostgreSQL";

    public static final String DESCRIPTION = "Relational database system optimized for transactional workload that provides an advanced set of features.";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 5432 ),
            new AdapterSettingString( "database",  false, true, false, "postgres"),
            new AdapterSettingString( "username",  false, true, false, "postgres"),
            new AdapterSettingString( "password",  false, true, false, "")
    );

    private final BasicDataSource dataSource;
    private JdbcSchema currentJdbcSchema;
    private SqlDialect dialect;

    // Connection information
    private String dbHostname;
    private int dbPort;
    private String dbName;
    private String dbUsername;
    private String dbPassword;

    public PostgresqlStore( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings );

        // Parse settings
        this.dbHostname = settings.get( "host" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );
        this.dbName = settings.get( "database" );
        this.dbUsername = settings.get( "username" );
        this.dbPassword = settings.get( "password" );

        this.dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.postgresql.Driver" );
        dataSource.setUrl( getConnectionUrl() );
        if ( log.isInfoEnabled() ) {
            log.info( "Postgres Connection URL: {}", getConnectionUrl() );
        }
        dataSource.setUsername( this.dbUsername );
        dataSource.setPassword( this.dbPassword );

        // TODO: Change when implementing transaction support
        dataSource.setDefaultAutoCommit( true );

//        this.dataSource = dataSource;
        dialect = JdbcSchema.createDialect( SqlDialectFactoryImpl.INSTANCE, dataSource );

        // Register the JDBC Pool Size as information in the information manager
        registerJdbcPoolSizeInformation( uniqueName, dataSource );
    }


    @Override
    public void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name ) {
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, dataSource, null, null, new JdbcPhysicalNameProvider( transaction.getCatalog() ) ); // TODO MV: Potential bug! This only works as long as we do not cache the schema between mutliple transactions
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
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
        String physicalTableName = new JdbcPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhysicalTableName( qualifiedNames ).names.get( 0 );
        if (log.isDebugEnabled()) {
            log.debug( "PostgreSQL createTable: Qualified names: {}, physicalTableName: {}",
                    qualifiedNames, physicalTableName);
        }
        builder.append( "CREATE TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) ).append( " ( " );
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
        executeUpdate( builder );
    }


    @Override
    public void dropTable( Context context, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
        String physicalTableName = new JdbcPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhysicalTableName( qualifiedNames ).names.get( 0 );
        builder.append( "DROP TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder );
    }


    @Override
    public void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        // Based on: https://www.postgresql.org/docs/current/sql-altertable.html
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( catalogTable.getSchema().name );
        qualifiedNames.add( catalogTable.getTable().name );
        String physicalTableName = new JdbcPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhysicalTableName( qualifiedNames ).names.get( 0 );
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
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
        executeUpdate( builder );
    }


    @Override
    public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( catalogTable.getSchema().name );
        qualifiedNames.add( catalogTable.getTable().name );
        String physicalTableName = new JdbcPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhysicalTableName( qualifiedNames ).names.get( 0 );
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " DROP " ).append( dialect.quoteIdentifier( catalogColumn.name ) );
        executeUpdate( builder );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        // TODO: implement
        log.warn( "Not implemented yet" );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        // TODO: implement
        log.warn( "Not implemented yet" );
    }


    @Override
    public void truncate( Context context, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
        String physicalTableName = new JdbcPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhysicalTableName( qualifiedNames ).names.get( 0 );
        builder.append( "TRUNCATE TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( catalogColumn.schemaName );
        qualifiedNames.add( catalogColumn.tableName );
        String physicalTableName = new JdbcPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhysicalTableName( qualifiedNames ).names.get( 0 );
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( catalogColumn.name ) );
        builder.append( " TYPE " ).append( catalogColumn.type );
        if ( catalogColumn.length != null ) {
            builder.append( "(" );
            builder.append( catalogColumn.length );
            if ( catalogColumn.scale != null ) {
                builder.append( "," ).append( catalogColumn.scale );
            }
            builder.append( ")" );
        }
        executeUpdate( builder );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        try {
            dataSource.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down " + getUniqueName(), e );
        }
    }


    @Override
    protected void applySetting( AdapterSetting s, String newValue ) {
        // This is in preparation of an AdapterSetting rework...
        // first we disconnect from the postgres instance
        /*try {
            dataSource.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down " + getUniqueName(), e );
        }*/
        // Now we can reconnect.
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

    private void executeUpdate( StringBuilder builder ) {
        if (log.isDebugEnabled()) {
            log.debug( "PostgreSQL JDBC executing query: {}", builder.toString() );
        }
        Statement statement = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate( builder.toString() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                    log.info( "Exception while closing statement!", e );
                }
            }
            if ( connection != null ) {
                try {
                    connection.close();
                } catch ( SQLException e ) {
                    log.info( "Exception while closing connection!", e );
                }
            }
        }
    }

    private String getConnectionUrl() {
        return String.format( "jdbc:postgresql://%s:%d/%s", this.dbHostname, this.dbPort, this.dbName );
    }

}
