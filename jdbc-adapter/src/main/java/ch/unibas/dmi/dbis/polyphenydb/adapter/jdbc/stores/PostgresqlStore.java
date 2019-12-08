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
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcPhysicalNameProvider;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphData;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphType;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialectFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PostgresqlSqlDialect;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

// TODO(jan): General PostgresqlStore todo list:
//   - Implement better logging.
//   - Check all the functions whether they are properly adjusted to Postgres.
//   - Link to Postgres documentation.
//   - Add to 'catalogSchema.sql' in a way that does not require a running Postgres instance.

@Slf4j
public class PostgresqlStore extends Store {

    public static final String ADAPTER_NAME = "PGJDBC";

    public static final String DESCRIPTION = "The World's Most Advanced Open Source Relational Database";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 5432 ),
            new AdapterSettingString( "username",  false, true, false, "postgres"),
            new AdapterSettingString( "password",  false, true, false, "")
    );

    private final BasicDataSource dataSource;
    private JdbcSchema currentJdbcSchema;
    private SqlDialect dialect;

    public PostgresqlStore( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings );
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.postgresql.Driver" );
        // TODO(jan): Improve initial connection handling
        dataSource.setUrl( "jdbc:postgresql://" + settings.get( "host" ) + ":"
                + settings.get( "port" ) + "/" + uniqueName );
        dataSource.setUsername( settings.get( "username" ) );
        dataSource.setPassword( settings.get( "password" ) );

        // TODO: Change when implementing transaction support
        dataSource.setDefaultAutoCommit( true );

        this.dataSource = dataSource;
        dialect = JdbcSchema.createDialect( SqlDialectFactoryImpl.INSTANCE, dataSource );

        // TODO(jan): Figure out how to properly use the InformationManager
        // ------ Information Manager -----------
        final InformationPage informationPage = new InformationPage( uniqueName, uniqueName );
        final InformationGroup informationGroupConnectionPool = new InformationGroup( "PGJDBC Connection Pool", informationPage.getId() );

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupConnectionPool );

        InformationGraph connectionPoolSizeGraph = new InformationGraph(
                "connectionPoolSizeGraph",
                informationGroupConnectionPool.getId(),
                GraphType.DOUGHNUT,
                new String[]{ "Active", "Available", "Idle" }
        );
        im.registerInformation( connectionPoolSizeGraph );

        InformationTable connectionPoolSizeTable = new InformationTable(
                "connectionPoolSizeTable",
                informationGroupConnectionPool.getId(),
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( connectionPoolSizeTable );

        ConnectionPoolSizeInfo connectionPoolSizeInfo = new ConnectionPoolSizeInfo( connectionPoolSizeGraph, connectionPoolSizeTable );
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate( connectionPoolSizeInfo, 0, 5, TimeUnit.SECONDS );
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
        // There is no modifiable setting for this store
    }


    // TODO(jan): Is this required for the PostgreSQL JDBC driver?
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

    private class ConnectionPoolSizeInfo implements Runnable {

        private final InformationGraph graph;
        private final InformationTable table;

        ConnectionPoolSizeInfo( InformationGraph graph, InformationTable table ) {
            this.graph = graph;
            this.table = table;
        }

        @Override
        public void run() {
            int idle = dataSource.getNumIdle();
            int active = dataSource.getNumActive();
            int max = dataSource.getMaxTotal();
            int available = max - idle - active;

            graph.updateGraph(
                    new String[]{ "Active", "Available", "Idle" },
                    new GraphData<>( "pgjdbc-connection-pool-data", new Integer[]{ active, available, idle } )
            );

            table.reset();
            table.addRow( "Active", "" + active );
            table.addRow( "Idle", "" + idle );
            table.addRow( "Max", "" + max );
        }
    }
}
