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
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionHandlerException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumnPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.information.Information;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphData;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphType;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskPriority;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskSchedulingType;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTaskManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class AbstractJdbcStore extends Store {

    private InformationPage informationPage;
    private InformationGroup informationGroupConnectionPool;
    private List<Information> informationElements;

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;


    public AbstractJdbcStore(
            int storeId,
            String uniqueName,
            Map<String, String> settings,
            ConnectionFactory connectionFactory,
            SqlDialect dialect ) {
        super( storeId, uniqueName, settings );
        this.connectionFactory = connectionFactory;
        this.dialect = dialect;
        // Register the JDBC Pool Size as information in the information manager
        registerJdbcPoolSizeInformation( uniqueName );
    }


    protected void registerJdbcPoolSizeInformation( String uniqueName ) {
        informationPage = new InformationPage( uniqueName, uniqueName );
        informationGroupConnectionPool = new InformationGroup( informationPage, "JDBC Connection Pool" );

        informationElements = new ArrayList<>();

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupConnectionPool );

        InformationGraph connectionPoolSizeGraph = new InformationGraph(
                informationGroupConnectionPool,
                GraphType.DOUGHNUT,
                new String[]{ "Active", "Available", "Idle" }
        );
        im.registerInformation( connectionPoolSizeGraph );
        informationElements.add( connectionPoolSizeGraph );

        InformationTable connectionPoolSizeTable = new InformationTable(
                informationGroupConnectionPool,
                Arrays.asList( "Attribute", "Value" ) );
        im.registerInformation( connectionPoolSizeTable );
        informationElements.add( connectionPoolSizeTable );

        BackgroundTaskManager.INSTANCE.registerTask(
                new ConnectionPoolSizeInfo( connectionPoolSizeGraph, connectionPoolSizeTable ),
                "Update " + uniqueName + " JDBC connection factory pool size information",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS );
    }


    @Override
    public void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name ) {
        //return new JdbcSchema( dataSource, DatabaseProduct.HSQLDB.getDialect(), new JdbcConvention( DatabaseProduct.HSQLDB.getDialect(), expression, "myjdbcconvention" ), "testdb", null, combinedSchema );
        // TODO MV: Potential bug! This only works as long as we do not cache the schema between multiple transactions
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, connectionFactory, dialect, null, this );
    }


    protected abstract String getTypeString( PolySqlType polySqlType );


    @Override
    public void createTable( Context context, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
        String physicalTableName = getPhysicalTableName( combinedTable.getTable().id );
        if ( log.isDebugEnabled() ) {
            log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), qualifiedNames, physicalTableName );
        }
        builder.append( "CREATE TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) ).append( " ( " );
        boolean first = true;
        for ( CatalogColumnPlacement placement : combinedTable.getColumnPlacementsByStore().get( getStoreId() ) ) {
            CatalogColumn catalogColumn;
            try {
                catalogColumn = context.getTransaction().getCatalog().getColumn( placement.columnId );
            } catch ( GenericCatalogException | UnknownColumnException e ) {
                throw new RuntimeException( e );
            }
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( "col" + placement.columnId ) ).append( " " );
            builder.append( getTypeString( catalogColumn.type ) );
            if ( catalogColumn.length != null ) {
                builder.append( "(" ).append( catalogColumn.length );
                if ( catalogColumn.scale != null ) {
                    builder.append( "," ).append( catalogColumn.scale );
                }
                builder.append( ")" );
            }

        }
        builder.append( " )" );
        executeUpdate( builder, context );
        // Add physical names to placements
        for ( CatalogColumnPlacement placement : combinedTable.getColumnPlacementsByStore().get( getStoreId() ) ) {
            try {
                context.getTransaction().getCatalog().updateColumnPlacementPhysicalNames(
                        getStoreId(),
                        placement.columnId,
                        "public", // TODO MV: physical schema name
                        physicalTableName,
                        "col" + placement.columnId );
            } catch ( GenericCatalogException e ) {
                throw new RuntimeException( e );
            }
        }

    }


    @Override
    public void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        String physicalTableName = getPhysicalTableName( catalogColumn.tableId );
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
        executeUpdate( builder, context );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        String physicalTableName = getPhysicalTableName( catalogColumn.tableId );
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
        executeUpdate( builder, context );
    }


    @Override
    public void dropTable( Context context, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        String physicalTableName = getPhysicalTableName( combinedTable.getTable().id );
        builder.append( "DROP TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder, context );
    }


    @Override
    public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        String physicalTableName = getPhysicalTableName( catalogColumn.tableId );
        builder.append( "ALTER TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " DROP " ).append( dialect.quoteIdentifier( catalogColumn.name ) );
        executeUpdate( builder, context );
    }


    @Override
    public void truncate( Context context, CatalogCombinedTable combinedTable ) {
        StringBuilder builder = new StringBuilder();
        String physicalTableName = getPhysicalTableName( combinedTable.getTable().id );
        builder.append( "TRUNCATE TABLE " ).append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder, context );
    }


    protected void executeUpdate( StringBuilder builder, Context context ) {
        try {
            context.getTransaction().registerInvolvedStore( this );
            connectionFactory.getOrCreateConnectionHandler( context.getTransaction().getXid() ).executeUpdate( builder.toString() );
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new RuntimeException( e );
        }
    }


    @SneakyThrows
    @Override
    public boolean prepare( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            return connectionFactory.getConnectionHandler( xid ).prepare();
        } else {
            log.warn( "There is no connection to prepare (Uniquename: {}, XID: {})! Returning true.", getUniqueName(), xid );
            return true;
        }
    }


    @SneakyThrows
    @Override
    public void commit( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).commit();
        } else {
            log.warn( "There is no connection to commit (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    @SneakyThrows
    @Override
    public void rollback( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).rollback();
        } else {
            log.warn( "There is no connection to rollback (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    protected String getPhysicalTableName( long tableId ) {
        return "tab" + tableId;
    }


    protected void removeInformationPage() {
        if ( informationElements.size() > 0 ) {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( informationElements.toArray( new Information[0] ) );
            im.removeGroup( informationGroupConnectionPool );
            im.removePage( informationPage );
        }
    }


    private class ConnectionPoolSizeInfo implements BackgroundTask {

        private final InformationGraph graph;
        private final InformationTable table;


        ConnectionPoolSizeInfo( InformationGraph graph, InformationTable table ) {
            this.graph = graph;
            this.table = table;
        }


        @Override
        public void backgroundTask() {
            int idle = connectionFactory.getNumIdle();
            int active = connectionFactory.getNumActive();
            int max = connectionFactory.getMaxTotal();
            int available = max - idle - active;

            graph.updateGraph(
                    new String[]{ "Active", "Available", "Idle" },
                    new GraphData<>( getUniqueName() + "-connection-pool-data", new Integer[]{ active, available, idle } )
            );

            table.reset();
            table.addRow( "Active", active );
            table.addRow( "Idle", idle );
            table.addRow( "Max", max );
        }
    }

}
