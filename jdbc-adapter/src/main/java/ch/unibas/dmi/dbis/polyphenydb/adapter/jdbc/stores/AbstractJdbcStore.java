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


import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcPhysicalNameProvider;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionHandlerException;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.TransactionalConnectionFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.XaConnectionFactory;
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
import java.util.List;
import java.util.Map;
import javax.sql.XADataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;


@Slf4j
public abstract class AbstractJdbcStore extends Store {

    private InformationPage informationPage;
    private InformationGroup informationGroupConnectionPool;
    private List<Information> informationElements;

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;


    public AbstractJdbcStore( int storeId, String uniqueName, Map<String, String> settings, BasicDataSource dataSource, SqlDialect dialect ) {
        super( storeId, uniqueName, settings );
        this.connectionFactory = new TransactionalConnectionFactory( dataSource );
        this.dialect = dialect;
        // Register the JDBC Pool Size as information in the information manager
        registerJdbcPoolSizeInformation( uniqueName, dataSource );
    }


    public AbstractJdbcStore( int storeId, String uniqueName, Map<String, String> settings, XADataSource dataSource, SqlDialect dialect ) {
        super( storeId, uniqueName, settings );
        this.connectionFactory = new XaConnectionFactory( dataSource );
        this.dialect = dialect;
        // TODO MV: Register the JDBC Pool Size as information in the information manager
        //registerJdbcPoolSizeInformation( uniqueName, dataSource. );
    }


    protected void registerJdbcPoolSizeInformation( String uniqueName, BasicDataSource dataSource ) {
        informationPage = new InformationPage( uniqueName, uniqueName );
        informationGroupConnectionPool = new InformationGroup( informationPage, uniqueName + " JDBC Connection Pool" );

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

        ConnectionPoolSizeInfo connectionPoolSizeInfo = new ConnectionPoolSizeInfo( connectionPoolSizeGraph, connectionPoolSizeTable, dataSource );
        BackgroundTaskManager.INSTANCE.registerTask( connectionPoolSizeInfo, "Update " + uniqueName + " JDBC conncetion pool size information", TaskPriority.LOW, TaskSchedulingType.EVERY_FIVE_SECONDS );
    }


    @Override
    public void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name ) {
        //return new JdbcSchema( dataSource, DatabaseProduct.HSQLDB.getDialect(), new JdbcConvention( DatabaseProduct.HSQLDB.getDialect(), expression, "myjdbcconvention" ), "testdb", null, combinedSchema );
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, connectionFactory, dialect, null, null, new JdbcPhysicalNameProvider( transaction.getCatalog() ) ); // TODO MV: Potential bug! This only works as long as we do not cache the schema between multiple transactions
    }


    protected void executeUpdate( StringBuilder builder, Context context ) {
        try {
            connectionFactory.getOrCreateConnectionHandler( context.getTransaction().getXid() ).executeUpdate( builder.toString() );
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        // TODO: implement
        log.warn( "prepare() is not implemented yet (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        return true;
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


    protected void removeInformationPage() {
        if ( informationElements.size() > 0 ) {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( informationElements.toArray( new Information[0] ) );
            im.removeGroup( informationGroupConnectionPool );
            im.removePage( informationPage );
        }
    }


    private static class ConnectionPoolSizeInfo implements BackgroundTask {

        private final InformationGraph graph;
        private final InformationTable table;
        private final BasicDataSource dataSource;


        ConnectionPoolSizeInfo( InformationGraph graph, InformationTable table, BasicDataSource dataSource ) {
            this.graph = graph;
            this.table = table;
            this.dataSource = dataSource;
        }


        @Override
        public void backgroundTask() {
            int idle = dataSource.getNumIdle();
            int active = dataSource.getNumActive();
            int max = dataSource.getMaxTotal();
            int available = max - idle - active;

            graph.updateGraph(
                    new String[]{ "Active", "Available", "Idle" },
                    new GraphData<>( "hsqldb-connection-pool-data", new Integer[]{ active, available, idle } )
            );

            table.reset();
            table.addRow( "Active", active );
            table.addRow( "Idle", idle );
            table.addRow( "Max", max );
        }
    }

}
