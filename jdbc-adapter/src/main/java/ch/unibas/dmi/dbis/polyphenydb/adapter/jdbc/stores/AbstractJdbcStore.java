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


import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.information.Information;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphData;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGraph.GraphType;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.dbcp2.BasicDataSource;


public abstract class AbstractJdbcStore extends Store {

    private InformationPage informationPage;
    private InformationGroup informationGroupConnectionPool;
    private List<Information> informationElements;


    public AbstractJdbcStore( int storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings );
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
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate( connectionPoolSizeInfo, 0, 5, TimeUnit.SECONDS );
    }


    protected void removeInformationPage() {
        if ( informationElements.size() > 0 ) {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( informationElements.toArray( new Information[0] ) );
            im.removeGroup( informationGroupConnectionPool );
            im.removePage( informationPage );
        }
    }


    private static class ConnectionPoolSizeInfo implements Runnable {

        private final InformationGraph graph;
        private final InformationTable table;
        private final BasicDataSource dataSource;


        ConnectionPoolSizeInfo( InformationGraph graph, InformationTable table, BasicDataSource dataSource ) {
            this.graph = graph;
            this.table = table;
            this.dataSource = dataSource;
        }


        @Override
        public void run() {
            int idle = dataSource.getNumIdle();
            int active = dataSource.getNumActive();
            int max = dataSource.getMaxTotal();
            int available = max - idle - active;

            graph.updateGraph(
                    new String[]{ "Active", "Available", "Idle" },
                    new GraphData<>( "hsqldb-connection-pool-data", new Integer[]{ active, available, idle } )
            );

            table.reset();
            table.addRow( "Active", "" + active );
            table.addRow( "Idle", "" + idle );
            table.addRow( "Max", "" + max );
        }
    }

}
