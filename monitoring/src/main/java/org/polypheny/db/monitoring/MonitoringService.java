/*
 * Copyright 2019-2021 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring;


import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException.SerializationError;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.util.FileSystemManager;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


//ToDo add some kind of configuration which can for one decide on which backend to select, if we might have severall like
// * InfluxDB
// * File
// * map db
// * etc
@Slf4j
public class MonitoringService {

    public static final MonitoringService INSTANCE = new MonitoringService();
    private static final long serialVersionUID = 2312903251112906177L;

    private final String MONITORING_BACKEND = "simple"; //InfluxDB
    private BackendConnector backendConnector;
    BackendConnectorFactory backendConnectorFactory = new BackendConnectorFactory();


    private static final String FILE_PATH = "queueMapDB";
    private static DB db;

    private static final AtomicLong queueIdBuilder = new AtomicLong();
    private static BTreeMap<Long, MonitorEvent> eventQueue;

    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    private InformationTable queueOverviewTable;

    public MonitoringService(){

        initializeMonitoringBackend();

        initPersistentDBQueue();


        //Initialize Information Page
        informationPage = new InformationPage( "Monitoring Queue" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Queue Overview" );

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        queueOverviewTable = new InformationTable(
                informationGroupOverview,
                Arrays.asList( "Queue ID", "STMT", "Description", " Recorded Timestamp", "Field Names") );
        im.registerInformation( queueOverviewTable );



        // Background Task
        String taskId = BackgroundTaskManager.INSTANCE.registerTask(
                this::executeEventInQueue,
                "Add monitoring events from queue to backend",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_TEN_SECONDS
        );


    }

    private void initPersistentDBQueue() {


        if ( db != null ) {
            db.close();
        }
        synchronized ( this ) {

            File folder = FileSystemManager.getInstance().registerNewFolder( "monitoring" );

            db = DBMaker.fileDB( new File( folder, this.FILE_PATH ) )
                            .closeOnJvmShutdown()
                            .transactionEnable()
                            .fileMmapEnableIfSupported()
                            .fileMmapPreclearDisable()
                            .make();

            db.getStore().fileLoad();

            eventQueue = db.treeMap( "partitions", Serializer.LONG, Serializer.JAVA ).createOrOpen();

            try{

                restoreIdBuilder(eventQueue, queueIdBuilder);
            } catch (SerializationError e ) {
                log.error( "!!!!!!!!!!! Error while restoring the monitoring queue !!!!!!!!!!!" );
                log.error( "This usually means that there have been changes to the internal structure of the monitoring queue with the last update of Polypheny-DB." );
                log.error( "To fix this, you must reset the catalog. To do this, please ..." );
                System.exit( 1 );
            }


        }

    }

    private void restoreIdBuilder( Map<Long, ?> map, AtomicLong idBuilder ) {
        if ( !map.isEmpty() ) {
            idBuilder.set( Collections.max( map.keySet() ) + 1 );
        }
    }

    /**
     * This method faces should be used to add new items to backend
     * it should be invoked in directly
     *
     * It is backend agnostic and makes sure to parse and extract all necessary information
     * which should be added to the backend
     *
     * @param event to add to the queue which will registered as a new monitoring metric
     */
    public void addWorkloadEventToQueue(MonitorEvent event){

        long id = queueIdBuilder.getAndIncrement();

        System.out.println("\nHENNLO: Added new Worklaod event:"
                + "\n\t STMT_TYPE:" + event.monitoringType + " "
                + "\n\t Description: " + event.getDescription() + " "
                + "\n\t Timestamp " + event.getRecordedTimestamp() + " "
                + "\n\t QUEUE_ID " + id + " "
                + "\n\t Field Names " + event.getFieldNames());


        //Add event to persitent queue
        synchronized ( this ) {
            //eventQueue.put( id, event );
        }

        queueOverviewTable.addRow( id, event.monitoringType, event.getDescription(), event.getRecordedTimestamp(),event.getFieldNames() );

    }

    public void executeEventInQueue(){
        //Will be executed every 5seconds due to Background Task Manager and checks the queue and then asyncronously writes them to backend
        System.out.println("Executed Background Task at: " + new Timestamp(System.currentTimeMillis()) );
    }

    /**
     * This is currently a dummy Service mimicking the final retrieval of monitoring data
     *
     * @param type  Search for specific workload type
     * @param filter on select worklaod type
     *
     * @return some event or statistic which can be immediately used
     */
    public String getWorkloadItem(String type, String filter){
        System.out.println("HENNLO: Looking for: '" + type +"' with filter: '" + filter + "'");

        backendConnector.readStatisticEvent( " " );

        return "EMPTY WORKLOAD EVENT";
    }


    private void initializeMonitoringBackend(){
        backendConnector = backendConnectorFactory.getBackendInstance(MONITORING_BACKEND);
    }

    private class BackendConnectorFactory {

        //Returns backend based on configured statistic Backend in runtimeconfig
        public BackendConnector getBackendInstance( String statisticBackend ) {
            switch ( statisticBackend ) {
                case "InfluxDB":
                    //TODO add error handling or fallback to default backend when no Influx is available
                    return new InfluxBackendConnector();

                case "simple":
                    return new SimpleBackendConnector();

                default :
                    throw new RuntimeException( "Unknown Backend type: '" + statisticBackend  + "' ");
            }


        }

    }

}

