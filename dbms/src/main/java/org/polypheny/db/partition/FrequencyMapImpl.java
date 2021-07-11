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

package org.polypheny.db.partition;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DMLDataPoint;
import org.polypheny.db.monitoring.events.metrics.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


/**
 * Periodically retrieves information from the MonitoringService to get current statistics about
 * the frequency map to determine which chunk of data should reside in HOT & which in COLD partition
 *
 * Only one instance of the MAP exists.
 * Which gets created once the first TEMPERATURE partitioned table gets created. (Including creation of BackgroundTask)
 * and consequently will be shutdown when no TEMPERATURE partitioned tables exist anymore
 */
public class FrequencyMapImpl extends FrequencyMap {

    public static FrequencyMap INSTANCE = null;

    //Make use of central configuration
    private final long checkInterval = 20; //in seconds
    private String backgroundTaskId;
    private Map<Long,Long> accessCounter = new HashMap<>();

    @Override
    public void initialize() {
        startBackgroundTask();
    }


    @Override
    public void terminate() {
        BackgroundTaskManager.INSTANCE.removeBackgroundTask( backgroundTaskId );
    }

    public static FrequencyMap getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new FrequencyMapImpl();
        }
        return INSTANCE;
    }

    private void startBackgroundTask() {
        if ( backgroundTaskId == null ) {
            backgroundTaskId = BackgroundTaskManager.INSTANCE.registerTask(
                    this::processAllPeriodicTables,
                    "Send monitoring jobs to job consumers",
                    TaskPriority.MEDIUM,
                    TaskSchedulingType.EVERY_THIRTY_SECONDS );
        }
    }

    private void processAllPeriodicTables(){

        Catalog catalog = Catalog.getInstance();

        long invocationTimestamp = System.currentTimeMillis();

        //retrieve all Tables which rely on periodic Processing
        for ( CatalogTable table: catalog.getTablesForPeriodicProcessing() ) {
            if ( table.partitionType == PartitionType.TEMPERATURE){
                determinePartitionFrequency(table, invocationTimestamp);
            }
        }

    }

    private void incrementPartitionAccess(long partitionId){
        accessCounter.replace( partitionId, accessCounter.get( partitionId )+1 );
    }

    private void redistributePartitions(CatalogTable table){

        //Get percentage of tables which can remain in HOT
        long numberOfPartitionsInHot = table.partitionProperty.partitionIds.size() * ( ((TemperaturePartitionProperty)table.partitionProperty).getHotAccessPercentageIn() / 100);

        //These are the tables than can remain in HOT
        long allowedTablesInHot = table.partitionProperty.partitionIds.size() * ( ((TemperaturePartitionProperty)table.partitionProperty).getHotAccessPercentageOut() / 100);




        long thresholdValue = Long.MAX_VALUE;
        long thresholdPartitionId = -1;

        List<Long> partitionsFromColdToHot = new ArrayList<>();
        List<Long> partitionsFromHotToCold = new ArrayList<>();

        List<Long> partitionsAllowedInHot = new ArrayList<>();



        HashMap<Long, Long> descSortedMap = new HashMap<>();
        accessCounter.entrySet().stream().sorted(Map.Entry.comparingByValue( Comparator.reverseOrder()))
                .forEachOrdered( x -> descSortedMap.put( x.getKey(),x.getValue() ) );



        //Start gathering the partitions begining with the most frequently accessed
        int hotCounter = 0;
        int toleranceCounter = 0;
        for ( Entry<Long,Long> currentEntry : descSortedMap.entrySet() ){

            //Gather until you reach getHotAccessPercentageIn() #tables
            if (hotCounter < numberOfPartitionsInHot ){
                //Tables that should be placed in HOT if not already there
                partitionsFromColdToHot.add(  currentEntry.getKey() );
                hotCounter++;
            }

            if ( toleranceCounter >= allowedTablesInHot ){
                break;
            }else {
                //Tables that can remain in HOT if they happen to be in that threshold
                partitionsAllowedInHot.add( currentEntry.getKey() );
                toleranceCounter++;
            }

        }

        //For every partition get accessValues
        /*for ( Long partitionId : table.partitionProperty.partitionIds ) {
            long tempValue = accessCounter.get( partitionId );

            //Only start replacing partitions if List (with percentage of allowed partitions) is already full
            if ( partitionsFromColdToHot.size() >=  numberOfPartitionsInHot ){

                //Swap out entries in List
                if ( tempValue > thresholdValue ){
                    partitionsFromColdToHot.remove( thresholdPartitionId );
                    partitionsFromColdToHot.add( partitionId );

                    long tempThresholdValue = Long.MAX_VALUE;
                    long tempThresholdPartitionid = -1;

                    //After replacement now find partition with lowest AccessFrequency
                    for ( long comparePartitionId : partitionsFromColdToHot ) {
                        long tempCounter = accessCounter.get( comparePartitionId );

                        if ( tempCounter < tempThresholdValue){
                            tempThresholdValue = tempCounter;
                            tempThresholdPartitionid = comparePartitionId;
                        }
                    }
                    thresholdValue = tempThresholdValue;
                    thresholdPartitionId = tempThresholdPartitionid;
                }

            }else{ //When list is not full, no need to check for constraints
                partitionsFromColdToHot.add( partitionId );

                //Update thresholdValue until list is full then start "sorting"
                if ( tempValue < thresholdValue ) {
                    thresholdValue = tempValue;
                    thresholdPartitionId = partitionId;
                }
            }
        }*/


        //Which partitions are in top X % ( to be placed in HOT)

            //Which of those are currently in cold --> action needed

        List<CatalogPartition> currentHotPartitions = Catalog.INSTANCE.getPartitions( ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId() );
        for ( CatalogPartition catalogPartition : currentHotPartitions ){

            //Remove partitions from List if they are already in HOT (not necessary to send to DataMigrator)
            if ( partitionsFromColdToHot.contains( catalogPartition.id ) ){
                partitionsFromColdToHot.remove( catalogPartition.id );

            }else{ //If they are currently in hot but should not be placed in HOT anymore. This means that they should possibly be thrown out and placed in cold

                if ( partitionsAllowedInHot.contains( catalogPartition.id )){
                    continue;
                }
                else { // place from HOT to cold
                    partitionsFromHotToCold.add( catalogPartition.id );
                }
            }

        }




        // Invoke DdlManager/dataMigrator to copy data with both new Lists


    }


    public void determineTableFrequency(){

    }

    public void determinePartitionFrequency( CatalogTable table, long invocationTimestamp ){
        Timestamp queryStart = new Timestamp(  invocationTimestamp - ((TemperaturePartitionProperty) table.partitionProperty).getFrequencyInterval() );

        accessCounter = new HashMap<>();
        table.partitionProperty.partitionIds.forEach( p -> accessCounter.put( p, (long) 0 ) );

        switch ( ((TemperaturePartitionProperty) table.partitionProperty).getPartitionCostIndication() ){
            case ALL:
                List<MonitoringDataPoint> totalAccesses = MonitoringServiceProvider.getInstance().getDataPointsAfter( MonitoringDataPoint.class, queryStart );
                for ( MonitoringDataPoint monitoringDataPoint: totalAccesses ) {
                    if ( monitoringDataPoint instanceof QueryDataPoint ) {
                        ((QueryDataPoint) monitoringDataPoint).getAccessedPartitions().forEach( p -> incrementPartitionAccess( p ) );
                    }
                    else if ( monitoringDataPoint instanceof DMLDataPoint  ){
                        ((DMLDataPoint) monitoringDataPoint).getAccessedPartitions().forEach( p -> incrementPartitionAccess( p ) );
                    }
                }

                break;

            case READ:
                List<QueryDataPoint> readAccesses= MonitoringServiceProvider.getInstance().getDataPointsAfter( QueryDataPoint.class, queryStart );
                for ( QueryDataPoint queryDataPoint: readAccesses ) {
                    queryDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p ) );
                }
                break;

            case WRITE:
                List<DMLDataPoint> writeAccesses= MonitoringServiceProvider.getInstance().getDataPointsAfter( DMLDataPoint.class, queryStart );
                for ( DMLDataPoint dmlDataPoint: writeAccesses ) {
                    dmlDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p ) );
                }
                break;
        }
        redistributePartitions(table);
    }

    public void determinePartitionFrequencyOnStore(){

    }

}
