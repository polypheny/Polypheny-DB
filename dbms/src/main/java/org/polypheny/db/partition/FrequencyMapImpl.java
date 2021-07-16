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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DMLDataPoint;
import org.polypheny.db.monitoring.events.metrics.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
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
@Slf4j
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


    @Override
    public void determineTableFrequency() {

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

        log.debug( "Start processing access frequency of tables" );
        Catalog catalog = Catalog.getInstance();

        long invocationTimestamp = System.currentTimeMillis();

        //retrieve all Tables which rely on periodic Processing
        for ( CatalogTable table: catalog.getTablesForPeriodicProcessing() ) {
            if ( table.partitionType == PartitionType.TEMPERATURE){
                determinePartitionFrequency(table, invocationTimestamp);
            }
        }
        log.debug( "Finished processing access frequency of tables" );
    }

    private void incrementPartitionAccess(long partitionId){
        accessCounter.replace( partitionId, accessCounter.get( partitionId )+1 );
    }

    private void determinePartitionDistribution(CatalogTable table){
        log.debug( "Determine access frequency of partitions of table: " + table.name );

        //Get percentage of tables which can remain in HOT
        long numberOfPartitionsInHot = ( table.partitionProperty.partitionIds.size() *  ((TemperaturePartitionProperty)table.partitionProperty).getHotAccessPercentageIn() ) / 100;

        //These are the tables than can remain in HOT
        long allowedTablesInHot = ( table.partitionProperty.partitionIds.size() *  ((TemperaturePartitionProperty)table.partitionProperty).getHotAccessPercentageOut() ) / 100;




        long thresholdValue = Long.MAX_VALUE;
        long thresholdPartitionId = -1;

        List<Long> partitionsFromColdToHot = new ArrayList<>();
        List<Long> partitionsFromHotToCold = new ArrayList<>();

        List<Long> partitionsAllowedInHot = new ArrayList<>();



        HashMap<Long, Long> descSortedMap = accessCounter
                .entrySet()
                .stream()
                .sorted( (Map.Entry.<Long,Long>comparingByValue().reversed()) )
                .collect( Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new) );


                //= new HashMap<>();
        //accessCounter.entrySet().stream().sorted(Map.Entry.comparingByValue( Comparator.reverseOrder()))
         //       .forEachOrdered( x -> descSortedMap.put( x.getKey(),x.getValue() ) );




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

        if ( !partitionsFromColdToHot.isEmpty() || !partitionsFromHotToCold.isEmpty()) {
            redistributePartitions( table, partitionsFromColdToHot, partitionsFromHotToCold );
        }
    }

    private void redistributePartitions(CatalogTable table, List<Long> partitionsFromColdToHot, List<Long> partitionsFromHotToCold){
        // Invoke DdlManager/dataMigrator to copy data with both new Lists

        log.debug( "Execute physical redistribution of partitions for table: " + table.name );
        log.debug( "Partitions to move from HOT to COLD: " + partitionsFromHotToCold );
        log.debug( "Partitions to move from COLD to HOT: " + partitionsFromColdToHot );

        Map<DataStore,List<Long>> partitionsToRemoveFromStore = new HashMap<>();

        TransactionManager transactionManager = new TransactionManagerImpl();
        Transaction transaction = null;
        try {
            transaction = transactionManager.startTransaction( "pa", table.getDatabaseName(),false,"FrequencyMap" );


            Statement statement = transaction.createStatement();

            List<CatalogAdapter> adaptersWithHot = Catalog.getInstance().getAdaptersByPartitionGroup( table.id, ((TemperaturePartitionProperty)table.partitionProperty).getHotPartitionGroupId() );
            List<CatalogAdapter> adaptersWithCold = Catalog.getInstance().getAdaptersByPartitionGroup( table.id, ((TemperaturePartitionProperty)table.partitionProperty).getColdPartitionGroupId() );

            log.debug( "Get adapters to create physical tables");
            //Validate that partition does not already exist on store
            for ( CatalogAdapter catalogAdapter : adaptersWithHot){

                // Skip creation/deletion because this adapter contains both groups HOT & COLD
                if ( adaptersWithCold.contains( catalogAdapter ) ){
                    log.debug( " Skip adapter " + catalogAdapter.uniqueName + ", hold both partitionGroups HOT & COLD" );
                    continue;
                }

                //First create new HOT tables
                Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );
                if ( adapter instanceof DataStore ) {
                    DataStore store = (DataStore) adapter;

                    List<Long> HotPartitionsToCreate = filterList( catalogAdapter.id, table.id, partitionsFromColdToHot );
                    //List<Long> coldPartitionsToDelete = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );

                    //IF this store contains both Groups HOT & COLD do nothing
                    if (HotPartitionsToCreate.size() != 0) {
                        Catalog.getInstance().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );
                        store.createTable( statement.getPrepareContext(), table, HotPartitionsToCreate );

                        if (  !partitionsToRemoveFromStore.containsKey( store )) {
                            partitionsToRemoveFromStore.put( store, partitionsFromHotToCold );
                        }else{
                            partitionsToRemoveFromStore.replace( store,
                                    Stream.of(
                                            partitionsToRemoveFromStore.get( store ),
                                            partitionsFromHotToCold )
                                    .flatMap( p -> p.stream())
                                    .collect( Collectors.toList() )
                            );
                        }

                        //store.dropTable( statement.getPrepareContext(),table, partitionsFromHotToCold );
                    }
                    //Copy data

                    //Create new COLD tables

                    //Copy data

                    //DELETE TABLEs based on moved partitions in HOT

                    //DELETE TABLEs based on moved partitions in HOT
                }
            }

            for ( CatalogAdapter catalogAdapter : adaptersWithCold) {

                // Skip creation/deletion because this adapter contains both groups HOT & COLD
                if ( adaptersWithHot.contains( catalogAdapter ) ){
                    continue;
                }
                //First create new HOT tables
                Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );
                if ( adapter instanceof DataStore ) {
                    DataStore store = (DataStore) adapter;
                    List<Long> partitionsToCreate = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );
                    if (partitionsToCreate.size() != 0) {
                        Catalog.getInstance().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );
                        store.createTable( statement.getPrepareContext(), table, partitionsToCreate );

                        if (  !partitionsToRemoveFromStore.containsKey( store )) {
                            partitionsToRemoveFromStore.put( store, partitionsFromColdToHot );
                        }else{
                            partitionsToRemoveFromStore.replace( store,
                                    Stream.of(
                                            partitionsToRemoveFromStore.get( store ),
                                            partitionsFromColdToHot )
                                            .flatMap( p -> p.stream())
                                            .collect( Collectors.toList() )
                            );
                        }

                    }
                }
            }

            //DROP all partitions on each store

            long hotPartitionGroupId = ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId();
            long coldPartitionGroupId = ((TemperaturePartitionProperty) table.partitionProperty).getColdPartitionGroupId();

            //Update catalogInformation
            partitionsFromColdToHot.forEach( p -> Catalog.getInstance().updatePartition( p, hotPartitionGroupId ) );
            partitionsFromHotToCold.forEach( p -> Catalog.getInstance().updatePartition( p, coldPartitionGroupId ) );

            //Remove all tables that have been moved
            for ( DataStore store : partitionsToRemoveFromStore.keySet()) {
                store.dropTable( statement.getPrepareContext(), table, partitionsToRemoveFromStore.get( store ) );
            }

            transaction.commit();
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException | TransactionException e ) {
            e.printStackTrace();
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Error while rolling back the transaction", e );
                }
            }
        }


    }

    private List<Long> filterList(int adapterId, long tableId, List<Long> partitionsToFilter){

        //Remove partition from list if its already contained on the store
        for ( long partitionId : Catalog.getInstance().getPartitionsOnDataPlacement( adapterId, tableId ) ) {
            if ( partitionsToFilter.contains( partitionId ) ) {
                partitionsToFilter.remove( partitionId );
            }
        }
        return partitionsToFilter;
    }


    public void determinePartitionFrequency( CatalogTable table, long invocationTimestamp ){
        Timestamp queryStart = new Timestamp(  invocationTimestamp - ((TemperaturePartitionProperty) table.partitionProperty).getFrequencyInterval()*1000 );

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
        }

        determinePartitionDistribution(table);
    }


    @Override
    public void determinePartitionFrequencyOnStore() {

    }

}
