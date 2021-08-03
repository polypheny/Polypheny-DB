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


import static java.util.stream.Collectors.toCollection;

import java.sql.Timestamp;
import java.util.ArrayList;
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
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DMLDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.processing.DataMigrator;
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

    private final Catalog catalog;

    //Make use of central configuration
    private final long checkInterval = 20; //in seconds
    private String backgroundTaskId;
    private Map<Long,Long> accessCounter = new HashMap<>();

    public FrequencyMapImpl(Catalog catalog){ this.catalog = catalog; }

    @Override
    public void initialize() {
        startBackgroundTask();
    }


    @Override
    public void terminate() {
        BackgroundTaskManager.INSTANCE.removeBackgroundTask( backgroundTaskId );
    }


    private void startBackgroundTask() {
        if ( backgroundTaskId == null ) {
            backgroundTaskId = BackgroundTaskManager.INSTANCE.registerTask(
                    this::processAllPeriodicTables,
                    "Send monitoring jobs to job consumers",
                    TaskPriority.MEDIUM,
                    (TaskSchedulingType) RuntimeConfig.TEMPERATURE_FREQUENCY_PROCESSING_INTERVAL.getEnum() );
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

    private void incrementPartitionAccess( long partitionId, List<Long> partitionIds ){

        //Outer of is needed to ignore frequencies from old non-existing partitionIds
        //Which are not yet linked to the table but are still in monitoring
        //TODO @CEDRIC or @HENNLO introduce monitoring cleanisng of datapoints
        if ( partitionIds.contains( partitionId ) ) {
            if ( accessCounter.containsKey( partitionId ) ) {
                accessCounter.replace( partitionId, accessCounter.get( partitionId ) + 1 );
            } else {
                accessCounter.put( partitionId, (long) 1 );
            }
        }
    }

    private void determinePartitionDistribution(CatalogTable table) {
        log.debug( "Determine access frequency of partitions of table: " + table.name );

        //Get percentage of tables which can remain in HOT
        long numberOfPartitionsInHot = (table.partitionProperty.partitionIds.size() * ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageIn()) / 100;

        //These are the tables than can remain in HOT
        long allowedTablesInHot = (table.partitionProperty.partitionIds.size() * ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageOut()) / 100;

        if ( numberOfPartitionsInHot == 0 ) {
            numberOfPartitionsInHot = 1;
        }
        if ( allowedTablesInHot == 0 ) {
            allowedTablesInHot = 1;
        }

        long thresholdValue = Long.MAX_VALUE;
        long thresholdPartitionId = -1;

        List<Long> partitionsFromColdToHot = new ArrayList<>();
        List<Long> partitionsFromHotToCold = new ArrayList<>();

        List<Long> partitionsAllowedInHot = new ArrayList<>();

        HashMap<Long, Long> descSortedMap = accessCounter
                .entrySet()
                .stream()
                .sorted( (Map.Entry.<Long, Long>comparingByValue().reversed()) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, ( e1, e2 ) -> e1, LinkedHashMap::new ) );


        //Start gathering the partitions begining with the most frequently accessed
        int hotCounter = 0;
        int toleranceCounter = 0;
        boolean skip =false;
        boolean firstRound = true;
        for ( Entry<Long, Long> currentEntry : descSortedMap.entrySet() ) {

            if ( currentEntry.getValue() == 0 ) {
                if ( firstRound ) {
                    skip = true;
                }
                break;
            }
            firstRound = false;
            //Gather until you reach getHotAccessPercentageIn() #tables
            if ( hotCounter < numberOfPartitionsInHot ) {
                //Tables that should be placed in HOT if not already there
                partitionsFromColdToHot.add( currentEntry.getKey() );
                hotCounter++;

            }

            if ( toleranceCounter >= allowedTablesInHot ) {
                break;
            } else {
                //Tables that can remain in HOT if they happen to be in that threshold
                partitionsAllowedInHot.add( currentEntry.getKey() );
                toleranceCounter++;
            }

        }

        if( !skip ){
            //Which partitions are in top X % ( to be placed in HOT)

            //Which of those are currently in cold --> action needed

            List<CatalogPartition> currentHotPartitions = Catalog.INSTANCE.getPartitions( ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId() );
            for ( CatalogPartition catalogPartition : currentHotPartitions ) {

                //Remove partitions from List if they are already in HOT (not necessary to send to DataMigrator)
                if ( partitionsFromColdToHot.contains( catalogPartition.id ) ) {
                    partitionsFromColdToHot.remove( catalogPartition.id );

                } else { //If they are currently in hot but should not be placed in HOT anymore. This means that they should possibly be thrown out and placed in cold

                    if ( partitionsAllowedInHot.contains( catalogPartition.id ) ) {
                        continue;
                    } else { // place from HOT to cold
                        partitionsFromHotToCold.add( catalogPartition.id );
                    }
                }

            }

            if ( ( !partitionsFromColdToHot.isEmpty() || !partitionsFromHotToCold.isEmpty() ) ){
                redistributePartitions( table, partitionsFromColdToHot, partitionsFromHotToCold );
            }
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
            DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();


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

                    List<Long> hotPartitionsToCreate = filterList( catalogAdapter.id, table.id, partitionsFromColdToHot );
                    //List<Long> coldPartitionsToDelete = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );

                    //IF this store contains both Groups HOT & COLD do nothing
                    if (hotPartitionsToCreate.size() != 0) {
                        Catalog.getInstance().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );

                        for ( long partitionId: hotPartitionsToCreate ){
                            catalog.addPartitionPlacement(
                                    store.getAdapterId(),
                                    table.id,
                                    partitionId,
                                    PlacementType.AUTOMATIC,
                                    null,
                                    null);
                        }

                        store.createTable( statement.getPrepareContext(), table, hotPartitionsToCreate );

                        List<CatalogColumn> catalogColumns =  new ArrayList<>();
                        catalog.getColumnPlacementsOnAdapterPerTable( store.getAdapterId(), table.id ).forEach( cp -> catalogColumns.add( catalog.getColumn( cp.columnId ) ) );

                        dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( store.getAdapterId() ), catalogColumns, hotPartitionsToCreate);

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
                    }
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
                    List<Long> coldPartitionsToCreate = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );
                    if (coldPartitionsToCreate.size() != 0) {
                        Catalog.getInstance().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );


                        for ( long partitionId: coldPartitionsToCreate ){
                            catalog.addPartitionPlacement(
                                    store.getAdapterId(),
                                    table.id,
                                    partitionId,
                                    PlacementType.AUTOMATIC,
                                    null,
                                    null);
                        }
                        store.createTable( statement.getPrepareContext(), table, coldPartitionsToCreate );

                        List<CatalogColumn> catalogColumns =  new ArrayList<>();
                        catalog.getColumnPlacementsOnAdapterPerTable( store.getAdapterId(), table.id ).forEach( cp -> catalogColumns.add( catalog.getColumn( cp.columnId ) ) );

                        dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( store.getAdapterId() ), catalogColumns, coldPartitionsToCreate);

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
        List<Long> tempPartitionIds = table.partitionProperty.partitionIds.stream().collect(toCollection(ArrayList::new));;
        tempPartitionIds.forEach( p -> accessCounter.put( p, (long) 0 ) );

        switch ( ((TemperaturePartitionProperty) table.partitionProperty).getPartitionCostIndication() ){
            case ALL:
                for ( QueryDataPoint queryDataPoint: MonitoringServiceProvider.getInstance().getDataPointsAfter( QueryDataPoint.class, queryStart ) ) {
                    queryDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds) );
                }
                for ( DMLDataPoint dmlDataPoint: MonitoringServiceProvider.getInstance().getDataPointsAfter( DMLDataPoint.class, queryStart ) ) {
                    dmlDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds) );
                }

                break;

            case READ:
                List<QueryDataPoint> readAccesses= MonitoringServiceProvider.getInstance().getDataPointsAfter( QueryDataPoint.class, queryStart );
                for ( QueryDataPoint queryDataPoint: readAccesses ) {
                    queryDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds) );
                }
                break;

            case WRITE:
                List<DMLDataPoint> writeAccesses= MonitoringServiceProvider.getInstance().getDataPointsAfter( DMLDataPoint.class, queryStart );
                for ( DMLDataPoint dmlDataPoint: writeAccesses ) {
                    dmlDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds) );
                }
        }

        //TODO @HENNLO  create a new monitoring page to give information what partitions are currently placed in hot and with which frequencies.
        //To gain observability
        //Update infoPage here
        determinePartitionDistribution(table);
    }

}
