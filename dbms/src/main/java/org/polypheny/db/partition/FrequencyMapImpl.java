/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.Collection;
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
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.partition.properties.PartitionProperty;
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
 * the frequency map to determine which chunk of data should reside in HOT {@literal &}  which in COLD partition
 *
 * Only one instance of the MAP exists.
 * Which gets created once the first TEMPERATURE partitioned table gets created. (Including creation of BackgroundTask)
 * and consequently will be shutdown when no TEMPERATURE partitioned tables exist anymore
 */
@Slf4j
public class FrequencyMapImpl extends FrequencyMap {

    public static FrequencyMap INSTANCE = null;

    private TransactionManager transactionManager = TransactionManagerImpl.getInstance();

    private final Catalog catalog;

    // Make use of central configuration
    private String backgroundTaskId;
    private Map<Long, Long> accessCounter = new HashMap<>();


    public FrequencyMapImpl( Catalog catalog ) {
        this.catalog = catalog;
    }


    /**
     * Initializes the periodic frequency check by starting a background task.
     * Which gathers frequency related access information on
     */
    @Override
    public void initialize() {
        startBackgroundTask();
    }


    /**
     * Stops all background processing and disables the accumulation of frequency related access information.
     */
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


    /**
     * Retrieves all tables which require periodic processing and starts the access frequency process
     */
    private void processAllPeriodicTables() {
        log.debug( "Start processing access frequency of tables" );
        Catalog catalog = Catalog.getInstance();

        long invocationTimestamp = System.currentTimeMillis();
        List<LogicalTable> periodicTables = catalog.getSnapshot().getTablesForPeriodicProcessing();
        // Retrieve all Tables which rely on periodic processing
        for ( LogicalTable table : periodicTables ) {
            if ( catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow().partitionType == PartitionType.TEMPERATURE ) {
                determinePartitionFrequency( table, invocationTimestamp );
            }
        }
        log.debug( "Finished processing access frequency of tables" );
    }


    private void incrementPartitionAccess( long identifiedPartitionId, List<Long> partitionIds ) {
        // Outer if is needed to ignore frequencies from old non-existing partitionIds
        // Which are not yet linked to the table but are still in monitoring
        if ( partitionIds.contains( identifiedPartitionId ) ) {
            if ( accessCounter.containsKey( identifiedPartitionId ) ) {
                accessCounter.replace( identifiedPartitionId, accessCounter.get( identifiedPartitionId ) + 1 );
            } else {
                accessCounter.put( identifiedPartitionId, (long) 1 );
            }
        }
    }


    /**
     * Determines the partition distribution for temperature partitioned tables by deciding which partitions should be moved from HOT to COLD
     * and from COLD to HOT. To setup the table corresponding to the current access frequencies patterns.
     *
     * @param table Temperature partitioned Table
     */
    private void determinePartitionDistribution( LogicalTable table ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Determine access frequency of partitions of table: {}", table.name );
        }

        PartitionProperty property = catalog.getSnapshot().alloc().getPartitionProperty( table.id ).orElseThrow();

        // Get percentage of tables which can remain in HOT
        long numberOfPartitionsInHot = ((long) property.partitionIds.size() * ((TemperaturePartitionProperty) property).getHotAccessPercentageIn()) / 100;

        // These are the tables than can remain in HOT
        long allowedTablesInHot = ((long) property.partitionIds.size() * ((TemperaturePartitionProperty) property).getHotAccessPercentageOut()) / 100;

        if ( numberOfPartitionsInHot == 0 ) {
            numberOfPartitionsInHot = 1;
        }
        if ( allowedTablesInHot == 0 ) {
            allowedTablesInHot = 1;
        }

        List<Long> partitionsFromColdToHot = new ArrayList<>();
        List<Long> partitionsFromHotToCold = new ArrayList<>();

        List<Long> partitionsAllowedInHot = new ArrayList<>();

        HashMap<Long, Long> descSortedMap = accessCounter
                .entrySet()
                .stream()
                .sorted( (Map.Entry.<Long, Long>comparingByValue().reversed()) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue, ( e1, e2 ) -> e1, LinkedHashMap::new ) );

        //Start gathering the partitions beginning with the most frequently accessed
        int hotCounter = 0;
        int toleranceCounter = 0;
        boolean skip = false;
        boolean firstRound = true;
        for ( Entry<Long, Long> currentEntry : descSortedMap.entrySet() ) {
            if ( currentEntry.getValue() == 0 ) {
                if ( firstRound ) {
                    skip = true;
                }
                break;
            }
            firstRound = false;
            // Gather until you reach getHotAccessPercentageIn() #tables
            if ( hotCounter < numberOfPartitionsInHot ) {
                //Tables that should be placed in HOT if not already there
                partitionsFromColdToHot.add( currentEntry.getKey() );
                hotCounter++;

            }

            if ( toleranceCounter >= allowedTablesInHot ) {
                break;
            } else {
                // Tables that can remain in HOT if they happen to be in that threshold
                partitionsAllowedInHot.add( currentEntry.getKey() );
                toleranceCounter++;
            }
        }

        if ( !skip ) {
            // Which partitions are in top X % (to be placed in HOT)

            // Which of those are currently in cold --> action needed

            List<AllocationPartition> currentHotPartitions = Catalog.getInstance().getSnapshot().alloc().getPartitions( ((TemperaturePartitionProperty) property).getHotPartitionGroupId() );
            for ( AllocationPartition logicalPartition : currentHotPartitions ) {

                // Remove partitions from List if they are already in HOT (not necessary to send to DataMigrator)
                if ( partitionsFromColdToHot.contains( logicalPartition.id ) ) {
                    partitionsFromColdToHot.remove( logicalPartition.id );

                } else { // If they are currently in hot but should not be placed in HOT anymore. This means that they should possibly be thrown out and placed in cold

                    if ( partitionsAllowedInHot.contains( logicalPartition.id ) ) {
                        continue;
                    } else { // place from HOT to cold
                        partitionsFromHotToCold.add( logicalPartition.id );
                    }
                }

            }

            if ( (!partitionsFromColdToHot.isEmpty() || !partitionsFromHotToCold.isEmpty()) ) {
                redistributePartitions( table, partitionsFromColdToHot, partitionsFromHotToCold );
            }
        }
    }


    /**
     * Physically executes the data redistribution of the specific internal partitions and consequently creates new physical tables
     * as well as removing tables which are not needed anymore.
     *
     * @param table Temperature partitioned table
     * @param partitionsFromColdToHot Partitions which should be moved from COLD to HOT PartitionGroup
     * @param partitionsFromHotToCold Partitions which should be moved from HOT to COLD PartitionGroup
     */
    private void redistributePartitions( LogicalTable table, List<Long> partitionsFromColdToHot, List<Long> partitionsFromHotToCold ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Execute physical redistribution of partitions for table: {}", table.name );
            log.debug( "Partitions to move from HOT to COLD: {}", partitionsFromHotToCold );
            log.debug( "Partitions to move from COLD to HOT: {}", partitionsFromColdToHot );
        }

        Map<DataStore<?>, List<Long>> partitionsToRemoveFromStore = new HashMap<>();

        Transaction transaction = null;
        try {
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "FrequencyMap" );

            Statement statement = transaction.createStatement();
            DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();
            Snapshot snapshot = transaction.getSnapshot();

            PartitionProperty property = snapshot.alloc().getPartitionProperty( table.id ).orElseThrow();

            List<LogicalAdapter> adaptersWithHot = snapshot.alloc().getAdaptersByPartitionGroup( table.id, ((TemperaturePartitionProperty) property).getHotPartitionGroupId() );
            List<LogicalAdapter> adaptersWithCold = snapshot.alloc().getAdaptersByPartitionGroup( table.id, ((TemperaturePartitionProperty) property).getColdPartitionGroupId() );

            log.debug( "Get adapters to create physical tables" );
            // Validate that partition does not already exist on storeId
            for ( LogicalAdapter logicalAdapter : adaptersWithHot ) {
                // Skip creation/deletion because this adapter contains both groups HOT {@literal &} COLD
                if ( adaptersWithCold.contains( logicalAdapter ) ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Skip adapter {}, hold both partitionGroups HOT & COLD", logicalAdapter.uniqueName );
                    }
                    continue;
                }

                // First create new HOT tables
                createHotTables( table, partitionsFromColdToHot, partitionsFromHotToCold, partitionsToRemoveFromStore, statement, dataMigrator, logicalAdapter );
            }

            for ( LogicalAdapter logicalAdapter : adaptersWithCold ) {
                // Skip creation/deletion because this adapter contains both groups HOT {@literal &}  COLD
                if ( adaptersWithHot.contains( logicalAdapter ) ) {
                    continue;
                }
                // First create new HOT tables
                createHotTables( table, partitionsFromHotToCold, partitionsFromColdToHot, partitionsToRemoveFromStore, statement, dataMigrator, logicalAdapter );
            }

            // DROP all partitions on each storeId
            long hotPartitionGroupId = ((TemperaturePartitionProperty) property).getHotPartitionGroupId();
            long coldPartitionGroupId = ((TemperaturePartitionProperty) property).getColdPartitionGroupId();

            // Update catalogInformation
            partitionsFromColdToHot.forEach( p -> Catalog.getInstance().getAllocRel( table.namespaceId ).updatePartition( p, hotPartitionGroupId ) );
            partitionsFromHotToCold.forEach( p -> Catalog.getInstance().getAllocRel( table.namespaceId ).updatePartition( p, coldPartitionGroupId ) );

            // Remove all tables that have been moved
            for ( DataStore<?> store : partitionsToRemoveFromStore.keySet() ) {
                store.dropTable( statement.getPrepareContext(), -1 );
            }

            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Error while reassigning new location for temperature-based partitions", e );
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Error while rolling back the transaction", e );
                }
            }
        }
    }


    private void createHotTables( LogicalTable table, List<Long> partitionsFromColdToHot, List<Long> partitionsFromHotToCold, Map<DataStore<?>, List<Long>> partitionsToRemoveFromStore, Statement statement, DataMigrator dataMigrator, LogicalAdapter logicalAdapter ) {
        Adapter<?> adapter = AdapterManager.getInstance().getAdapter( logicalAdapter.id ).orElseThrow();
        if ( adapter instanceof DataStore<?> store ) {

            List<Long> hotPartitionsToCreate = filterList( table.namespaceId, logicalAdapter.id, table.id, partitionsFromColdToHot );
            //List<Long> coldPartitionsToDelete = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );

            // If this storeId contains both Groups HOT {@literal &}  COLD do nothing
            if ( !hotPartitionsToCreate.isEmpty() ) {
                Catalog.getInstance().getSnapshot().alloc().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );


                store.createTable( statement.getPrepareContext(), LogicalTableWrapper.of( null, null, null ), AllocationTableWrapper.of( null, null ) );

                List<LogicalColumn> logicalColumns = new ArrayList<>();
                catalog.getSnapshot().alloc().getColumnPlacementsOnAdapterPerEntity( store.getAdapterId(), table.id ).forEach( cp -> logicalColumns.add( catalog.getSnapshot().rel().getColumn( cp.columnId ).orElseThrow() ) );

                AllocationPlacement placement = catalog.getSnapshot().alloc().getPlacement( store.getAdapterId(), table.id ).orElseThrow();

                for ( long id : hotPartitionsToCreate ) {
                    AllocationEntity entity = Catalog.snapshot().alloc().getAlloc( placement.id, id ).orElseThrow();
                    dataMigrator.copyData(
                            statement.getTransaction(),
                            catalog.getSnapshot().getAdapter( store.getAdapterId() ).orElseThrow(),
                            table,
                            logicalColumns,
                            entity );
                    /*hotPartitionsToCreate*/
                }

                if ( !partitionsToRemoveFromStore.containsKey( store ) ) {
                    partitionsToRemoveFromStore.put( store, partitionsFromHotToCold );
                } else {
                    partitionsToRemoveFromStore.replace(
                            store,
                            Stream.of( partitionsToRemoveFromStore.get( store ), partitionsFromHotToCold )
                                    .flatMap( Collection::stream )
                                    .toList()
                    );
                }
            }
        }
    }


    /**
     * Cleanses the List if physical partitions already resides on storeId. Happens if PartitionGroups HOT and COLD logically reside on same storeId.
     * Therefore no actual data distribution has to take place
     *
     * @param namespaceId
     * @param adapterId Adapter which ist subject of receiving new tables
     * @param tableId Id of temperature partitioned table
     * @param partitionsToFilter List of partitions to be filtered
     * @return The filtered and cleansed list
     */
    private List<Long> filterList( long namespaceId, long adapterId, long tableId, List<Long> partitionsToFilter ) {
        // Remove partition from list if it's already contained on the storeId
        for ( long partitionId : Catalog.getInstance().getSnapshot().alloc().getPartitionsOnDataPlacement( adapterId, tableId ) ) {
            partitionsToFilter.remove( partitionId );
        }
        return partitionsToFilter;
    }


    /**
     * Determines the partition frequency for each partition of a temperature partitioned table based on the chosen Cost Indication (ALL, WRITE,READ)
     * in a desired time interval.
     *
     * @param table Temperature partitioned table
     * @param invocationTimestamp Timestamp do determine the interval for which monitoring metrics should be collected.
     */
    @Override
    public void determinePartitionFrequency( LogicalTable table, long invocationTimestamp ) {
        Snapshot snapshot = catalog.getSnapshot();
        PartitionProperty property = snapshot.alloc().getPartitionProperty( table.id ).orElseThrow();
        Timestamp queryStart = new Timestamp( invocationTimestamp - ((TemperaturePartitionProperty) property).getFrequencyInterval() * 1000 );

        accessCounter = new HashMap<>();
        List<Long> tempPartitionIds = new ArrayList<>( property.partitionIds );

        tempPartitionIds.forEach( p -> accessCounter.put( p, (long) 0 ) );

        switch ( ((TemperaturePartitionProperty) property).getPartitionCostIndication() ) {
            case ALL:
                for ( QueryDataPointImpl queryDataPoint : MonitoringServiceProvider.getInstance().getDataPointsAfter( QueryDataPointImpl.class, queryStart ) ) {
                    queryDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds ) );
                }
                for ( DmlDataPoint dmlDataPoint : MonitoringServiceProvider.getInstance().getDataPointsAfter( DmlDataPoint.class, queryStart ) ) {
                    dmlDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds ) );
                }

                break;

            case READ:
                List<QueryDataPointImpl> readAccesses = MonitoringServiceProvider.getInstance().getDataPointsAfter( QueryDataPointImpl.class, queryStart );
                for ( QueryDataPointImpl queryDataPoint : readAccesses ) {
                    queryDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds ) );
                }
                break;

            case WRITE:
                List<DmlDataPoint> writeAccesses = MonitoringServiceProvider.getInstance().getDataPointsAfter( DmlDataPoint.class, queryStart );
                for ( DmlDataPoint dmlDataPoint : writeAccesses ) {
                    dmlDataPoint.getAccessedPartitions().forEach( p -> incrementPartitionAccess( p, tempPartitionIds ) );
                }
        }

        // To gain observability
        // Update infoPage here
        determinePartitionDistribution( table );
    }

}
