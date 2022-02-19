/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
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
        List<CatalogTable> periodicTables = catalog.getTablesForPeriodicProcessing();
        // Retrieve all Tables which rely on periodic processing
        for ( CatalogTable table : periodicTables ) {
            if ( table.partitionProperty.partitionType == PartitionType.TEMPERATURE ) {
                determinePartitionFrequency( table, invocationTimestamp );
            }
        }
        log.debug( "Finished processing access frequency of tables" );
    }


    private void incrementPartitionAccess( long identifiedPartitionId, List<Long> partitionIds ) {
        // Outer if is needed to ignore frequencies from old non-existing partitionIds
        // Which are not yet linked to the table but are still in monitoring
        // TODO @CEDRIC or @HENNLO introduce monitoring cleaning of data points
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
    private void determinePartitionDistribution( CatalogTable table ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Determine access frequency of partitions of table: {}", table.name );
        }

        // Get percentage of tables which can remain in HOT
        long numberOfPartitionsInHot = (table.partitionProperty.partitionIds.size() * ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageIn()) / 100;

        // These are the tables than can remain in HOT
        long allowedTablesInHot = (table.partitionProperty.partitionIds.size() * ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageOut()) / 100;

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

            List<CatalogPartition> currentHotPartitions = Catalog.INSTANCE.getPartitions( ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId() );
            for ( CatalogPartition catalogPartition : currentHotPartitions ) {

                // Remove partitions from List if they are already in HOT (not necessary to send to DataMigrator)
                if ( partitionsFromColdToHot.contains( catalogPartition.id ) ) {
                    partitionsFromColdToHot.remove( catalogPartition.id );

                } else { // If they are currently in hot but should not be placed in HOT anymore. This means that they should possibly be thrown out and placed in cold

                    if ( partitionsAllowedInHot.contains( catalogPartition.id ) ) {
                        continue;
                    } else { // place from HOT to cold
                        partitionsFromHotToCold.add( catalogPartition.id );
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
    private void redistributePartitions( CatalogTable table, List<Long> partitionsFromColdToHot, List<Long> partitionsFromHotToCold ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Execute physical redistribution of partitions for table: {}", table.name );
            log.debug( "Partitions to move from HOT to COLD: {}", partitionsFromHotToCold );
            log.debug( "Partitions to move from COLD to HOT: {}", partitionsFromColdToHot );
        }

        Map<DataStore, List<Long>> partitionsToRemoveFromStore = new HashMap<>();

        TransactionManager transactionManager = new TransactionManagerImpl();
        Transaction transaction = null;
        try {
            transaction = transactionManager.startTransaction( "pa", table.getDatabaseName(), false, "FrequencyMap" );

            Statement statement = transaction.createStatement();
            DataMigrator dataMigrator = statement.getTransaction().getDataMigrator();

            List<CatalogAdapter> adaptersWithHot = Catalog.getInstance().getAdaptersByPartitionGroup( table.id, ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId() );
            List<CatalogAdapter> adaptersWithCold = Catalog.getInstance().getAdaptersByPartitionGroup( table.id, ((TemperaturePartitionProperty) table.partitionProperty).getColdPartitionGroupId() );

            log.debug( "Get adapters to create physical tables" );
            // Validate that partition does not already exist on store
            for ( CatalogAdapter catalogAdapter : adaptersWithHot ) {
                // Skip creation/deletion because this adapter contains both groups HOT {@literal &} COLD
                if ( adaptersWithCold.contains( catalogAdapter ) ) {
                    if ( log.isDebugEnabled() ) {
                        log.debug( " Skip adapter {}, hold both partitionGroups HOT & COLD", catalogAdapter.uniqueName );
                    }
                    continue;
                }

                // First create new HOT tables
                Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );
                if ( adapter instanceof DataStore ) {
                    DataStore store = (DataStore) adapter;

                    List<Long> hotPartitionsToCreate = filterList( catalogAdapter.id, table.id, partitionsFromColdToHot );
                    //List<Long> coldPartitionsToDelete = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );

                    // If this store contains both Groups HOT {@literal &}  COLD do nothing
                    if ( hotPartitionsToCreate.size() != 0 ) {
                        Catalog.getInstance().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );

                        for ( long partitionId : hotPartitionsToCreate ) {
                            catalog.addPartitionPlacement(
                                    store.getAdapterId(),
                                    table.id,
                                    partitionId,
                                    PlacementType.AUTOMATIC,
                                    null,
                                    null );
                        }

                        store.createTable( statement.getPrepareContext(), table, hotPartitionsToCreate );

                        List<CatalogColumn> catalogColumns = new ArrayList<>();
                        catalog.getColumnPlacementsOnAdapterPerTable( store.getAdapterId(), table.id ).forEach( cp -> catalogColumns.add( catalog.getColumn( cp.columnId ) ) );

                        dataMigrator.copyData(
                                statement.getTransaction(),
                                catalog.getAdapter( store.getAdapterId() ),
                                catalogColumns,
                                hotPartitionsToCreate );

                        if ( !partitionsToRemoveFromStore.containsKey( store ) ) {
                            partitionsToRemoveFromStore.put( store, partitionsFromHotToCold );
                        } else {
                            partitionsToRemoveFromStore.replace(
                                    store,
                                    Stream.of( partitionsToRemoveFromStore.get( store ), partitionsFromHotToCold )
                                            .flatMap( Collection::stream )
                                            .collect( Collectors.toList() )
                            );
                        }
                    }
                }
            }

            for ( CatalogAdapter catalogAdapter : adaptersWithCold ) {
                // Skip creation/deletion because this adapter contains both groups HOT {@literal &}  COLD
                if ( adaptersWithHot.contains( catalogAdapter ) ) {
                    continue;
                }
                // First create new HOT tables
                Adapter adapter = AdapterManager.getInstance().getAdapter( catalogAdapter.id );
                if ( adapter instanceof DataStore ) {
                    DataStore store = (DataStore) adapter;
                    List<Long> coldPartitionsToCreate = filterList( catalogAdapter.id, table.id, partitionsFromHotToCold );
                    if ( coldPartitionsToCreate.size() != 0 ) {
                        Catalog.getInstance().getPartitionsOnDataPlacement( store.getAdapterId(), table.id );

                        for ( long partitionId : coldPartitionsToCreate ) {
                            catalog.addPartitionPlacement(
                                    store.getAdapterId(),
                                    table.id,
                                    partitionId,
                                    PlacementType.AUTOMATIC,
                                    null,
                                    null );
                        }
                        store.createTable( statement.getPrepareContext(), table, coldPartitionsToCreate );

                        List<CatalogColumn> catalogColumns = new ArrayList<>();
                        catalog.getColumnPlacementsOnAdapterPerTable( store.getAdapterId(), table.id ).forEach( cp -> catalogColumns.add( catalog.getColumn( cp.columnId ) ) );

                        dataMigrator.copyData( statement.getTransaction(), catalog.getAdapter( store.getAdapterId() ), catalogColumns, coldPartitionsToCreate );

                        if ( !partitionsToRemoveFromStore.containsKey( store ) ) {
                            partitionsToRemoveFromStore.put( store, partitionsFromColdToHot );
                        } else {
                            partitionsToRemoveFromStore.replace(
                                    store,
                                    Stream.of( partitionsToRemoveFromStore.get( store ), partitionsFromColdToHot ).flatMap( Collection::stream ).collect( Collectors.toList() )
                            );
                        }
                    }
                }
            }

            // DROP all partitions on each store

            long hotPartitionGroupId = ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId();
            long coldPartitionGroupId = ((TemperaturePartitionProperty) table.partitionProperty).getColdPartitionGroupId();

            // Update catalogInformation
            partitionsFromColdToHot.forEach( p -> Catalog.getInstance().updatePartition( p, hotPartitionGroupId ) );
            partitionsFromHotToCold.forEach( p -> Catalog.getInstance().updatePartition( p, coldPartitionGroupId ) );

            // Remove all tables that have been moved
            for ( DataStore store : partitionsToRemoveFromStore.keySet() ) {
                store.dropTable( statement.getPrepareContext(), table, partitionsToRemoveFromStore.get( store ) );
            }

            transaction.commit();
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException | TransactionException e ) {
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


    /**
     * Cleanses the List if physical partitions already resides on store. Happens if PartitionGroups HOT and COLD logically reside on same store.
     * Therefore no actual data distribution has to take place
     *
     * @param adapterId Adapter which ist subject of receiving new tables
     * @param tableId Id of temperature partitioned table
     * @param partitionsToFilter List of partitions to be filtered
     * @return The filtered and cleansed list
     */
    private List<Long> filterList( int adapterId, long tableId, List<Long> partitionsToFilter ) {
        // Remove partition from list if it's already contained on the store
        for ( long partitionId : Catalog.getInstance().getPartitionsOnDataPlacement( adapterId, tableId ) ) {
            if ( partitionsToFilter.contains( partitionId ) ) {
                partitionsToFilter.remove( partitionId );
            }
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
    public void determinePartitionFrequency( CatalogTable table, long invocationTimestamp ) {
        Timestamp queryStart = new Timestamp( invocationTimestamp - ((TemperaturePartitionProperty) table.partitionProperty).getFrequencyInterval() * 1000 );

        accessCounter = new HashMap<>();
        List<Long> tempPartitionIds = new ArrayList<>( table.partitionProperty.partitionIds );

        tempPartitionIds.forEach( p -> accessCounter.put( p, (long) 0 ) );

        switch ( ((TemperaturePartitionProperty) table.partitionProperty).getPartitionCostIndication() ) {
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
