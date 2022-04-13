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

package org.polypheny.db.routing.routers;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.BatchIterator;
import org.polypheny.db.algebra.core.ConditionalExecute;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.replication.freshness.FreshnessManager;
import org.polypheny.db.replication.freshness.FreshnessManagerImpl;
import org.polypheny.db.replication.freshness.exceptions.InsufficientFreshnessOptionsException;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.EntityAccessMap;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;


/**
 * Abstract router for DQL routing.
 * It will create alg builders and select column placements per table.
 * There are three abstract methods:
 * handleHorizontalPartitioning
 * handleVerticalPartitioning
 * handleNonePartitioning
 * Every table will be checked by the following order:
 * first if partitioned horizontally {@code ->} handleHorizontalPartitioning called.
 * second if partitioned vertically or replicated {@code ->} handleVerticalPartitioningOrReplication
 * third, all other cases {@code ->} handleNonePartitioning
 */
@Slf4j
public abstract class AbstractDqlRouter extends BaseRouter implements Router {

    /**
     * Boolean which cancel routing plan generation.
     *
     * In Universal Routing, not every router needs to propose a plan for every query. But, every router will be asked to try
     * to prepare a plan for the query. If a router sees, that he is not able to route (maybe after tablePlacement 2 out of 4,
     * the router is expected to return an empty list of proposed routing plans. To "abort" routing plan creation, the boolean
     * cancelQuery is introduced and will be checked during creation. As we are in the middle of traversing a AlgNode, this
     * is the new to "abort" traversing the AlgNode.
     */
    protected boolean cancelQuery = false;

    boolean useFreshness = false;
    protected FreshnessManager freshnessManager = new FreshnessManagerImpl();


    /**
     * Abstract methods which will determine routing strategy. Not implemented in abstract class.
     * If implementing router is not supporting one of the three methods, empty list can be returned and cancelQuery boolean set to false.
     */
    protected abstract List<RoutedAlgBuilder> handleHorizontalPartitioning(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation );

    protected abstract List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation );

    protected abstract List<RoutedAlgBuilder> handleNonePartitioning(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation );


    /**
     * Abstract router only routes DQL queries.
     */
    @Override
    public List<RoutedAlgBuilder> route( AlgRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation ) {
        // Reset cancel query this run
        this.cancelQuery = false;

        if ( logicalRoot.alg instanceof LogicalTableModify ) {
            throw new IllegalStateException( "Should never happen for DML" );
        } else if ( logicalRoot.alg instanceof ConditionalExecute ) {
            throw new IllegalStateException( "Should never happen for conditional executes" );
        } else if ( logicalRoot.alg instanceof BatchIterator ) {
            throw new IllegalStateException( "Should never happen for Iterator" );
        } else {
            RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
            List<RoutedAlgBuilder> routedAlgBuilders = buildDql(
                    logicalRoot.alg,
                    Lists.newArrayList( builder ),
                    statement,
                    logicalRoot.alg.getCluster(),
                    queryInformation );
            return routedAlgBuilders;
        }
    }


    @Override
    public void resetCaches() {
        joinedTableScanCache.invalidateAll();
    }


    protected List<RoutedAlgBuilder> buildDql( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( node instanceof SetOp ) {
            if ( node instanceof Union ) {
                // unions can have more than one child
                return buildUnion( node, builders, statement, cluster, queryInformation );
            } else {
                return buildSetOp( node, builders, statement, cluster, queryInformation );
            }
        } else {
            return buildSelect( node, builders, statement, cluster, queryInformation );
        }
    }


    protected List<RoutedAlgBuilder> buildSelect( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builders = this.buildDql( node.getInput( i ), builders, statement, cluster, queryInformation );
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            AlgOptTableImpl table = (AlgOptTableImpl) node.getTable();

            if ( !(table.getTable() instanceof LogicalTable) ) {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }

            LogicalTable logicalTable = ((LogicalTable) table.getTable());
            CatalogTable catalogTable = catalog.getTable( logicalTable.getTableId() );

            // Consider Freshness only when specified in query or table supports it
            if ( statement.getTransaction().acceptsOutdatedCopies() ) {
                statement.getTransaction().setUseCache( false );
                if ( catalog.doesTableSupportOutdatedPlacements( catalogTable.id ) ) {

                    try {

                        return handleFreshness( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );
                    }
                    // If freshness cannot be provided
                    catch ( InsufficientFreshnessOptionsException e ) {

                        // No need to select specific placements, just carry out the regular routing process.

                        // Apply locking if necessary
                        // Freshness retrieval has failed so continue with regular locking
                        acquireLock( node, statement, queryInformation );

                        //TODO @HENNLO Depending on the strategy check if the transaction has to be aborted
                    }
                } else {
                    log.debug( "Freshness-Query has been specified. However this table does not contain any REFRESHABLE Placements. Everything is updated eagerly" );
                    log.debug( "There are no outdated placements that can be used. Redirecting request to primary." );
                }
            }

            // Check if table is even horizontal partitioned
            if ( catalogTable.partitionProperty.isPartitioned ) {
                return handleHorizontalPartitioning( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );

            } else {
                // At the moment multiple strategies
                if ( catalogTable.dataPlacements.size() > 1 ) {
                    return handleVerticalPartitioningOrReplication( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );
                }
                return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
            }

        } else if ( node instanceof LogicalValues ) {
            return Lists.newArrayList( super.handleValues( (LogicalValues) node, builders ) );
        } else {
            return Lists.newArrayList( super.handleGeneric( node, builders ) );
        }
    }


    protected List<RoutedAlgBuilder> buildSetOp( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, statement, cluster, queryInformation );

        RoutedAlgBuilder builder0 = RoutedAlgBuilder.create( statement, cluster );
        RoutedAlgBuilder b0 = buildDql( node.getInput( 1 ), Lists.newArrayList( builder0 ), statement, cluster, queryInformation ).get( 0 );

        builders.forEach(
                builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek(), b0.build() ) ) )
        );

        return builders;
    }


    protected List<RoutedAlgBuilder> buildUnion( AlgNode node, List<RoutedAlgBuilder> builders, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, statement, cluster, queryInformation );

        RoutedAlgBuilder builder0 = RoutedAlgBuilder.create( statement, cluster );
        List<RoutedAlgBuilder> b0s = new ArrayList<>();
        for ( AlgNode input : node.getInputs().subList( 1, node.getInputs().size() ) ) {
            b0s.add( buildDql( input, Lists.newArrayList( builder0 ), statement, cluster, queryInformation ).get( 0 ) );
        }

        builders.forEach(
                builder -> builder.replaceTop( node.copy(
                        node.getTraitSet(),
                        ImmutableList.copyOf(
                                Stream.concat(
                                                Stream.of( builder.peek() ),
                                                b0s.stream().map( AlgBuilder::build ) )
                                        .collect( Collectors.toList() ) ) ) )
        );

        return builders;
    }


    private Map<Long, List<CatalogPartitionPlacement>> filterFreshnessPlacements( AlgNode node, CatalogTable catalogTable, LogicalQueryInformation queryInformation, Statement statement ) throws InsufficientFreshnessOptionsException {

        // TODO @HENNLO get several possibilities if possible

        // Return all possible placements that conform to the tolerated Freshness, without regards to the
        // partition/Column distribution. This needs to be checked and validated later on
        Map<Long, List<CatalogPartitionPlacement>> placementOptionsPerPartition = freshnessManager.getRelevantPartitionPlacements(
                catalogTable,
                queryInformation.getAccessedPartitions().get( node.getId() ),
                statement.getFreshnessSpecification() );

        // TODO @HENNLO This can be more easily checked directly at one point in time
        // Pre check if at least all requested partitionIds have a placements
        // Otherwise freshness cannot be delivered
        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : placementOptionsPerPartition.entrySet() ) {
            if ( entry.getValue().size() <= 0 ) {
                throw new InsufficientFreshnessOptionsException();
            }
        }

        return placementOptionsPerPartition;
    }


    private List<RoutedAlgBuilder> handleFreshness( AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) throws InsufficientFreshnessOptionsException {

        Map<Long, List<CatalogPartitionPlacement>> placementOptionsPerPartition = filterFreshnessPlacements( node, catalogTable, queryInformation, statement );

        // List of all possible placement distributions to generate plans
        List<Map<Long, List<CatalogColumnPlacement>>> placements = selectFreshnessPlacements( placementOptionsPerPartition, catalogTable, queryInformation );

        List<RoutedAlgBuilder> newBuilders = new ArrayList<>();
        for ( Map<Long, List<CatalogColumnPlacement>> placementCombination : placements ) {
            for ( RoutedAlgBuilder builder : builders ) {
                RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builder );
                newBuilder.addPhysicalInfo( placementCombination );
                newBuilder.addOrderedPartitionPlacements( orderFreshnessPartitionPlacements( placementCombination ) );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, placementCombination ) );
                newBuilders.add( newBuilder );
            }
        }

        builders.clear();
        builders.addAll( newBuilders );

        return builders;
    }


    private List<CatalogPartitionPlacement> orderFreshnessPartitionPlacements( Map<Long, List<CatalogColumnPlacement>> placementCombination ) {
        // Get all used partitionPlacements
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        List<Pair<Integer, Long>> orderedPartitionPlacements = new ArrayList<>();

        for ( Entry<Long, List<CatalogColumnPlacement>> entry : placementCombination.entrySet() ) {
            long partitionId = entry.getKey();
            List<CatalogColumnPlacement> columnPlacements = entry.getValue();
            columnPlacements.forEach( cp -> partitionPlacements.add( catalog.getPartitionPlacement( cp.adapterId, partitionId ) ) );
        }

        // Order based on oldest partition placement.
        Collections.sort( partitionPlacements, Comparator.comparingLong( CatalogPartitionPlacement::getCommitTimestamp ) );

        return !partitionPlacements.isEmpty() ? partitionPlacements : Collections.emptyList();
    }


    // TODO @HENNLO this could be maybe decentralized per router to have different handling options per strategy
    private List<Map<Long, List<CatalogColumnPlacement>>> selectFreshnessPlacements(
            Map<Long, List<CatalogPartitionPlacement>> placementOptionsPerPartition,
            CatalogTable catalogTable,
            LogicalQueryInformation queryInformation
    ) throws InsufficientFreshnessOptionsException {

        // Contains all possible placementDistributions to later generate plans for
        // Each element contains on possible distribution (query plan)
        List<Map<Long, List<CatalogColumnPlacement>>> placementDistributionCandidates = new ArrayList<>();

        List<Long> requiredColumnIds = queryInformation.getUsedColumnsPerTable( catalogTable.id );

        // We already know on which Physical Partition Placements we could look for data
        // This is why we directly know on which DataPlacement we reside
        // Consequently we therefore already know which ColumnPlacements to use

        //TODO @HENNLO Remove the naive case which is currently only for testing
        // For now we do simple routing
        List<CatalogPartitionPlacement> flatPartitionPlacementSet = placementOptionsPerPartition.values().stream().flatMap( List::stream ).collect( Collectors.toList() );
        for ( int i = 0; i < flatPartitionPlacementSet.size(); i++ ) {

            CatalogPartitionPlacement currentPartitionPlacement = flatPartitionPlacementSet.get( i );
            List<Long> remainingColumnIds = requiredColumnIds.stream().collect( Collectors.toList() );

            List<Integer> checkedAdapters = new ArrayList<>();
            checkedAdapters.add( currentPartitionPlacement.adapterId );

            long partitionId = currentPartitionPlacement.partitionId;
            Map<Long, List<CatalogColumnPlacement>> currentPartitionDistribution = new HashMap<>();

            List<CatalogColumnPlacement> columnPlacements = catalog.getColumnPlacementsOnAdapterPerTable( currentPartitionPlacement.adapterId, currentPartitionPlacement.tableId );
            columnPlacements.forEach( cp -> remainingColumnIds.remove( cp.columnId ) );

            if ( remainingColumnIds.size() == 0 ) {
                currentPartitionDistribution.put( partitionId, columnPlacements );
            } else {
                for ( CatalogPartitionPlacement comparePlacement : flatPartitionPlacementSet ) {
                    // Skip already visited adapters
                    if ( checkedAdapters.contains( comparePlacement.adapterId ) ) {
                        continue;
                    }

                    CatalogDataPlacement dataPlacement = catalog.getDataPlacement( comparePlacement.adapterId, catalogTable.id );
                    // Gathers all columnIds on this Data Placements
                    List<Long> relevantColumnIds = dataPlacement.columnPlacementsOnAdapter.stream().collect( Collectors.toList() );

                    // Only retain those columnIds the still need a suitable ColumnPlacement
                    relevantColumnIds.retainAll( remainingColumnIds );
                    relevantColumnIds.forEach( columnId -> columnPlacements.add( catalog.getColumnPlacement( comparePlacement.adapterId, columnId ) ) );
                    remainingColumnIds.removeAll( relevantColumnIds );

                    // Add this adapter to the list of placements we have already checked to save computation
                    checkedAdapters.add( comparePlacement.adapterId );

                    // If all columns are present on this Data Placement, no further action is required.
                    if ( remainingColumnIds.size() == 0 ) {
                        currentPartitionDistribution.put( partitionId, columnPlacements );
                        break;
                    }
                }
            }

            // Only add if we have acquired enough column placements per partition
            if ( currentPartitionDistribution.get( partitionId ).size() == requiredColumnIds.size() ) {
                placementDistributionCandidates.add( currentPartitionDistribution );
            }
        }

        // TODO @HENNLO if no distribution could be found at all
        // abort freshness processing for this statement
        if ( placementDistributionCandidates.size() == 0 ) {
            throw new InsufficientFreshnessOptionsException();
        }

        return placementDistributionCandidates;
    }


    private void acquireLock( AlgNode node, Statement statement, LogicalQueryInformation queryInformation ) {

        // Locking needed if no valid outdated placements could be found, and we need to fall back to the primary placements
        try {
            Collection<Entry<EntityIdentifier, LockMode>> idAccessMap = new ArrayList<>();
            // Get a shared global schema lock (only DDLs acquire an exclusive global schema lock)
            idAccessMap.add( Pair.of( LockManager.GLOBAL_LOCK, LockMode.SHARED ) );
            // Get locks for individual Entities (tables-partitions)
            EntityAccessMap accessMap = new EntityAccessMap( node, queryInformation.getAccessedPartitions() );

            idAccessMap.addAll( accessMap.getAccessedEntityPair() );
            LockManager.INSTANCE.lock( idAccessMap, (TransactionImpl) statement.getTransaction() );

        } catch ( DeadlockException e ) {
            throw new RuntimeException( e );
        }

    }

}
