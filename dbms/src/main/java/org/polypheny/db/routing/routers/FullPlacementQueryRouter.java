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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;


@Slf4j
public class FullPlacementQueryRouter extends AbstractDqlRouter {

    @Override
    protected List<RoutedAlgBuilder> handleHorizontalPartitioning(
            AlgNode node,
            LogicalTable catalogTable,
            Statement statement,
            LogicalEntity logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) {

        if ( log.isDebugEnabled() ) {
            log.debug( "{} is horizontally partitioned", catalogTable.name );
        }

        Collection<Map<Long, List<AllocationColumn>>> placements = selectPlacementHorizontalPartitioning( node, catalogTable, queryInformation );

        List<RoutedAlgBuilder> newBuilders = new ArrayList<>();
        for ( Map<Long, List<AllocationColumn>> placementCombination : placements ) {
            for ( RoutedAlgBuilder builder : builders ) {
                RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builder );
                newBuilder.addPhysicalInfo( placementCombination );
                newBuilder.push( super.buildJoinedScan( statement, cluster, null ) );
                newBuilders.add( newBuilder );
            }
        }

        builders.clear();
        builders.addAll( newBuilders );

        return builders;
    }


    @Override
    protected List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication(
            AlgNode node,
            LogicalTable catalogTable,
            Statement statement,
            LogicalEntity logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) {
        // Same as no partitioning
        return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
    }


    @Override
    protected List<RoutedAlgBuilder> handleNonePartitioning(
            AlgNode node,
            LogicalTable catalogTable,
            Statement statement,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) {

        if ( log.isDebugEnabled() ) {
            log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );
        }

        final Set<List<AllocationColumn>> placements = selectPlacement( catalogTable, queryInformation );

        List<RoutedAlgBuilder> newBuilders = new ArrayList<>();
        /*for ( List<CatalogColumnPlacement> placementCombination : placements ) {
            Map<Long, List<CatalogColumnPlacement>> currentPlacementDistribution = new HashMap<>();
            PartitionProperty property = snapshot.alloc().getPartitionProperty( catalogTable.id );*/
        //currentPlacementDistribution.put( property.partitionIds.get( 0 ), placementCombination );

        List<AllocationEntity> allocationEntities = Catalog.snapshot().alloc().getFromLogical( catalogTable.id );

        for ( RoutedAlgBuilder builder : builders ) {
            RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builder );
            //newBuilder.addPhysicalInfo( currentPlacementDistribution );
            newBuilder.push( super.buildJoinedScan( statement, cluster, allocationEntities ) );
            newBuilders.add( newBuilder );
        }
        //}

        builders.clear();
        builders.addAll( newBuilders );

        return builders;
    }


    protected Collection<Map<Long, List<AllocationColumn>>> selectPlacementHorizontalPartitioning( AlgNode node, LogicalTable catalogTable, LogicalQueryInformation queryInformation ) {
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionProperty property = Catalog.snapshot().alloc().getPartitionProperty( catalogTable.id );
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( property.partitionType );

        // Utilize scanId to retrieve Partitions being accessed
        List<Long> partitionIds = queryInformation.getAccessedPartitions().get( node.getId() );

        Map<Long, Map<Long, List<AllocationColumn>>> allPlacements = partitionManager.getAllPlacements( catalogTable, partitionIds );

        return allPlacements.values();
    }


    protected Set<List<AllocationColumn>> selectPlacement( LogicalTable catalogTable, LogicalQueryInformation queryInformation ) {
        // Get used columns from analyze
        List<Long> usedColumns = queryInformation.getAllColumnsPerTable( catalogTable.id );

        // Filter for placements by adapters
        List<AllocationEntity> allocs = Catalog.snapshot().alloc().getFromLogical( catalogTable.id ).stream()
                .map( a -> a.unwrap( AllocationTable.class ) )
                .filter( a -> new HashSet<>( a.getColumnIds() ).containsAll( usedColumns ) )
                .collect( Collectors.toList() );

        /*List<Long> adapters = Catalog.snapshot().alloc().getColumnPlacementsByAdapter( catalogTable.id ).entrySet()
                .stream()
                .filter( elem -> new HashSet<>( elem.getValue() ).containsAll( usedColumns ) )
                .map( Entry::getKey )
                .collect( Collectors.toList() );*/

        final Set<List<AllocationColumn>> result = new HashSet<>();
        for ( AllocationEntity alloc : allocs ) {
            List<AllocationColumn> placements = usedColumns.stream()
                    .map( colId -> alloc.unwrap( AllocationTable.class ).getColumns().stream().filter( c -> c.columnId == colId ).findFirst().get() )
                    .collect( Collectors.toList() );

            if ( !placements.isEmpty() ) {
                result.add( placements );
            } else {
                // no available placements found
                this.cancelQuery = true;
            }
        }

        return result;
    }


    public static class FullPlacementQueryRouterFactory extends RouterFactory {

        @Override
        public Router createInstance() {
            return new FullPlacementQueryRouter();
        }

    }

}
