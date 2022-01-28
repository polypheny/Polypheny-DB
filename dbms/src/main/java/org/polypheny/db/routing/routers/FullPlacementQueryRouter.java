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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;


@Slf4j
public class FullPlacementQueryRouter extends AbstractDqlRouter {

    @Override
    protected List<RoutedAlgBuilder> handleHorizontalPartitioning(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) {

        if ( log.isDebugEnabled() ) {
            log.debug( "{} is horizontally partitioned", catalogTable.name );
        }

        Collection<Map<Long, List<CatalogColumnPlacement>>> placements = selectPlacementHorizontalPartitioning( node, catalogTable, queryInformation );

        List<RoutedAlgBuilder> newBuilders = new ArrayList<>();
        for ( Map<Long, List<CatalogColumnPlacement>> placementCombination : placements ) {
            for ( RoutedAlgBuilder builder : builders ) {
                RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builder );
                newBuilder.addPhysicalInfo( placementCombination );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, placementCombination ) );
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
            CatalogTable catalogTable,
            Statement statement,
            LogicalTable logicalTable,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) {
        // Same as no partitioning
        return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
    }


    @Override
    protected List<RoutedAlgBuilder> handleNonePartitioning(
            AlgNode node,
            CatalogTable catalogTable,
            Statement statement,
            List<RoutedAlgBuilder> builders,
            AlgOptCluster cluster,
            LogicalQueryInformation queryInformation ) {

        if ( log.isDebugEnabled() ) {
            log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );
        }

        final Set<List<CatalogColumnPlacement>> placements = selectPlacement( catalogTable, queryInformation );

        List<RoutedAlgBuilder> newBuilders = new ArrayList<>();
        for ( List<CatalogColumnPlacement> placementCombination : placements ) {
            Map<Long, List<CatalogColumnPlacement>> currentPlacementDistribution = new HashMap<>();
            currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), placementCombination );

            for ( RoutedAlgBuilder builder : builders ) {
                RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builder );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
            }
        }

        builders.clear();
        builders.addAll( newBuilders );

        return builders;
    }


    protected Collection<Map<Long, List<CatalogColumnPlacement>>> selectPlacementHorizontalPartitioning( AlgNode node, CatalogTable catalogTable, LogicalQueryInformation queryInformation ) {
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionProperty.partitionType );

        // Utilize scanId to retrieve Partitions being accessed
        List<Long> partitionIds = queryInformation.getAccessedPartitions().get( node.getId() );

        Map<Integer, Map<Long, List<CatalogColumnPlacement>>> allPlacements = partitionManager.getAllPlacements( catalogTable, partitionIds );

        return allPlacements.values();
    }


    protected Set<List<CatalogColumnPlacement>> selectPlacement( CatalogTable catalogTable, LogicalQueryInformation queryInformation ) {
        // Get used columns from analyze
        List<Long> usedColumns = queryInformation.getAllColumnsPerTable( catalogTable.id );

        // Filter for placements by adapters
        List<Integer> adapters = catalog.getColumnPlacementsByAdapter( catalogTable.id ).entrySet()
                .stream()
                .filter( elem -> elem.getValue().containsAll( usedColumns ) )
                .map( Entry::getKey )
                .collect( Collectors.toList() );

        final Set<List<CatalogColumnPlacement>> result = new HashSet<>();
        for ( int adapterId : adapters ) {
            List<CatalogColumnPlacement> placements = usedColumns.stream()
                    .map( colId -> catalog.getColumnPlacement( adapterId, colId ) )
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
