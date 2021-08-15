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

package org.polypheny.db.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class FullPlacementQueryRouter extends AbstractDqlRouter {

    @Override
    protected List<RoutedRelBuilder> handleHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        log.debug( "{} is horizontally partitioned", catalogTable.name );

        val placements = selectPlacementHorizontalPartitioning( catalogTable, queryInformation );

        val newBuilders = new ArrayList<RoutedRelBuilder>();
        for ( val placementCombination : placements ) {
            for ( val builder : builders ) {
                val newBuilder = RoutedRelBuilder.createCopy( statement, cluster, builder );
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
    protected List<RoutedRelBuilder> handleVerticalPartitioningOrReplication( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // same as no partitioning
        return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
    }


    @Override
    protected List<RoutedRelBuilder> handleNonePartitioning( RelNode node, CatalogTable catalogTable, Statement statement, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );

        val placements = selectPlacement( catalogTable, queryInformation );

        val newBuilders = new ArrayList<RoutedRelBuilder>();
        for ( val placementCombination : placements ) {

            val currentPlacementDistribution = new HashMap<Long, List<CatalogColumnPlacement>>();
            currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), placementCombination );

            for ( val builder : builders ) {
                val newBuilder = RoutedRelBuilder.createCopy( statement, cluster, builder );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
            }

        }

        builders.clear();
        builders.addAll( newBuilders );

        return builders;
    }


    protected Collection<Map<Long, List<CatalogColumnPlacement>>> selectPlacementHorizontalPartitioning( CatalogTable catalogTable, LogicalQueryInformation queryInformation ) {
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionType );

        // get info from whereClauseVisitor
        List<Long> partitionIds = queryInformation.getAccessedPartitions().get( catalogTable.id );

        val allPlacements = partitionManager.getAllPlacements( catalogTable, partitionIds );

        val placements = allPlacements.values();
        return placements;
    }


    protected Set<List<CatalogColumnPlacement>> selectPlacement( CatalogTable catalogTable, LogicalQueryInformation queryInformation ) {
        // get used columns from analyze
        val usedColumns = queryInformation.getAllColumnsPerTable( catalogTable.id );

        // filter for placements by adapters
        val adapters = catalogTable.placementsByAdapter.entrySet()
                .stream()
                .filter( elem -> elem.getValue().containsAll( usedColumns ) )
                .map( elem -> elem.getKey() )
                .collect( Collectors.toList() );

        val result = new HashSet<List<CatalogColumnPlacement>>();
        for ( val adapterId : adapters ) {
            val placements = usedColumns.stream()
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
