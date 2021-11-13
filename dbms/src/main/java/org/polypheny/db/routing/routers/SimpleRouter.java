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

package org.polypheny.db.routing.routers;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class SimpleRouter extends AbstractDqlRouter {

    private SimpleRouter() {
        // Intentionally left empty
    }


    @Override
    protected List<RoutedRelBuilder> handleVerticalPartitioningOrReplication( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // do same as without any partitioning
        return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
    }


    @Override
    protected List<RoutedRelBuilder> handleNonePartitioning( RelNode node, CatalogTable catalogTable, Statement statement, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // get placements and convert i
        val placements = selectPlacement( catalogTable );

        // only one builder available
        builders.get( 0 ).addPhysicalInfo( placements );
        builders.get( 0 ).push( super.buildJoinedTableScan( statement, cluster, placements ) );

        return builders;
    }


    @Override
    protected List<RoutedRelBuilder> handleHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionType );

        //TODO @HENNLO Fix this. Routing should not use precomputed logical infomration
        // get info from whereClauseVisitor
        //List<Long> partitionIds = queryInformation.getAccessedPartitions().get( catalogTable.id );
        List<Long> partitionIds = catalogTable.partitionProperty.partitionIds;

        Map<Long, List<CatalogColumnPlacement>> placementDistribution = partitionIds != null
                ? partitionManager.getRelevantPlacements( catalogTable, partitionIds, Collections.emptyList() )
                : partitionManager.getRelevantPlacements( catalogTable, catalogTable.partitionProperty.partitionIds, Collections.emptyList() );

        // only one builder available
        builders.get( 0 ).addPhysicalInfo( placementDistribution );
        builders.get( 0 ).push( super.buildJoinedTableScan( statement, cluster, placementDistribution ) );

        return builders;
    }


    /**
     * @param node
     * @param builder
     * @param statement
     * @param cluster
     * @param queryInformation
     * @return
     */
    public RoutedRelBuilder routeFirst( RelNode node, RoutedRelBuilder builder, Statement statement, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        val result = this.buildSelect( node, Lists.newArrayList( builder ), statement, cluster, queryInformation );
        if ( result.size() > 1 ) {
            log.error( "Single build select with multiple results " );
        }
        return result.get( 0 );
    }


    /**
     * // Execute the table scan on the first placement of a table
     */
    private Map<Long, List<CatalogColumnPlacement>> selectPlacement( CatalogTable table ) {
        // Find the adapter with the most column placements
        int adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Integer, ImmutableList<Long>> entry : table.placementsByAdapter.entrySet() ) {
            if ( entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        // Take the adapter with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( long cid : table.columnIds ) {
            if ( table.placementsByAdapter.get( adapterIdWithMostPlacements ).contains( cid ) ) {
                placementList.add( Catalog.getInstance().getColumnPlacement( adapterIdWithMostPlacements, cid ) );
            } else {
                placementList.add( Catalog.getInstance().getColumnPlacement( cid ).get( 0 ) );
            }
        }

        return new HashMap<Long, List<CatalogColumnPlacement>>() {{
            put( table.partitionProperty.partitionIds.get( 0 ), placementList );
        }};
    }


    public static class SimpleRouterFactory extends RouterFactory {

        public static SimpleRouter createSimpleRouterInstance() {
            return new SimpleRouter();
        }


        @Override
        public Router createInstance() {
            return new SimpleRouter();
        }

    }

}
