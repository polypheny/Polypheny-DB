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

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public class SimpleRouter extends AbstractDqlRouter {

    private SimpleRouter() {
        // Intentionally left empty
    }


    @Override
    protected List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication( AlgNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // Do same as without any partitioning
        return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
    }


    @Override
    protected List<RoutedAlgBuilder> handleNonePartitioning( AlgNode node, CatalogTable catalogTable, Statement statement, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // Get placements and convert into placement distribution
        final Map<Long, List<CatalogColumnPlacement>> placements = selectPlacement( catalogTable );

        // Only one builder available
        builders.get( 0 ).addPhysicalInfo( placements );
        builders.get( 0 ).push( super.buildJoinedScan( statement, cluster, placements ) );

        return builders;
    }


    @Override
    protected List<RoutedAlgBuilder> handleHorizontalPartitioning( AlgNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionProperty.partitionType );

        // Utilize scanId to retrieve Partitions being accessed
        List<Long> partitionIds = queryInformation.getAccessedPartitions().get( node.getId() );

        Map<Long, List<CatalogColumnPlacement>> placementDistribution = partitionIds != null
                ? partitionManager.getRelevantPlacements( catalogTable, partitionIds, Collections.emptyList() )
                : partitionManager.getRelevantPlacements( catalogTable, catalogTable.partitionProperty.partitionIds, Collections.emptyList() );

        // Only one builder available
        builders.get( 0 ).addPhysicalInfo( placementDistribution );
        builders.get( 0 ).push( super.buildJoinedScan( statement, cluster, placementDistribution ) );

        return builders;
    }


    public RoutedAlgBuilder routeFirst( AlgNode node, RoutedAlgBuilder builder, Statement statement, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        List<RoutedAlgBuilder> result = this.buildSelect( node, Lists.newArrayList( builder ), statement, cluster, queryInformation );
        if ( result.size() > 1 ) {
            log.error( "Single build select with multiple results " );
        }
        return result.get( 0 );
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
