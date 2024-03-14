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

package org.polypheny.db.routing.routers;

import com.google.common.collect.Lists;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.routing.ColumnDistribution;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.tools.RoutedAlgBuilder;


@Slf4j
public class SimpleRouter extends AbstractDqlRouter {

    private SimpleRouter() {
        // Intentionally left empty
    }


    @Override
    protected List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication( AlgNode node, LogicalTable table, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        // Do same as without any partitioning
        // placementId -> List<AllocColumn>

        List<AllocationPartition> partitions = context.getStatement().getTransaction().getSnapshot().alloc().getPartitionsFromLogical( table.id );
        // Only one builder available

        List<Long> partitionIds = partitions.stream().map( p -> p.id ).toList();
        ColumnDistribution columnDistribution = new ColumnDistribution( table.id, table.getColumnIds(), partitionIds, partitionIds, List.of(), context.getCluster().getSnapshot() );
        context.fieldDistribution = columnDistribution;
        builders.get( 0 ).push( super.buildJoinedScan( columnDistribution, context ) );

        return builders;
    }


    @Override
    protected List<RoutedAlgBuilder> handleNonePartitioning( AlgNode node, LogicalTable table, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        // Get placements and convert into placement distribution
        List<AllocationEntity> entities = Catalog.snapshot().alloc().getFromLogical( table.id );

        List<Long> partitionId = List.of( entities.get( 0 ).partitionId );
        context.fieldDistribution = new ColumnDistribution( table.id, table.getColumnIds(), partitionId, partitionId, List.of(), context.getSnapshot() );

        // Only one builder available
        super.handleRelScan( builders.get( 0 ), context.getStatement(), entities.get( 0 ) );

        return builders;
    }


    @Override
    protected List<RoutedAlgBuilder> handleHorizontalPartitioning( AlgNode node, LogicalTable table, List<RoutedAlgBuilder> builders, RoutingContext context ) {
        // Utilize scanId to retrieve Partitions being accessed
        List<Long> partitionIds = context.getQueryInformation().getAccessedPartitions().get( node.getEntity().id );

        // Only one builder available
        //builders.get( 0 ).addPhysicalInfo( placementDistribution );
        ColumnDistribution columnDistribution = new ColumnDistribution( table.id, table.getColumnIds(), partitionIds, partitionIds, List.of(), context.getSnapshot() );
        context.fieldDistribution = columnDistribution;
        builders.get( 0 ).push( super.buildJoinedScan( columnDistribution, context ) );

        return builders;
    }


    public RoutedAlgBuilder routeFirst( AlgNode node, RoutedAlgBuilder builder, RoutingContext context ) {
        List<RoutedAlgBuilder> result = this.buildSelect( node, Lists.newArrayList( builder ), context );
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
