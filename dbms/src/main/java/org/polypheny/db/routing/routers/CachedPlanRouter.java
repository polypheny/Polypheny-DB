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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;


/**
 * Routing of cached plan. Nothing needs to be found, everything included in the routed plan.
 */
@Slf4j
public class CachedPlanRouter extends BaseRouter {

    final static Catalog catalog = Catalog.getInstance();


    public RoutedAlgBuilder routeCached( AlgRoot logicalRoot, CachedProposedRoutingPlan routingPlanCached, Statement statement, LogicalQueryInformation queryInformation ) {
        final RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        return buildCachedSelect( logicalRoot.alg, builder, statement, logicalRoot.alg.getCluster(), routingPlanCached, queryInformation );
    }


    private RoutedAlgBuilder buildCachedSelect( AlgNode node, RoutedAlgBuilder builder, Statement statement, AlgOptCluster cluster, CachedProposedRoutingPlan cachedPlan, LogicalQueryInformation queryInformation ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builder = buildCachedSelect( node.getInput( i ), builder, statement, cluster, cachedPlan, queryInformation );
        }

        if ( node instanceof DocumentScan ) {
            return super.handleDocScan( builder, statement, node.getEntity() );
        }

        if ( node instanceof LogicalRelScan && node.getEntity() != null ) {
            LogicalTable catalogTable = node.getEntity().unwrap( LogicalTable.class );
            PartitionProperty property = Catalog.snapshot().alloc().getPartitionProperty( catalogTable.id );
            List<Long> partitionIds = property.partitionIds;
            Map<Long, List<AllocationColumn>> placement = new HashMap<>();
            for ( long partition : partitionIds ) {
                if ( cachedPlan.physicalPlacementsOfPartitions.get( partition ) != null ) {
                    List<AllocationColumn> colPlacements = cachedPlan.physicalPlacementsOfPartitions.get( partition ).stream()
                            .map( placementInfo -> catalog.getSnapshot().alloc().getColumn( placementInfo.left, placementInfo.right ).orElseThrow() )
                            .collect( Collectors.toList() );
                    placement.put( partition, colPlacements );
                }
            }

            return builder.push( super.buildJoinedScan( statement, cluster, null ) );
        } else if ( node instanceof LogicalValues ) {
            return super.handleValues( (LogicalValues) node, builder );
        } else {
            return super.handleGeneric( node, builder );
        }
    }

}
