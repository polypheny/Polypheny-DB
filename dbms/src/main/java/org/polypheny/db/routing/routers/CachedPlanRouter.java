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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;


/**
 * Routing of cached plan. Nothing needs to be found, everything included in the routed plan.
 */
@Slf4j
public class CachedPlanRouter extends BaseRouter {

    final static Catalog catalog = Catalog.getInstance();


    public RoutedRelBuilder routeCached( RelRoot logicalRoot, CachedProposedRoutingPlan routingPlanCached, Statement statement ) {
        final RoutedRelBuilder builder = RoutedRelBuilder.create( statement, logicalRoot.rel.getCluster() );
        return buildCachedSelect( logicalRoot.rel, builder, statement, logicalRoot.rel.getCluster(), routingPlanCached );
    }


    private RoutedRelBuilder buildCachedSelect( RelNode node, RoutedRelBuilder builder, Statement statement, RelOptCluster cluster, CachedProposedRoutingPlan cachedPlan ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builder = buildCachedSelect( node.getInput( i ), builder, statement, cluster, cachedPlan );
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( !(table.getTable() instanceof LogicalTable) ) {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
            LogicalTable logicalTable = ((LogicalTable) table.getTable());
            CatalogTable catalogTable = catalog.getTable( logicalTable.getTableId() );

            List<Long> partitionIds = catalogTable.partitionProperty.partitionIds;
            Map<Long, List<CatalogColumnPlacement>> placement = new HashMap<>();
            for ( long partition : partitionIds ) {
                if ( cachedPlan.physicalPlacementsOfPartitions.get( partition ) != null ) {
                    List<CatalogColumnPlacement> colPlacements = cachedPlan.physicalPlacementsOfPartitions.get( partition ).stream()
                            .map( placementInfo -> catalog.getColumnPlacement( placementInfo.left, placementInfo.right ) )
                            .collect( Collectors.toList() );
                    placement.put( partition, colPlacements );
                }
            }

            return builder.push( super.buildJoinedTableScan( statement, cluster, placement ) );

        } else if ( node instanceof LogicalValues ) {
            return super.handleValues( (LogicalValues) node, builder );
        } else {
            return super.handleGeneric( node, builder );
        }
    }

}
