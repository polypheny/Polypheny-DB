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

package org.polypheny.db.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class CachedPlanRouter {

    final static Catalog catalog = Catalog.getInstance();

    public static List<RoutedRelBuilder> routeCached( RelRoot logicalRoot, List<CachedProposedRoutingPlan> routingPlansCached, Statement statement ) {

        val result = new ArrayList<RoutedRelBuilder>();
        for( val plan : routingPlansCached){
            var builder = RoutedRelBuilder.create( statement, logicalRoot.rel.getCluster() );
            builder =  buildCachedSelect( logicalRoot.rel, builder, statement, logicalRoot.rel.getCluster(), plan );
            result.add( builder );
        }

        return result;
    }

    private static RoutedRelBuilder buildCachedSelect( RelNode node, RoutedRelBuilder builder, Statement statement, RelOptCluster cluster, CachedProposedRoutingPlan cachedPlan){
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

            val partitionIds = catalogTable.partitionProperty.partitionIds;
            val placement = new HashMap<Long, List<CatalogColumnPlacement>>();
            for ( val partition: partitionIds) {
                val colPlacemets =
                        cachedPlan.physicalPlacementsOfPartitions.get( partition ).stream().map( placementInfo -> catalog.getColumnPlacement( placementInfo.left, placementInfo.right )).collect( Collectors.toList());
                placement.put( partition, colPlacemets );
            }

            return builder.push( RoutingHelpers.buildJoinedTableScan( statement, cluster, placement ) );

        } else if ( node instanceof LogicalValues ) {
            log.info( "handleValues" );
            return RoutingHelpers.handleValues( (LogicalValues) node, builder);
        } else {
            log.info( "handleGeneric" );
            return RoutingHelpers.handleGeneric( node, builder );
        }

    }

}
