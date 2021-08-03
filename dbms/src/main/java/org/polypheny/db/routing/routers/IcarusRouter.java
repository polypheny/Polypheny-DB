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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class IcarusRouter extends HorizontalFullPlacementQueryRouter {

    @Override
    protected List<RoutedRelBuilder> handleNoneHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, List<RoutedRelBuilder> builders, RelOptCluster cluster ) {
        log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );

        val placements = selectPlacement( node, catalogTable, statement );
        val newBuilders = new ArrayList<RoutedRelBuilder>();
        if ( placements.isEmpty() ) {
            this.cancelQuery = true;
            return Collections.emptyList();
        }

        // initial case with multiple placements
        // create new builds
        if ( placements.size() > builders.size() && builders.size() == 1 ) {
            for ( val currentPlacement : placements ) {

                val currentPlacementDistribution = new HashMap<Long, List<CatalogColumnPlacement>>();
                currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), currentPlacement );

                val newBuilder = RoutedRelBuilder.createCopy( statement, cluster, builders.get( 0 ) );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
            }
        } else {
            // already one placement
            // add placement in order of list to combine full placements of one store
            if ( placements.size() != builders.size() ) {
                log.error( " not allowed! icarus should not happen" );
            }

            var counter = 0;
            for ( val currentPlacement : placements ) {

                val currentPlacementDistribution = new HashMap<Long, List<CatalogColumnPlacement>>();
                currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), currentPlacement );

                val newBuilder = RoutedRelBuilder.createCopy( statement, cluster, builders.get( counter ) );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );

                counter++;
            }

        }

        builders.clear();
        builders.addAll( newBuilders );

        return builders;
    }


    public static class IcarusRouterFactory extends RouterFactory {


        @Override
        public Router createInstance() {
            return new IcarusRouter();
        }

    }

}
