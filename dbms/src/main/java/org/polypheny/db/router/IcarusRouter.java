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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.router.factories.RouterFactory;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class IcarusRouter extends FullPlacementQueryRouter {

    @Override
    protected List<RoutedRelBuilder> handleHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        this.cancelQuery = true;
        return Collections.emptyList();
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
        if ( placements.isEmpty() ) {
            this.cancelQuery = true;
            return Collections.emptyList();
        }

        // initial case with empty single builder
        if ( builders.size() == 1  && builders.get( 0 ).getPhysicalPlacementsOfPartitions().isEmpty()) {
            for ( val currentPlacement : placements ) {

                val currentPlacementDistribution = new HashMap<Long, List<CatalogColumnPlacement>>();
                currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), currentPlacement );

                val newBuilder = RoutedRelBuilder.createCopy( statement, cluster, builders.get( 0 ));
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
            }
        } else {
            // already one placement added
            // add placement in order of list to combine full placements of one store
            if ( placements.size() != builders.size() ) {
                log.error( " not allowed! icarus should not happen" );
            }

            for ( val currentPlacement : placements ) {

                val currentPlacementDistribution = new HashMap<Long, List<CatalogColumnPlacement>>();
                currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), currentPlacement );

                // adapterId for all col placements same
                val adapterId = currentPlacement.get( 0 ).adapterId;

                // find corresponding builder:
                val builder = builders.stream().filter( b -> {
                            val listPairs = b.getPhysicalPlacementsOfPartitions().values()
                                    .stream().flatMap( Collection::stream ).collect( Collectors.toList() );

                            val found = listPairs.stream().map( elem -> elem.left ).filter( elem -> elem == adapterId ).findFirst();

                            return found.isPresent();
                        }
                ).findAny();

                if ( !builder.isPresent() ) {
                    // if builder not found, adapter with id will be removed.
                    continue;
                }

                val newBuilder = RoutedRelBuilder.createCopy( statement, cluster, builder.get() );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
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
