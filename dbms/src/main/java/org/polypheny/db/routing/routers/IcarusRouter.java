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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;


@Slf4j
public class IcarusRouter extends FullPlacementQueryRouter {

    @Override
    protected List<RoutedAlgBuilder> handleHorizontalPartitioning( AlgNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        this.cancelQuery = true;
        return Collections.emptyList();
    }


    @Override
    protected List<RoutedAlgBuilder> handleVerticalPartitioningOrReplication( AlgNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        // same as no partitioning
        return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
    }


    @Override
    protected List<RoutedAlgBuilder> handleNonePartitioning( AlgNode node, CatalogTable catalogTable, Statement statement, List<RoutedAlgBuilder> builders, AlgOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );
        }

        final Set<List<CatalogColumnPlacement>> placements = selectPlacement( catalogTable, queryInformation );
        List<RoutedAlgBuilder> newBuilders = new ArrayList<>();
        if ( placements.isEmpty() ) {
            this.cancelQuery = true;
            return Collections.emptyList();
        }

        // Initial case with empty single builder
        if ( builders.size() == 1 && builders.get( 0 ).getPhysicalPlacementsOfPartitions().isEmpty() ) {
            for ( List<CatalogColumnPlacement> currentPlacement : placements ) {
                final Map<Long, List<CatalogColumnPlacement>> currentPlacementDistribution = new HashMap<>();
                currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), currentPlacement );

                final RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builders.get( 0 ) );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
            }
        } else {
            // Already one placement added
            // Add placement in order of list to combine full placements of one store
            if ( placements.size() != builders.size() ) {
                log.error( "Not allowed! With Icarus, this should not happen" );
                throw new RuntimeException( "Not allowed! With Icarus, this should not happen" );
            }

            for ( List<CatalogColumnPlacement> currentPlacement : placements ) {
                final Map<Long, List<CatalogColumnPlacement>> currentPlacementDistribution = new HashMap<>();
                currentPlacementDistribution.put( catalogTable.partitionProperty.partitionIds.get( 0 ), currentPlacement );

                // AdapterId for all col placements same
                final int adapterId = currentPlacement.get( 0 ).adapterId;

                // Find corresponding builder:
                final RoutedAlgBuilder builder = builders.stream().filter( b -> {
                            final List<Pair<Integer, Long>> listPairs = b.getPhysicalPlacementsOfPartitions().values().stream()
                                    .flatMap( Collection::stream )
                                    .collect( Collectors.toList() );
                            final Optional<Integer> found = listPairs.stream()
                                    .map( elem -> elem.left )
                                    .filter( elem -> elem == adapterId )
                                    .findFirst();
                            return found.isPresent();
                        }
                ).findAny().orElse( null );

                if ( builder == null ) {
                    // If builder not found, adapter with id will be removed.
                    continue;
                }

                final RoutedAlgBuilder newBuilder = RoutedAlgBuilder.createCopy( statement, cluster, builder );
                newBuilder.addPhysicalInfo( currentPlacementDistribution );
                newBuilder.push( super.buildJoinedTableScan( statement, cluster, currentPlacementDistribution ) );
                newBuilders.add( newBuilder );
            }
            if ( newBuilders.isEmpty() ) {
                // apparently we have a problem and no builder fits
                cancelQuery = true;
                log.error( "Icarus did not find a suitable builder!" );
                return Collections.emptyList();
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
