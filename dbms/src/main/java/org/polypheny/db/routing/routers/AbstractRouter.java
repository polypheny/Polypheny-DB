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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.core.SetOp;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;


@Slf4j
public abstract class AbstractRouter extends BaseRouter implements Router {


    protected ExecutionTimeMonitor executionTimeMonitor;
    protected InformationPage page = null;
    // For reporting purposes
    //protected Map<Long, SelectedAdapterInfo> selectedAdapter;
    protected boolean cancelQuery = false;


    @Override
    public List<RoutedRelBuilder> route( RelRoot logicalRoot, Statement statement ) {
        this.executionTimeMonitor = executionTimeMonitor;
        //this.selectedAdapter = new HashMap<>();
        this.cancelQuery = false;

        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            throw new IllegalStateException( "Should never happen for dml" );
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            throw new IllegalStateException( "Should never happen for conditional executes" );
        } else {
            val builder = RoutedRelBuilder.create( statement, logicalRoot.rel.getCluster() );
            return buildDql( logicalRoot.rel, Lists.newArrayList( builder ), statement, logicalRoot.rel.getCluster() );

        }
    }


    // Select the placement on which a table scan should be executed
    protected abstract Set<List<CatalogColumnPlacement>> selectPlacement( RelNode node, CatalogTable catalogTable, Statement statement );


    protected List<RoutedRelBuilder> buildDql( RelNode node, List<RoutedRelBuilder> builders, Statement statement, RelOptCluster cluster ) {
        if ( node instanceof SetOp ) {
            return buildSetOp( node, builders, statement, cluster );
        } else {
            return buildSelect( node, builders, statement, cluster );
        }
    }


    @Override
    public RoutedRelBuilder buildSelect( RelNode node, RoutedRelBuilder builder, Statement statement, RelOptCluster cluster ) {
        val result = this.buildSelect( node, Lists.newArrayList( builder ), statement, cluster );
        if ( result.size() > 1 ) {
            log.error( "Single build select with multiple results " );
        }
        return result.get( 0 );
    }


    protected List<RoutedRelBuilder> buildSelect( RelNode node, List<RoutedRelBuilder> builders, Statement statement, RelOptCluster cluster ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builders = this.buildDql( node.getInput( i ), builders, statement, cluster );
        }

        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( !(table.getTable() instanceof LogicalTable) ) {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }

            LogicalTable logicalTable = ((LogicalTable) table.getTable());
            CatalogTable catalogTable = Catalog.getInstance().getTable( logicalTable.getTableId() );

            // Check if table is even horizontal partitioned
            if ( catalogTable.isPartitioned ) {

                // default routing
                return handleHorizontalPartitioning( node, catalogTable, statement, logicalTable, builders, cluster );

            } else {
                // at the moment multiple strategies
                return handleNoneHorizontalPartitioning( node, catalogTable, statement, builders, cluster );
            }

        } else if ( node instanceof LogicalValues ) {
            return Lists.newArrayList( super.handleValues( (LogicalValues) node, builders ) );
        } else {
            return Lists.newArrayList( super.handleGeneric( node, builders ) );
        }
    }


    protected List<RoutedRelBuilder> handleNoneHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, List<RoutedRelBuilder> builders, RelOptCluster cluster ) {
        log.debug( "{} is NOT partitioned - Routing will be easy", catalogTable.name );
        val placements = selectPlacement( node, catalogTable, statement );

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


    protected List<RoutedRelBuilder> handleHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster ) {
        PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
        PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( catalogTable.partitionType );

        // get info from whereClauseVisitor
        StatementEvent event = (StatementEvent) statement.getTransaction().getMonitoringEvent();
        List<Long> partitionIds = event.getAccessedPartitions().get( catalogTable.id );

        Map<Long, List<CatalogColumnPlacement>> placementDistribution;

        if ( partitionIds != null ) {
            placementDistribution = partitionManager.getRelevantPlacements( catalogTable, partitionIds );
        } else {
            placementDistribution = partitionManager.getRelevantPlacements( catalogTable, catalogTable.partitionProperty.partitionIds );
        }

        builders.forEach( b -> b.addPhysicalInfo( placementDistribution ) );
        return builders.stream().map( builder -> builder.push( super.buildJoinedTableScan( statement, cluster, placementDistribution ) ) ).collect( Collectors.toList() );
    }


    protected List<RoutedRelBuilder> buildSetOp( RelNode node, List<RoutedRelBuilder> builders, Statement statement, RelOptCluster cluster ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, statement, cluster );

        RoutedRelBuilder builder0 = RoutedRelBuilder.create( statement, cluster );
        val b0 = buildDql( node.getInput( 1 ), Lists.newArrayList( builder0 ), statement, cluster ).get( 0 );

        builders.forEach(
                builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek(), b0.build() ) ) )
        );

        return builders;
    }


    @Override
    public void resetCaches() {
        super.joinedTableScanCache.invalidateAll();
    }


}
