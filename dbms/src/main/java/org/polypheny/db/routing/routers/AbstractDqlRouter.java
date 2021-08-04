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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.core.SetOp;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.Statement;


/**
 * Abstract router for DQL routing.
 * It will create rel builders and select column placements per table.
 * There are three abstract methods:
 * handleHorizontalPartitioning
 * handleVerticalPartitioning
 * handleNonePartitioning
 * Every table will be check by the following order:
 * first if partitioned horizontally -> handleHorizontalPartitioning called.
 * second if partitioned vertically or replicated -> handleVerticalPartitioningOrReplication
 * third, all other cases -> handleNonePartitioning
 */
@Slf4j
public abstract class AbstractDqlRouter extends BaseRouter implements Router {

    /**
     * Boolean which cancel routing plan generation.
     * This is useful if implementing router can not propose plans in every situation.
     */
    protected boolean cancelQuery = false;


    /**
     * Abstract methods which will determine routing strategy. Not implemented in abstract class.
     * If implementing router is not supporting one of the three methods, empty list can be returned and cancelQuery boolean set to false.
     */
    protected abstract List<RoutedRelBuilder> handleHorizontalPartitioning( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation );

    protected abstract List<RoutedRelBuilder> handleVerticalPartitioningOrReplication( RelNode node, CatalogTable catalogTable, Statement statement, LogicalTable logicalTable, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation );

    protected abstract List<RoutedRelBuilder> handleNonePartitioning( RelNode node, CatalogTable catalogTable, Statement statement, List<RoutedRelBuilder> builders, RelOptCluster cluster, LogicalQueryInformation queryInformation );


    /**
     * Abstract router only routes DQL queries.
     */
    @Override
    public List<RoutedRelBuilder> route( RelRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation ) {
        // reset cancel query this run
        this.cancelQuery = false;

        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            throw new IllegalStateException( "Should never happen for dml" );
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            throw new IllegalStateException( "Should never happen for conditional executes" );
        } else {
            val builder = RoutedRelBuilder.create( statement, logicalRoot.rel.getCluster() );
            return buildDql( logicalRoot.rel, Lists.newArrayList( builder ), statement, logicalRoot.rel.getCluster(), queryInformation );

        }
    }


    @Override
    public RoutedRelBuilder buildSelect( RelNode node, RoutedRelBuilder builder, Statement statement, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        val result = this.buildSelect( node, Lists.newArrayList( builder ), statement, cluster, queryInformation );
        if ( result.size() > 1 ) {
            log.error( "Single build select with multiple results " );
        }
        return result.get( 0 );
    }


    @Override
    public void resetCaches() {
        joinedTableScanCache.invalidateAll();
    }


    protected List<RoutedRelBuilder> buildDql( RelNode node, List<RoutedRelBuilder> builders, Statement statement, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( node instanceof SetOp ) {
            return buildSetOp( node, builders, statement, cluster, queryInformation );
        } else {
            return buildSelect( node, builders, statement, cluster, queryInformation );
        }
    }


    protected List<RoutedRelBuilder> buildSelect( RelNode node, List<RoutedRelBuilder> builders, Statement statement, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builders = this.buildDql( node.getInput( i ), builders, statement, cluster, queryInformation );
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
                return handleHorizontalPartitioning( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );

            } else {
                // at the moment multiple strategies
                if ( catalogTable.placementsByAdapter.keySet().size() > 1 ) {
                    return handleVerticalPartitioningOrReplication( node, catalogTable, statement, logicalTable, builders, cluster, queryInformation );
                }
                return handleNonePartitioning( node, catalogTable, statement, builders, cluster, queryInformation );
            }

        } else if ( node instanceof LogicalValues ) {
            return Lists.newArrayList( super.handleValues( (LogicalValues) node, builders ) );
        } else {
            return Lists.newArrayList( super.handleGeneric( node, builders ) );
        }
    }


    protected List<RoutedRelBuilder> buildSetOp( RelNode node, List<RoutedRelBuilder> builders, Statement statement, RelOptCluster cluster, LogicalQueryInformation queryInformation ) {
        if ( cancelQuery ) {
            return Collections.emptyList();
        }
        builders = buildDql( node.getInput( 0 ), builders, statement, cluster, queryInformation );

        RoutedRelBuilder builder0 = RoutedRelBuilder.create( statement, cluster );
        val b0 = buildDql( node.getInput( 1 ), Lists.newArrayList( builder0 ), statement, cluster, queryInformation ).get( 0 );

        builders.forEach(
                builder -> builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek(), b0.build() ) ) )
        );

        return builders;
    }

}
