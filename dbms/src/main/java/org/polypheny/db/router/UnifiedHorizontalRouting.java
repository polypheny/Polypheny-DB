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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class UnifiedHorizontalRouting extends AbstractRouter {

    @Override
    public List<DataStore> createTable( long schemaId, Statement statement ) {
        return null;
    }


    @Override
    public List<DataStore> addColumn( CatalogTable catalogTable, Statement statement ) {
        return null;
    }


    @Override
    public void dropPlacements( List<CatalogColumnPlacement> placements ) {

    }


    @Override
    protected void analyze( Statement statement, RelRoot logicalRoot ) {

    }


    @Override
    protected void wrapUp( Statement statement, RelNode routed ) {

    }

    @Override
    public List<RelRoot> route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        this.executionTimeMonitor = executionTimeMonitor;
        this.selectedAdapter = new HashMap<>();

        log.info( "Start Routing" );

        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            return Collections.emptyList();
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            return Collections.emptyList();
        }
        log.info( "Start build DQL" );
        // TODO: get many version
        List<RelBuilder> builders = buildDql( logicalRoot.rel, statement, logicalRoot.rel.getCluster() );
        List<RelNode> routed = builders.stream().map( RelBuilder::build ).collect( Collectors.toList() );
        log.info( "End DQL" );

        return routed.stream()
                .map( elem -> new RelRoot( elem, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation ) )
                .collect( Collectors.toList() );
    }


    @Override
    protected Set<List<CatalogColumnPlacement>> selectPlacement( RelNode node, CatalogTable catalogTable, Statement statement ) {
        val fullPlacementsPerTable = new HashSet<List<CatalogColumnPlacement>>();

        // get used columns from analyze
        StatementEvent event = (StatementEvent)statement.getTransaction().getMonitoringEvent();
        val usedColumns = event.getAnalyzeRelShuttle().getUsedColumnsPerTable(catalogTable.id);


        // filter for placements by adapters
        val adapters = catalogTable.placementsByAdapter.entrySet()
                .stream()
                .filter( elem -> elem.getValue().containsAll( usedColumns ))
                .map( elem -> elem.getKey() )
                .collect( Collectors.toList());

        val result = new HashSet<List<CatalogColumnPlacement>>();
        for(val adapterId : adapters){
            val placements = usedColumns.stream()
                    .map( colId -> Catalog.getInstance().getColumnPlacement( adapterId, colId ) )
                    .collect( Collectors.toList());
            result.add( placements );
        }

        return result;
    }


    public static class UnifiedHorizontalRoutingFactory extends RouterFactory {


        @Override
        public Router createInstance() {
            return new UnifiedHorizontalRouting();
        }

    }

}
