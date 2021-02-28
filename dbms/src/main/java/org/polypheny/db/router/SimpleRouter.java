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


import com.google.common.collect.ImmutableList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.routing.Router;
import org.polypheny.db.transaction.Statement;


public class SimpleRouter extends AbstractRouter {

    private SimpleRouter() {
        // Intentionally left empty
    }


    @Override
    protected void analyze( Statement statement, RelRoot logicalRoot ) {
        // Nothing to do. Simple router does nothing sophisticated...
    }


    @Override
    protected void wrapUp( Statement statement, RelNode routed ) {
        // Nothing to do. Simple router does nothing sophisticated...
    }


    // Execute the table scan on the first placement of a table
    @Override
    protected List<CatalogColumnPlacement> selectPlacement( RelNode node, CatalogTable table ) {
        // Find the adapter with the most column placements
        int adapterIdWithMostPlacements = -1;
        int numOfPlacements = 0;
        for ( Entry<Integer, ImmutableList<Long>> entry : table.placementsByAdapter.entrySet() ) {
            if ( entry.getValue().size() > numOfPlacements ) {
                adapterIdWithMostPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        // Take the adapter with most placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( long cid : table.columnIds ) {
            if ( table.placementsByAdapter.get( adapterIdWithMostPlacements ).contains( cid ) ) {
                placementList.add( Catalog.getInstance().getColumnPlacement( adapterIdWithMostPlacements, cid ) );
            } else {
                placementList.add( Catalog.getInstance().getColumnPlacements( cid ).get( 0 ) );
            }
        }

        return placementList;
    }


    // Create table on the first store in the list
    @Override
    public List<DataStore> createTable( long schemaId, Statement statement ) {
        Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
        for ( DataStore store : availableStores.values() ) {
            return ImmutableList.of( store );
        }
        throw new RuntimeException( "No suitable data store found" );
    }


    // Add column on the first store holding a placement of this table
    @Override
    public List<DataStore> addColumn( CatalogTable catalogTable, Statement statement ) {
        return ImmutableList.of( AdapterManager.getInstance().getStore( catalogTable.placementsByAdapter.keySet().asList().get( 0 ) ) );
    }


    @Override
    public void dropPlacements( List<CatalogColumnPlacement> placements ) {
        // Nothing to do. Simple router does nothing sophisticated...
    }


    public static class SimpleRouterFactory extends RouterFactory {

        @Override
        public Router createInstance() {
            return new SimpleRouter();
        }

    }

}
