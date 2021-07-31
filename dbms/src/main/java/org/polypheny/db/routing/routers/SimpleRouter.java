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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class SimpleRouter extends AbstractRouter {

    private SimpleRouter() {
        // Intentionally left empty
    }


    // Execute the table scan on the first placement of a table
    @Override
    protected Set<List<CatalogColumnPlacement>> selectPlacement( RelNode node, CatalogTable table, Statement statement ) {
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
                placementList.add( Catalog.getInstance().getColumnPlacement( cid ).get( 0 ) );
            }
        }

        return new HashSet<List<CatalogColumnPlacement>>() {{
            add( placementList );
        }};
    }


    public static class SimpleRouterFactory extends RouterFactory {

        public static SimpleRouter createSimpleRouterInstance() {
            return new SimpleRouter();
        }


        @Override
        public Router createInstance() {
            return new SimpleRouter();
        }

    }

}
