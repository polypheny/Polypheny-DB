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
import org.polypheny.db.transaction.Statement;

@Slf4j
public class ReverseSimpleRouter extends AbstractRouter {


    @Override
    protected Set<List<CatalogColumnPlacement>> selectPlacement( RelNode node, CatalogTable catalogTable , Statement statement) {
        // Find the adapter with the fewest column placements
        int adapterWithFewestPlacements = -1;
        int numOfPlacements = Integer.MAX_VALUE;
        for ( Entry<Integer, ImmutableList<Long>> entry : catalogTable.placementsByAdapter.entrySet() ) {
            if ( entry.getValue().size() < numOfPlacements && entry.getValue().size() > 1 ) {
                adapterWithFewestPlacements = entry.getKey();
                numOfPlacements = entry.getValue().size();
            }
        }

        // Take the adapter with fewest placements as base and add missing column placements
        List<CatalogColumnPlacement> placementList = new LinkedList<>();
        for ( long cid : catalogTable.columnIds ) {
            if ( catalogTable.placementsByAdapter.get( adapterWithFewestPlacements ).contains( cid ) ) {
                placementList.add( Catalog.getInstance().getColumnPlacement( adapterWithFewestPlacements, cid ) );
            } else {
                placementList.add( Catalog.getInstance().getColumnPlacement( cid ).get( 0 ) );
            }
        }
        return new HashSet<List<CatalogColumnPlacement>>() {{
            add( placementList );
        }};
    }


    public static class ReverseSimpleRouterFactory extends RouterFactory {

        public static ReverseSimpleRouter createReverseSimpleRouterInstance() {
            return new ReverseSimpleRouter();
        }


        @Override
        public Router createInstance() {
            return new ReverseSimpleRouter();
        }

    }

}
