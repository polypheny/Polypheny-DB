/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.partition.SimplePartition;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.routing.Router;
import org.polypheny.db.transaction.Transaction;


public class SimpleRouter extends AbstractRouter {

    private SimpleRouter() {
        // Intentionally left empty
    }


    @Override
    protected void analyze( Transaction transaction, RelRoot logicalRoot ) {
        // Nothing to do. Simple router does nothing sophisticated...
    }


    @Override
    protected void wrapUp( Transaction transaction, RelNode routed ) {
        // Nothing to do. Simple router does nothing sophisticated...
    }


    // Execute the table scan on the first placement of a table
    @Override
    protected CatalogColumnPlacement selectPlacement( RelNode node, List<CatalogColumnPlacement> available ) {
        // Take first
        System.out.println("HENNLO: SimpleRouter selectPlacement: " + available.get(0).tableName);
        return available.get( 0 );
    }


    // Create table on the first store in the list that supports schema changes
    @Override
    public List<Store> createTable( long schemaId, Transaction transaction ) {
        System.out.println("HENNLO: SimpleRouter createTable() schemaID: " + schemaId);
        Map<String, Store> availableStores = StoreManager.getInstance().getStores();
        for ( Store store : availableStores.values() ) {
            if ( !store.isSchemaReadOnly() ) {
                return ImmutableList.of( store );
            }
        }
        throw new RuntimeException( "No suitable store found" );
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
