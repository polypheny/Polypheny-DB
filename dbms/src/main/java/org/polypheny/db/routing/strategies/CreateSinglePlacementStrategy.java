/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.routing.strategies;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.policies.policy.policy.PoliciesManager;
import org.polypheny.db.policies.policy.policy.PoliciesManager.Action;


public class CreateSinglePlacementStrategy implements CreatePlacementStrategy {

    @Override
    public List<DataStore> getDataStoresForNewColumn( CatalogColumn addedColumn ) {
        CatalogTable catalogTable = Catalog.getInstance().getTable( addedColumn.tableId );
        return ImmutableList.of( AdapterManager.getInstance().getStore( catalogTable.dataPlacements.get( 0 ) ) );
    }


    @Override
    public List<DataStore> getDataStoresForNewTable( long schemaId ) {

        List<DataStore> stores = PoliciesManager.getInstance().makeDecision( DataStore.class, Action.CHECK_STORES_ADD, schemaId, null );
        if ( stores.isEmpty() ) {
            throw new RuntimeException( "Not possible to create Table because there is no persistent Datastore available." );
        } else {
            // todo ig: first improvement to choose the best store not just the first store.
            return Collections.singletonList( stores.get( 0 ));
        }
    }

}
