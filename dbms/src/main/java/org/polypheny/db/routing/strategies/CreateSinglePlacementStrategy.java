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
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;


public class CreateSinglePlacementStrategy implements CreatePlacementStrategy {

    @Override
    public List<DataStore> getDataStoresForNewColumn( CatalogColumn addedColumn ) {
        CatalogTable catalogTable = Catalog.getInstance().getTable( addedColumn.tableId );
        return ImmutableList.of( AdapterManager.getInstance().getStore( catalogTable.dataPlacements.get( 0 ) ) );
    }


    @Override
    public List<DataStore> getDataStoresForNewTable() {
        Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
        for ( DataStore store : availableStores.values() ) {
            return ImmutableList.of( store );
        }
        throw new RuntimeException( "No suitable data store found" );
    }

}
