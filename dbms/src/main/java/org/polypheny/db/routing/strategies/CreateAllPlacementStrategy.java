/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;


/**
 * Adds new columns and tables on all stores.
 */
public class CreateAllPlacementStrategy implements CreatePlacementStrategy {

    @Override
    public List<DataStore<?>> getDataStoresForNewRelField( LogicalColumn addedField ) {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        LogicalTable catalogTable = snapshot.rel().getTable( addedField.tableId ).orElseThrow();
        List<AllocationPlacement> placements = snapshot.alloc().getPlacementsFromLogical( catalogTable.id );
        return placements.stream()
                .map( elem -> AdapterManager.getInstance().getStore( elem.adapterId ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect( Collectors.toList() );
    }


    @Override
    public List<DataStore<?>> getDataStoresForNewEntity() {
        return AdapterManager.getInstance().getStores().values().asList();
    }

}
