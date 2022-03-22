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

package org.polypheny.db.adaptiveness.selfadaptiveness;

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

@Slf4j
public enum Action {

    /**
     * If the original Action was to add a Table, the Self-Adaptive Action is to move the table to a better suited store
     * therefore, first the new placement is added and after the worst old data placement is deleted.
     */
    CHECK_STORES_ADD {
        @Override
        public <T> void redo( Decision decision, Transaction transaction ) {
            if ( decision.getPreSelection() != null ) {
                // Do nothing, User selected something do not change that
                return;
            }
            Catalog catalog = Catalog.getInstance();
            DdlManager ddlManager = DdlManager.getInstance();
            AdapterManager adapterManager = AdapterManager.getInstance();
            CatalogTable catalogTable = catalog.getTable( decision.getEntityId() );
            DataStore bestDataStore = WeightedList.getBest( decision.getWeightedList() );

            if ( !catalogTable.dataPlacements.contains( bestDataStore.getAdapterId() ) ) {

                try {
                    ddlManager.addDataPlacement(
                            catalogTable,
                            catalogTable.columnIds,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            bestDataStore,
                            transaction.createStatement() );
                } catch ( PlacementAlreadyExistsException e ) {
                    log.warn( "Not possible to add this Placement because it already exists (Self-Adaptive-System)." );
                    throw new RuntimeException( "Not possible to add this Placement because it already exists (Self-Adaptive-System)." );
                }

                Pair<Object, Double> worstDataPlacement = null;
                for ( Integer dataPlacement : catalogTable.dataPlacements ) {

                    DataStore dataStore = adapterManager.getStore( dataPlacement );
                    Double storeRanking = (Double) decision.getWeightedList().get( dataStore );

                    if ( decision.getWeightedList().containsKey( dataStore ) ) {
                        if ( worstDataPlacement == null ) {
                            worstDataPlacement = new Pair<>( dataStore, storeRanking );
                        } else if ( worstDataPlacement.right < storeRanking ) {
                            worstDataPlacement = new Pair<>( dataStore, storeRanking );
                        }
                    }
                }

                if ( worstDataPlacement != null ) {
                    try {
                        ddlManager.dropDataPlacement( catalogTable, (DataStore) worstDataPlacement.left, transaction.createStatement() );
                    } catch ( PlacementNotExistsException | LastPlacementException e ) {
                        log.warn( "Not possible to drop this Placement (Self-Adaptive-System)." );
                        throw new RuntimeException( "Not possible to drop this Placement (Self-Adaptive-System)." );

                    }
                }

            }

        }
    },
    CHECK_STORES_DELETE {
        @Override
        public <T> void redo( Decision decision, Transaction transaction ) {

        }

    };


    public abstract <T> void redo( Decision decision, Transaction transaction );


}
