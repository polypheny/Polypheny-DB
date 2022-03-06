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

package org.polypheny.db.monitoring.statistics;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.monitoring.statistics.models.StoreInformation;

@Slf4j
public class StatisticPolypheny {

    private Map<Integer, StoreInformation> storeInformation = new HashMap<>();


    public void updatePolyphenyStatistic() {

        // update store information
        Map<String, DataStore> allStores = AdapterManager.getInstance().getStores();
        Map<Integer, StoreInformation> updatedStoreInformation = new HashMap<>();
        for ( DataStore store : allStores.values() ) {
            int storeId = store.getAdapterId();

            StoreInformation storeInfo;
            if ( storeInformation.containsKey( storeId ) ) {
                storeInfo = storeInformation.remove( storeId );
            } else {
                storeInfo = new StoreInformation( storeId, store.getDeployMode(), store.isPersistent() );
            }

            updatedStoreInformation.put( storeId, storeInfo );
        }
        storeInformation = updatedStoreInformation;


    }

}
