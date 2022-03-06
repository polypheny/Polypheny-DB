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

package org.polypheny.db.policies.policy;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.policies.policy.Clause.ClauseCategory;

@Slf4j
public class ClauseCategoryInformation {

    public void getCategoryInformation( ClauseCategory clauseCategory ){

        switch ( clauseCategory ){
            case STORE:
                Map<String, DataStore> allStores = AdapterManager.getInstance().getStores();
                for ( DataStore value : allStores.values() ) {

                }
                break;
            default:
                log.warn( "Not implemented or no Information for ClauseCategory: " + clauseCategory.toString() );

        }

    }

}
