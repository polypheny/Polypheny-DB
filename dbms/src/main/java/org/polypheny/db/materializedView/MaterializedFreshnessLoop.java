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

package org.polypheny.db.materializedView;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;

@Slf4j
public class MaterializedFreshnessLoop implements Runnable {


    private final MaterializedManagerImpl manager;


    public MaterializedFreshnessLoop( MaterializedManagerImpl manager ) {
        this.manager = manager;
    }


    @Override
    public void run() {

        try {
            startEventLoop();
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }
    }


    private void startEventLoop() throws InterruptedException {
        Map<Long, MaterializedCriteria> materializedViewInfo;
        while ( true ) {
            materializedViewInfo = ImmutableMap.copyOf( manager.updateMaterializedViewInfo() );
            materializedViewInfo.forEach( ( k, v ) -> {

                if ( v.getCriteriaType() == CriteriaType.INTERVAL ) {
                    if ( v.getLastUpdate().getTime() + v.getTimeInMillis() < System.currentTimeMillis() ) {

                        manager.prepareToUpdate( k );
                        manager.updateMaterializedTime( k );
                    }
                }
            } );

            Thread.sleep( 1000 );
        }

    }

}
