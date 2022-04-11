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

package org.polypheny.db.replication;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;


public class ReplicationEngineProviderImpl extends ReplicationEngineProvider {

    List<ReplicationEngine> preRegisteredEngines = new ArrayList<>();


    // This is only necessary if the replication engine needs processing on startup.
    // Otherwise, it's fine to initiate the engine once it is first used.
    public void enableReplicationEngines() {
        preRegisteredEngines.add( LazyReplicationEngine.getInstance() );
    }


    public ReplicationEngine getReplicationEngine( ReplicationStrategy replicationStrategy ) {
        switch ( replicationStrategy ) {

            case LAZY:
                return LazyReplicationEngine.getInstance();

            /*
            case YourCustomStrategy:
                return YourCustomReplicationEngine.getInstance();

             */

            default:
                throw new RuntimeException( "This Replication Strategy seems not to be supported!" );

        }
    }

}
