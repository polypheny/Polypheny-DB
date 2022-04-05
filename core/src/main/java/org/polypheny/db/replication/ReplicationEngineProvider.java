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


import org.polypheny.db.catalog.Catalog.ReplicationStrategy;


public abstract class ReplicationEngineProvider {

    public static ReplicationEngineProvider INSTANCE = null;


    public static ReplicationEngineProvider initializeReplicationEngines( ReplicationEngineProvider factory ) {

        // TODO @HENNLO here we could also start reprocessing outdated nodes that lost there changes

        if ( INSTANCE != null ) {
            throw new RuntimeException( "Initializing the ReplicationEngineProvider, when already set is not permitted." );
        }
        INSTANCE = factory;
        return INSTANCE;
    }


    public static ReplicationEngineProvider getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "ReplicationEngineProvider was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract ReplicationEngine getReplicationEngine( ReplicationStrategy replicationStrategy );

}
