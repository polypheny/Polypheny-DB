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

package org.polypheny.db.partition;


import org.polypheny.db.catalog.logistic.PartitionType;


public abstract class PartitionManagerFactory {


    public static PartitionManagerFactory INSTANCE = null;


    public static PartitionManagerFactory setAndGetInstance( PartitionManagerFactory factory ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Setting the PartitionManager, when already set is not permitted." );
        }
        INSTANCE = factory;
        return INSTANCE;
    }


    public static PartitionManagerFactory getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "PartitionManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract PartitionManager getPartitionManager( PartitionType partitionType );

}
