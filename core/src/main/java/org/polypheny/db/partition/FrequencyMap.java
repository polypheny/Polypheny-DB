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

import org.polypheny.db.catalog.entity.logical.LogicalTable;


public abstract class FrequencyMap {

    public static FrequencyMap INSTANCE = null;


    public static FrequencyMap setAndGetInstance( FrequencyMap frequencyMap ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the FrequencyMap, when already set is not permitted." );
        }
        INSTANCE = frequencyMap;
        return INSTANCE;
    }


    public abstract void initialize();

    public abstract void terminate();

    public abstract void determinePartitionFrequency( LogicalTable table, long invocationTimestamp );

}
