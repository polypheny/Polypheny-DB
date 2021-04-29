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
package org.polypheny.db.partition;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.partition.manager.HashPartitionManager;
import org.polypheny.db.partition.manager.ListPartitionManager;
import org.polypheny.db.partition.manager.PartitionManager;
import org.polypheny.db.partition.manager.RangePartitionManager;
import org.polypheny.db.partition.manager.TemperatureAwarePartitionManager;


public class PartitionManagerFactory {

    public PartitionManager getInstance( Catalog.PartitionType partitionType ) {
        switch ( partitionType ) {
            case HASH:
                return new HashPartitionManager();

            case LIST:
                return new ListPartitionManager();

            case RANGE:
                return new RangePartitionManager();

            //TODO @HENNLO think about excluding "UDPF" here, these should only be used for internal Partiiton Functions
            //Or create an internal mapping from PARTITIONTYPE to teh handling partition manager
            case TEMPERATURE:
                return new TemperatureAwarePartitionManager();
        }

        throw new RuntimeException( "Unknown partition type: " + partitionType );
    }

}
