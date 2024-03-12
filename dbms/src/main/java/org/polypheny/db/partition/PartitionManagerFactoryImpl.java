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


public class PartitionManagerFactoryImpl extends PartitionManagerFactory {

    @Override
    public PartitionManager getPartitionManager( PartitionType partitionType ) {
        return switch ( partitionType ) {
            case HASH -> new HashPartitionManager();
            case LIST -> new ListPartitionManager();
            case RANGE -> new RangePartitionManager();

            // TODO @HENNLO think about excluding "UDPF" here, these should only be used for internal Partition Functions
            // Or create an internal mapping from PARTITIONTYPE to teh handling partition manager
            case TEMPERATURE -> new TemperatureAwarePartitionManager();
            case NONE -> new NonePartitionManager();
        };

    }

}
