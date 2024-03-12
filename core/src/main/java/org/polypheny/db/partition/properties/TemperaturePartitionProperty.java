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

package org.polypheny.db.partition.properties;

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.logistic.PartitionType;


@SuperBuilder
@Getter
public class TemperaturePartitionProperty extends PartitionProperty {

    // Cost Model, Access Frequency: ALL, READ FREQUENCY, WRITE FREQUENCY
    public enum PartitionCostIndication {ALL, READ, WRITE}


    private final PartitionCostIndication partitionCostIndication;
    private final PartitionType internalPartitionFunction;

    // Maybe get default if left empty, centrally by configuration
    private final int hotAccessPercentageIn;
    private final int hotAccessPercentageOut;

    private final long frequencyInterval;

    private final long hotPartitionGroupId;
    private final long coldPartitionGroupId;

}
