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

import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.logistic.PartitionType;


@SuperBuilder
@Getter
@Value
@NonFinal
public class PartitionProperty implements Serializable {

    @Serialize
    public long entityId;

    @Serialize
    public PartitionType partitionType;
    @Serialize
    public boolean isPartitioned;
    @Serialize
    public ImmutableList<Long> partitionGroupIds;
    @Serialize
    public ImmutableList<Long> partitionIds;
    @Serialize
    @Builder.Default
    @NonFinal
    public long partitionColumnId = -1;

    @Serialize
    public long numPartitionGroups;
    @Serialize
    public long numPartitions;

    @Serialize
    public boolean reliesOnPeriodicChecks;


    public PartitionProperty(
            @Deserialize("entityId") long entityId,
            @Deserialize("partitionType") PartitionType partitionType,
            @Deserialize("isPartitioned") boolean isPartitioned,
            @Deserialize("partitionGroupIds") List<Long> partitionGroupIds,
            @Deserialize("partitionIds") List<Long> partitionIds,
            @Deserialize("partitionColumnId") long partitionColumnId,
            @Deserialize("numPartitionGroups") long numPartitionGroups,
            @Deserialize("numPartitions") long numPartitions,
            @Deserialize("reliesOnPeriodicChecks") boolean reliesOnPeriodicChecks ) {
        this.entityId = entityId;
        this.partitionType = partitionType;
        this.isPartitioned = isPartitioned;
        this.partitionGroupIds = ImmutableList.copyOf( partitionGroupIds );
        this.partitionIds = ImmutableList.copyOf( partitionIds );
        this.partitionColumnId = partitionColumnId;
        this.numPartitionGroups = numPartitionGroups;
        this.numPartitions = numPartitions;
        this.reliesOnPeriodicChecks = reliesOnPeriodicChecks;
    }

}
