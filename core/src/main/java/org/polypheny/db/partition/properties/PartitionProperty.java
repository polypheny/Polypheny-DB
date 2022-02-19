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

package org.polypheny.db.partition.properties;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.Catalog.PartitionType;


@SuperBuilder
@Getter
public class PartitionProperty implements Serializable {

    public final PartitionType partitionType;
    public final boolean isPartitioned;
    public final ImmutableList<Long> partitionGroupIds;
    public final ImmutableList<Long> partitionIds;
    public final long partitionColumnId;

    public final long numPartitionGroups;
    public final long numPartitions;

    public final boolean reliesOnPeriodicChecks;

}
