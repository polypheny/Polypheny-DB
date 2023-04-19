/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.entity.allocation;

import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Serialize;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PartitionType;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@Slf4j
@Builder(toBuilder = true)
public abstract class AllocationEntity extends CatalogEntity {

    @Serialize
    public long adapterId;
    @Serialize
    public long logicalId;
    @Serialize
    public ImmutableList<Long> physicalIds;


    protected AllocationEntity(
            long id,
            long logicalId,
            long namespaceId,
            long adapterId,
            NamespaceType type,
            @Nullable List<Long> physicalIds ) {
        super( id, null, namespaceId, EntityType.ENTITY, type, true );
        this.adapterId = adapterId;
        this.logicalId = logicalId;
        this.physicalIds = physicalIds == null ? null : ImmutableList.copyOf( physicalIds );
    }


    public State getCatalogType() {
        return State.ALLOCATION;
    }


    public PartitionType getPartitionType() {
        log.warn( "change me" );
        return PartitionType.NONE;
    }

}
