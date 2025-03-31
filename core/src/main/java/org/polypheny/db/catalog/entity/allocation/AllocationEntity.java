/*
 * Copyright 2019-2025 The Polypheny Project
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeVarLength;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.PartitionType;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@Slf4j
@SuperBuilder(toBuilder = true)
@SerializeClass(subclasses = { AllocationTable.class, AllocationGraph.class, AllocationCollection.class })
@JsonTypeInfo(use = Id.CLASS)
public abstract class AllocationEntity extends Entity {

    public static String PREFIX = "$alloc$";

    @Serialize
    @JsonProperty
    public long adapterId;

    @Serialize
    @JsonProperty
    public long logicalId;

    @Serialize
    @JsonProperty
    @SerializeVarLength
    public long partitionId;

    @Serialize
    @JsonProperty
    @SerializeVarLength
    public long placementId;


    protected AllocationEntity(
            long id,
            long placementId,
            long partitionId,
            long logicalId,
            long namespaceId,
            long adapterId,
            DataModel type ) {
        super( id, PREFIX + id, namespaceId, EntityType.ENTITY, type, true );
        this.adapterId = adapterId;
        this.logicalId = logicalId;
        this.partitionId = partitionId;
        this.placementId = placementId;
    }


    public State getLayer() {
        return State.ALLOCATION;
    }


    @Override
    public double getTupleCount() {
        return getTupleCount( logicalId );
    }


    public PartitionType getPartitionType() {
        log.warn( "change me" );
        return PartitionType.NONE;
    }


    public abstract Entity withName( String name );

}
