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

package org.polypheny.db.catalog;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;

@Value
public class IdBuilder {

    @Serialize
    public AtomicLong snapshotId;

    @Serialize
    public AtomicLong entityId;

    @Serialize
    public AtomicLong physicalId;

    @Serialize
    public AtomicLong allocId;

    @Serialize
    public AtomicLong fieldId;

    @Serialize
    public AtomicLong userId;

    @Serialize
    public AtomicLong indexId;

    @Serialize
    public AtomicLong keyId;

    @Serialize
    public AtomicLong adapterId;

    @Serialize
    public AtomicLong adapterTemplateId;

    @Serialize
    public AtomicLong interfaceId;

    @Serialize
    public AtomicLong constraintId;

    @Serialize
    public AtomicLong groupId;

    @Serialize
    public AtomicLong partitionId;

    @Serialize
    public AtomicLong placementId;

    private static IdBuilder INSTANCE;


    public static IdBuilder getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new IdBuilder();
        }
        return INSTANCE;
    }


    private IdBuilder() {
        this(
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ),
                new AtomicLong( 0 ) );
    }


    public IdBuilder(
            @Deserialize("snapshotId") AtomicLong snapshotId,
            @Deserialize("entityId") AtomicLong entityId,
            @Deserialize("fieldId") AtomicLong fieldId,
            @Deserialize("userId") AtomicLong userId,
            @Deserialize("allocId") AtomicLong allocId,
            @Deserialize("physicalId") AtomicLong physicalId,
            @Deserialize("indexId") AtomicLong indexId,
            @Deserialize("keyId") AtomicLong keyId,
            @Deserialize("adapterId") AtomicLong adapterId,
            @Deserialize("adapterTemplateId") AtomicLong adapterTemplateId,
            @Deserialize("interfaceId") AtomicLong interfaceId,
            @Deserialize("constraintId") AtomicLong constraintId,
            @Deserialize("groupId") AtomicLong groupId,
            @Deserialize("partitionId") AtomicLong partitionId,
            @Deserialize("placementId") AtomicLong placementId ) {
        this.snapshotId = snapshotId;
        this.entityId = entityId;
        this.fieldId = fieldId;

        this.indexId = indexId;
        this.keyId = keyId;
        this.userId = userId;
        this.allocId = allocId;
        this.physicalId = physicalId;
        this.constraintId = constraintId;

        this.adapterId = adapterId;
        this.adapterTemplateId = adapterTemplateId;
        this.interfaceId = interfaceId;
        this.groupId = groupId;
        this.partitionId = partitionId;
        this.placementId = placementId;
    }


    public long getNewSnapshotId() {
        return snapshotId.getAndIncrement();
    }


    public long getNewLogicalId() {
        return entityId.getAndIncrement();
    }


    public long getNewFieldId() {
        return fieldId.getAndIncrement();
    }


    public long getNewUserId() {
        return userId.getAndIncrement();
    }


    public long getNewAllocId() {
        return allocId.getAndIncrement();
    }


    public long getNewIndexId() {
        return indexId.getAndIncrement();
    }


    public long getNewKeyId() {
        return keyId.getAndIncrement();
    }


    public long getNewAdapterId() {
        return adapterId.getAndIncrement();
    }


    public long getNewInterfaceId() {
        return interfaceId.getAndIncrement();
    }


    public long getNewConstraintId() {
        return constraintId.getAndIncrement();
    }


    public long getNewPhysicalId() {
        return physicalId.getAndIncrement();
    }


    public long getNewGroupId() {
        return groupId.getAndIncrement();
    }


    public long getNewPartitionId() {
        return partitionId.getAndIncrement();
    }


    public long getNewPlacementId() {
        return placementId.getAndIncrement();
    }


    public long getNewAdapterTemplateId() {
        return adapterTemplateId.getAndIncrement();
    }


    public synchronized void restore( IdBuilder idBuilder ) {
        this.snapshotId.set( idBuilder.snapshotId.longValue() );
        this.entityId.set( idBuilder.entityId.longValue() );
        this.fieldId.set( idBuilder.fieldId.longValue() );

        this.indexId.set( idBuilder.indexId.longValue() );
        this.keyId.set( idBuilder.keyId.longValue() );
        this.userId.set( idBuilder.userId.longValue() );
        this.allocId.set( idBuilder.allocId.longValue() );
        this.physicalId.set( idBuilder.physicalId.longValue() );
        this.constraintId.set( idBuilder.constraintId.longValue() );

        this.adapterId.set( idBuilder.adapterId.longValue() );
        this.adapterTemplateId.set( idBuilder.adapterTemplateId.longValue() );
        this.interfaceId.set( idBuilder.interfaceId.longValue() );
        this.groupId.set( idBuilder.groupId.longValue() );
        this.partitionId.set( idBuilder.partitionId.longValue() );
        this.placementId.set( idBuilder.placementId.longValue() );
    }

}
