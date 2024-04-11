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

import java.util.concurrent.atomic.AtomicLong;
import lombok.Value;

@Value
public class IdBuilder {

    AtomicLong snapshotId;
    AtomicLong namespaceId;
    AtomicLong entityId;

    AtomicLong physicalId;

    AtomicLong allocId;
    AtomicLong fieldId;

    AtomicLong userId;

    AtomicLong indexId;

    AtomicLong keyId;

    AtomicLong adapterId;

    AtomicLong adapterTemplateId;

    AtomicLong interfaceId;

    AtomicLong constraintId;

    AtomicLong groupId;

    AtomicLong partitionId;
    AtomicLong placementId;

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
                new AtomicLong( 0 ),
                new AtomicLong( 0 ) );
    }


    public IdBuilder(
            AtomicLong snapshotId,
            AtomicLong namespaceId,
            AtomicLong entityId,
            AtomicLong fieldId,
            AtomicLong userId,
            AtomicLong allocId,
            AtomicLong physicalId,
            AtomicLong indexId,
            AtomicLong keyId,
            AtomicLong adapterId,
            AtomicLong adapterTemplateId,
            AtomicLong interfaceId,
            AtomicLong constraintId,
            AtomicLong groupId,
            AtomicLong partitionId,
            AtomicLong placementId ) {
        this.snapshotId = snapshotId;
        this.namespaceId = namespaceId;
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


    public long getNewNamespaceId() {
        return namespaceId.getAndIncrement();
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

}
