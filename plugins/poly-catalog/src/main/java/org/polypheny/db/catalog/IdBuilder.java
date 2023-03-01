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

package org.polypheny.db.catalog;

import java.util.concurrent.atomic.AtomicLong;

public class IdBuilder {

    private final AtomicLong snapshotId;
    private final AtomicLong databaseId;
    private final AtomicLong namespaceId;
    private final AtomicLong entityId;
    private final AtomicLong fieldId;

    private final AtomicLong userId;

    private final AtomicLong verticalId;

    private final AtomicLong horizontalId;
    private final AtomicLong indexId;

    private final AtomicLong keyId;

    private final AtomicLong adapterId;

    private final AtomicLong interfaceId;


    public IdBuilder() {
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
                new AtomicLong( 0 ) );
    }


    public IdBuilder(
            AtomicLong snapshotId,
            AtomicLong databaseId,
            AtomicLong namespaceId,
            AtomicLong entityId,
            AtomicLong fieldId,
            AtomicLong userId,
            AtomicLong verticalId,
            AtomicLong horizontalId,
            AtomicLong indexId,
            AtomicLong keyId,
            AtomicLong adapterId,
            AtomicLong interfaceId ) {
        this.snapshotId = snapshotId;

        this.databaseId = databaseId;
        this.namespaceId = namespaceId;
        this.entityId = entityId;
        this.fieldId = fieldId;

        this.indexId = indexId;
        this.keyId = keyId;
        this.userId = userId;
        this.verticalId = verticalId;
        this.horizontalId = horizontalId;

        this.adapterId = adapterId;
        this.interfaceId = interfaceId;
    }


    public long getNewSnapshotId() {
        return snapshotId.getAndIncrement();
    }


    public long getNewEntityId() {
        return entityId.getAndIncrement();
    }


    public long getNewFieldId() {
        return fieldId.getAndIncrement();
    }


    public long getNewDatabaseId() {
        return databaseId.getAndIncrement();
    }


    public long getNewNamespaceId() {
        return namespaceId.getAndIncrement();
    }


    public long getNewUserId() {
        return userId.getAndIncrement();
    }


    public long getNewVerticalId() {
        return verticalId.getAndIncrement();
    }


    public long getNewHorizontalId() {
        return horizontalId.getAndIncrement();
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

}
