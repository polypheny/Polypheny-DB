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

    private final AtomicLong databaseId;
    public final AtomicLong namespaceId;
    public final AtomicLong entityId;
    public final AtomicLong fieldId;

    public final AtomicLong userId;

    public final AtomicLong verticalId;

    public final AtomicLong horizontalId;


    public IdBuilder() {
        this( new AtomicLong( 0 ), new AtomicLong( 0 ), new AtomicLong( 0 ), new AtomicLong( 0 ), new AtomicLong( 0 ), new AtomicLong( 0 ), new AtomicLong( 0 ) );
    }


    public IdBuilder( AtomicLong databaseId, AtomicLong namespaceId, AtomicLong entityId, AtomicLong fieldId, AtomicLong userId, AtomicLong verticalId, AtomicLong horizontalId ) {
        this.databaseId = databaseId;
        this.namespaceId = namespaceId;
        this.entityId = entityId;
        this.fieldId = fieldId;

        this.userId = userId;
        this.verticalId = verticalId;
        this.horizontalId = horizontalId;
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

}
