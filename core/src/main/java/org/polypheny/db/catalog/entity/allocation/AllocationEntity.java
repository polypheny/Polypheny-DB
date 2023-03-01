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

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public abstract class AllocationEntity<L extends LogicalEntity> extends LogicalEntity {

    public long adapterId;
    public L logical;


    protected AllocationEntity( L logical, long id, String name, String namespaceName, EntityType type, NamespaceType namespaceType, long adapterId ) {
        super( id, name, namespaceName, type, namespaceType );
        this.adapterId = adapterId;
        this.logical = logical;
    }


    public State getCatalogType() {
        return State.ALLOCATION;
    }


}
