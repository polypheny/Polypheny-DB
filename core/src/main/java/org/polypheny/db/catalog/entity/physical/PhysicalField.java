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

package org.polypheny.db.catalog.entity.physical;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode(callSuper = true)
@Value
@SuperBuilder(toBuilder = true)
@NonFinal
@SerializeClass(subclasses = PhysicalColumn.class)
public abstract class PhysicalField extends CatalogEntity {

    @Serialize
    public long adapterId;

    @Serialize
    public long entityId;

    @Serialize
    public String logicalName;

    @Serialize
    public long allocId;


    public PhysicalField(
            final long id,
            final String name,
            final String logicalName,
            final long allocId,
            final long entityId,
            final long adapterId,
            final NamespaceType namespaceType,
            final boolean modifiable ) {
        super( id, name, allocId, EntityType.ENTITY, namespaceType, modifiable );
        this.logicalName = logicalName;
        this.entityId = entityId;
        this.allocId = allocId;
        this.adapterId = adapterId;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public State getCatalogType() {
        return State.PHYSICAL;
    }


    @Override
    public Expression asExpression() {
        return null;
    }

}
