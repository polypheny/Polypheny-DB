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

package org.polypheny.db.catalog.entity.physical;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@SuperBuilder(toBuilder = true)
@SerializeClass(subclasses = { PhysicalTable.class, PhysicalGraph.class, PhysicalCollection.class })
public abstract class PhysicalEntity extends Entity {

    @Serialize
    public String namespaceName;

    @Serialize
    public long adapterId;

    @Serialize
    public long allocationId;

    @Serialize
    public long logicalId;

    @Serialize
    public List<Long> uniqueFieldIds;


    protected PhysicalEntity( long id, long allocationId, long logicalId, String name, long namespaceId, String namespaceName, List<Long> uniqueFieldIds, DataModel dataModel, long adapterId ) {
        super( id, name, namespaceId, EntityType.ENTITY, dataModel, true );
        this.uniqueFieldIds = uniqueFieldIds;
        this.allocationId = allocationId;
        this.namespaceName = namespaceName;
        this.adapterId = adapterId;
        this.logicalId = logicalId;
    }


    @Override
    public State getLayer() {
        return State.PHYSICAL;
    }


    @Override
    public double getTupleCount() {
        return getTupleCount( logicalId );
    }


    public abstract PhysicalEntity normalize();

}
