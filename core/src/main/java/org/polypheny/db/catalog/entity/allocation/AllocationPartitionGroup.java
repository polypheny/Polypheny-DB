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

package org.polypheny.db.catalog.entity.allocation;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode
@Value
public class AllocationPartitionGroup implements PolyObject {

    private static final long serialVersionUID = 6229244317971622972L;

    @Serialize
    public long id;
    @Serialize
    @SerializeNullable
    public String name;
    @Serialize
    public long logicalEntityId;
    @Serialize
    public long namespaceId;
    @Serialize
    public boolean isUnbound;
    @Serialize
    public long partitionKey;


    public AllocationPartitionGroup(
            @Deserialize("id") final long id,
            @Deserialize("name") @Nullable final String name,
            @Deserialize("logicalEntityId") final long logicalEntityId,
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("partitionKey") final long partitionKey,
            @Deserialize("isUnbound") final boolean isUnbound ) {
        this.id = id;
        this.name = name;
        this.logicalEntityId = logicalEntityId;
        this.namespaceId = namespaceId;
        this.partitionKey = partitionKey;
        this.isUnbound = isUnbound;
    }


    @Override
    public PolyValue[] getParameterArray() {
        throw new GenericRuntimeException( "Not implemented" );
    }

}
