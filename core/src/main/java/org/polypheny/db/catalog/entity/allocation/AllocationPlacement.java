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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import lombok.Value;
import org.polypheny.db.catalog.entity.PolyObject;

@Value
public class AllocationPlacement implements PolyObject {

    @Serialize
    @JsonProperty
    public long id;
    @Serialize
    @JsonProperty
    public long adapterId;
    @Serialize
    @JsonProperty
    public long namespaceId;
    @Serialize
    @JsonProperty
    public long logicalEntityId;


    public AllocationPlacement(
            @Deserialize("id") long id,
            @Deserialize("logicalEntityId") long logicalEntityId,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("adapterId") long adapterId ) {
        this.adapterId = adapterId;
        this.namespaceId = namespaceId;
        this.logicalEntityId = logicalEntityId;
        this.id = id;
    }

}
