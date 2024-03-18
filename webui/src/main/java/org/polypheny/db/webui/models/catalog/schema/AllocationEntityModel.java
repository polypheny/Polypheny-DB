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

package org.polypheny.db.webui.models.catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.webui.models.catalog.IdEntity;


@EqualsAndHashCode(callSuper = true)
@Value
public class AllocationEntityModel extends IdEntity {

    @JsonProperty
    public long logicalEntityId;

    @JsonProperty
    public long placementId;

    @JsonProperty
    public long partitionId;


    public AllocationEntityModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("logicalEntityId") long logicalEntityId,
            @JsonProperty("placementId") long placementId,
            @JsonProperty("partitionId") long partitionId ) {
        super( id, null );
        this.logicalEntityId = logicalEntityId;
        this.placementId = placementId;
        this.partitionId = partitionId;
    }


    public static AllocationEntityModel from( AllocationEntity entity ) {
        return new AllocationEntityModel( entity.id, entity.logicalId, entity.placementId, entity.partitionId );
    }

}
