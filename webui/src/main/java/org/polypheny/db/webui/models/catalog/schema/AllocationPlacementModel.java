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
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
public class AllocationPlacementModel extends IdEntity {

    @JsonProperty
    public long logicalEntityId;

    @JsonProperty
    public long adapterId;


    public AllocationPlacementModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("logicalEntityId") long logicalEntityId,
            @JsonProperty("adapterId") long adapterId ) {
        super( id, null );
        this.logicalEntityId = logicalEntityId;
        this.adapterId = adapterId;
    }


    public static AllocationPlacementModel from( AllocationPlacement placement ) {
        return new AllocationPlacementModel( placement.id, placement.logicalEntityId, placement.adapterId );
    }

}
