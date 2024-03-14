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
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
public class AllocationColumnModel extends IdEntity {

    @JsonProperty
    public long namespaceId;

    @JsonProperty
    public long placementId;

    @JsonProperty
    public long logicalTableId;

    @JsonProperty
    public PlacementType placementType;

    @JsonProperty
    public int position;

    @JsonProperty
    public long adapterId;


    public AllocationColumnModel(
            @JsonProperty("namespaceId") long namespaceId,
            @JsonProperty("placementId") long placementId,
            @JsonProperty("logicalTableId") long logicalTableId,
            @JsonProperty("columnId") long columnId,
            @JsonProperty("placementType") PlacementType placementType,
            @JsonProperty("position") int position,
            @JsonProperty("adapterId") long adapterId ) {
        super( columnId, "" );
        this.namespaceId = namespaceId;
        this.placementId = placementId;
        this.logicalTableId = logicalTableId;
        this.placementType = placementType;
        this.position = position;
        this.adapterId = adapterId;
    }


    public static AllocationColumnModel from( AllocationColumn column ) {
        return new AllocationColumnModel(
                column.namespaceId,
                column.placementId,
                column.logicalTableId,
                column.columnId,
                column.placementType,
                column.position,
                column.adapterId );
    }

}
