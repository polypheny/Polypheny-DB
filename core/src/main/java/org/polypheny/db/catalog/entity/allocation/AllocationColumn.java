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
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.logistic.PlacementType;


@Value
@SuperBuilder(toBuilder = true)
public class AllocationColumn implements PolyObject {

    private static final long serialVersionUID = -1909757888176291095L;

    @Serialize
    @JsonProperty
    public long namespaceId;
    @Serialize
    @JsonProperty
    public long placementId;
    @Serialize
    @JsonProperty
    public long logicalTableId;
    @Serialize
    @JsonProperty
    public long columnId;
    @Serialize
    @JsonProperty
    public PlacementType placementType;
    @Serialize
    @JsonProperty
    public int position;
    @Serialize
    @JsonProperty
    public long adapterId;


    public AllocationColumn(
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("placementId") final long placementId,
            @Deserialize("logicalTableId") final long logicalTableId,
            @Deserialize("columnId") final long columnId,
            @Deserialize("placementType") @NonNull final PlacementType placementType,
            @Deserialize("position") final int position, // same as logical
            @Deserialize("adapterId") final long adapterId ) {
        this.namespaceId = namespaceId;
        this.placementId = placementId;
        this.logicalTableId = logicalTableId;
        this.columnId = columnId;
        this.placementType = placementType;
        this.position = position;
        this.adapterId = adapterId;
    }


    public String getLogicalColumnName() {
        return Catalog.snapshot().rel().getColumn( columnId ).orElseThrow().name;
    }


    public AlgDataType getAlgDataType() {
        return Catalog.snapshot().rel().getColumn( columnId ).orElseThrow().getAlgDataType( AlgDataTypeFactory.DEFAULT );
    }

}
