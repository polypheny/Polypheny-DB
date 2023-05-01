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

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.logistic.PlacementType;


@EqualsAndHashCode
@Value
@With
public class AllocationColumn implements CatalogObject {

    private static final long serialVersionUID = -1909757888176291095L;

    @Serialize
    public long namespaceId;
    @Serialize
    public long tableId;
    @Serialize
    public long columnId;
    @Serialize
    public PlacementType placementType;
    @Serialize
    public int position;
    @Serialize
    public long adapterId;


    public AllocationColumn(
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("tableId") final long tableId,
            @Deserialize("columnId") final long columnId,
            @Deserialize("placementType") @NonNull final PlacementType placementType,
            @Deserialize("position") final int position,
            @Deserialize("adapterId") final long adapterId ) {
        this.namespaceId = namespaceId;
        this.tableId = tableId;
        this.columnId = columnId;
        this.placementType = placementType;
        this.position = position;
        this.adapterId = adapterId;
    }



    public String getLogicalTableName() {
        return Catalog.snapshot().rel().getTable( tableId ).name;
    }


    public String getLogicalColumnName() {
        return Catalog.snapshot().rel().getColumn( columnId ).name;
    }



    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getLogicalTableName(),
                placementType.name() };
    }


    public AlgDataType getAlgDataType() {
        return Catalog.snapshot().rel().getColumn( columnId ).getAlgDataType( AlgDataTypeFactory.DEFAULT );
    }

}
