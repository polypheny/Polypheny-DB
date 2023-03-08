/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.catalog.entity;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import org.polypheny.db.catalog.logistic.PlacementType;


@EqualsAndHashCode
@Value
public class CatalogColumnPlacement implements CatalogObject {

    private static final long serialVersionUID = -1909757888176291095L;

    @Serialize
    public long namespaceId;
    @Serialize
    public long tableId;
    @Serialize
    public long columnId;
    @Serialize
    public long adapterId;
    @Serialize
    public PlacementType placementType;
    @Serialize
    public long physicalPosition;
    @Serialize
    public String physicalSchemaName;
    @Serialize
    public String physicalColumnName;


    public CatalogColumnPlacement(
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("tableId") final long tableId,
            @Deserialize("columnId") final long columnId,
            @Deserialize("adapterId") final long adapterId,
            @Deserialize("placementType") @NonNull final PlacementType placementType,
            @Deserialize("physicalSchemaName") final String physicalSchemaName,
            @Deserialize("physicalColumnName") final String physicalColumnName,
            @Deserialize("physicalPosition") final long physicalPosition ) {
        this.namespaceId = namespaceId;
        this.tableId = tableId;
        this.columnId = columnId;
        this.adapterId = adapterId;
        this.placementType = placementType;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalColumnName = physicalColumnName;
        this.physicalPosition = physicalPosition;
    }



    @SneakyThrows
    public String getLogicalTableName() {
        throw new org.apache.commons.lang3.NotImplementedException();
    }


    @SneakyThrows
    public String getLogicalColumnName() {
        //return Catalog.getInstance().getLogicalRel( namespaceId ).getColumn( columnId ).name;
        throw new org.apache.commons.lang3.NotImplementedException();
    }


    @SneakyThrows
    public String getAdapterUniqueName() {
        // return Catalog.getInstance().getAdapter( adapterId ).uniqueName;
        throw new org.apache.commons.lang3.NotImplementedException();
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getLogicalTableName(),
                placementType.name(),
                physicalSchemaName,
                physicalColumnName };
    }

}
