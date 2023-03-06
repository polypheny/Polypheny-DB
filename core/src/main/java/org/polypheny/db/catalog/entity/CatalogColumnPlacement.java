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

import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.PlacementType;


@EqualsAndHashCode
public class CatalogColumnPlacement implements CatalogObject {

    private static final long serialVersionUID = -1909757888176291095L;

    @Serialize
    public final long namespaceId;
    @Serialize
    public final long tableId;
    @Serialize
    public final long columnId;
    @Serialize
    public final long adapterId;
    @Serialize
    public final String adapterUniqueName;
    @Serialize
    public final PlacementType placementType;
    @Serialize
    public final long physicalPosition;
    @Serialize
    public final String physicalSchemaName;
    @Serialize
    public final String physicalColumnName;


    public CatalogColumnPlacement(
            final long namespaceId,
            final long tableId,
            final long columnId,
            final long adapterId,
            @NonNull final String adapterUniqueName,
            @NonNull final PlacementType placementType,
            final String physicalSchemaName,
            final String physicalColumnName,
            final long physicalPosition ) {
        this.namespaceId = namespaceId;
        this.tableId = tableId;
        this.columnId = columnId;
        this.adapterId = adapterId;
        this.adapterUniqueName = adapterUniqueName;
        this.placementType = placementType;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalColumnName = physicalColumnName;
        this.physicalPosition = physicalPosition;
    }


    @SneakyThrows
    public String getLogicalSchemaName() {
        throw new NotImplementedException();
    }


    @SneakyThrows
    public String getLogicalTableName() {
        return Catalog.getInstance().getLogicalRel( namespaceId ).getTable( tableId ).name;
    }


    @SneakyThrows
    public String getLogicalColumnName() {
        return Catalog.getInstance().getLogicalRel( namespaceId ).getColumn( columnId ).name;
    }


    @SneakyThrows
    public String getAdapterUniqueName() {
        return Catalog.getInstance().getAdapter( adapterId ).uniqueName;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getLogicalTableName(),
                adapterUniqueName,
                placementType.name(),
                physicalSchemaName,
                physicalColumnName };
    }

}
