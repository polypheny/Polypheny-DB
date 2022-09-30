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

import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;


@EqualsAndHashCode
public class CatalogColumnPlacement implements CatalogObject {

    private static final long serialVersionUID = -1909757888176291095L;

    public final long tableId;
    public final long columnId;
    public final int adapterId;
    public final String adapterUniqueName;
    public final PlacementType placementType;

    public final long physicalPosition;

    public final String physicalSchemaName;
    public final String physicalColumnName;


    public CatalogColumnPlacement(
            final long tableId,
            final long columnId,
            final int adapterId,
            @NonNull final String adapterUniqueName,
            @NonNull final PlacementType placementType,
            final String physicalSchemaName,
            final String physicalColumnName,
            final long physicalPosition ) {
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
        return Catalog.getInstance().getTable( tableId ).getNamespaceName();
    }


    @SneakyThrows
    public String getLogicalTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }


    @SneakyThrows
    public String getLogicalColumnName() {
        return Catalog.getInstance().getColumn( columnId ).name;
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
