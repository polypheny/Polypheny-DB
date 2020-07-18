/*
 * Copyright 2019-2020 The Polypheny Project
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
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;


public class CatalogColumnPlacement implements CatalogEntity {

    private static final long serialVersionUID = 4754069156177607149L;

    public final long tableId;
    public final long columnId;
    public final int storeId;
    public final String storeUniqueName;
    public final PlacementType placementType;

    public final String physicalSchemaName;
    public final String physicalTableName;
    public final String physicalColumnName;


    public CatalogColumnPlacement(
            final long tableId,
            final long columnId,
            final int storeId,
            @NonNull final String storeUniqueName,
            @NonNull final PlacementType placementType,
            final String physicalSchemaName,
            final String physicalTableName,
            final String physicalColumnName ) {
        this.tableId = tableId;
        this.columnId = columnId;
        this.storeId = storeId;
        this.storeUniqueName = storeUniqueName;
        this.placementType = placementType;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physicalTableName;
        this.physicalColumnName = physicalColumnName;
    }


    @SneakyThrows
    public String getLogicalTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }


    @SneakyThrows
    public String getLogicalColumnName() {
        return Catalog.getInstance().getColumn( columnId ).name;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getLogicalTableName(),
                storeUniqueName,
                placementType.name(),
                physicalSchemaName,
                physicalTableName,
                physicalColumnName };
    }

}
