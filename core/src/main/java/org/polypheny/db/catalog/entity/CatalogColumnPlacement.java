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
import org.polypheny.db.catalog.Catalog.PlacementType;


public class CatalogColumnPlacement implements CatalogEntity {

    private static final long serialVersionUID = 4754069156177607149L;

    public final long tableId;
    public final String tableName;
    public final long columnId;
    public final String columnName;
    public final int storeId;
    public final String storeUniqueName;
    public final PlacementType placementType;

    public final String physicalSchemaName;
    public final String physicalTableName;
    public final String physicalColumnName;


    public CatalogColumnPlacement(
            final long tableId,
            @NonNull final String tableName,
            final long columnId,
            @NonNull final String columnName,
            final int storeId,
            @NonNull final String storeUniqueName,
            final PlacementType placementType,
            final String physicalSchemaName,
            final String physicalTableName,
            final String physicalColumnName ) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.columnId = columnId;
        this.columnName = columnName;
        this.storeId = storeId;
        this.storeUniqueName = storeUniqueName;
        this.placementType = placementType;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physicalTableName;
        this.physicalColumnName = physicalColumnName;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ tableName, storeUniqueName, placementType.name(), physicalSchemaName, physicalTableName, physicalColumnName };
    }

    public static CatalogColumnPlacement replacePhysicalNames( CatalogColumnPlacement columnPlacement, String physicalSchemaName, String physicalTableName, String physicalColumnName ) {
        return new CatalogColumnPlacement( columnPlacement.tableId, columnPlacement.tableName, columnPlacement.columnId, columnPlacement.columnName, columnPlacement.storeId, columnPlacement.storeUniqueName, columnPlacement.placementType, physicalSchemaName, physicalTableName, physicalColumnName );
    }

}
