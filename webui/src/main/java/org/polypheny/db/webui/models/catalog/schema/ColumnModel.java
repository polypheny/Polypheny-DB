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

package org.polypheny.db.webui.models.catalog.schema;

import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.type.PolyType;

public class ColumnModel extends FieldModel {

    public final PolyType type;
    public final PolyType collectionsType;
    public final Integer precision;
    public final Integer scale;
    public final String defaultValue;
    public final Integer dimension;
    public final Integer cardinality;
    public final boolean nullable;
    public final int position;


    public ColumnModel(
            @Nullable Long id,
            @Nullable String name,
            long tableId,
            PolyType type,
            PolyType collectionsType,
            Integer precision,
            Integer scale,
            String defaultValue,
            Integer dimension,
            Integer cardinality,
            boolean nullable,
            int position ) {
        super( id, name, tableId );
        this.type = type;
        this.nullable = nullable;
        this.position = position;
        this.collectionsType = collectionsType;
        this.precision = precision;
        this.scale = scale;
        this.defaultValue = defaultValue;
        this.dimension = dimension;
        this.cardinality = cardinality;
    }


    public static ColumnModel from( LogicalColumn column ) {
        return new ColumnModel( column.id, column.name, column.tableId, column.type, column.collectionsType, column.length, column.scale, column.defaultValue == null ? null : column.defaultValue.value, column.dimension, column.cardinality, column.nullable, column.position );
    }

}
