/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.util.List;

/**
 * Immutable data holder
 * Metadata object that tells the Parquet scanner:
 * for this Polypheny column, where does its value come from
 * @param columnId - Polypheny physical column id connected to an actual column in Polypheny’s physical table catalog
 * @param columnName - relational column name
 * @param role - ParquetColumnRole - what kind of column this is
 * @param sourcePathElements - the path inside the Parquet schema that should be used to read this column.
 * flat/root column - path: order_id
 * nested - path: shipping_address.city
 */
public record ParquetColumnBinding(
        long columnId,
        String columnName,
        ParquetColumnRole role,
        List<String> sourcePathElements ) {

    public ParquetColumnBinding {
        sourcePathElements = sourcePathElements == null ? List.of() : List.copyOf( sourcePathElements );
    }

}
