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

import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import java.util.List;

/**
 * Represents one logical Polypheny table found during Parquet discovery, before the table is fully registered in Polypheny.
 * @param tableName - final discovered table name, prefixed with adapter name
 * @param columns - columns Polypheny should expose for this table
 * @param binding - metadata that explains how this logical table maps back to physical Parquet data
 */
public record DiscoveredTable(String tableName, List<ExportedColumn> columns, DiscoveredTableBinding binding ) {
    public DiscoveredTable {
        columns = columns == null ? List.of() : List.copyOf( columns );
    }
}
