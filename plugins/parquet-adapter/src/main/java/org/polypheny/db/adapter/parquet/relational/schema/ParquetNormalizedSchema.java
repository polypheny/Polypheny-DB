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

import lombok.Getter;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of normalization
 * contains normalized tables information
 */
@Getter
public class ParquetNormalizedSchema {

    private final Map<String, List<ExportedColumn>> tables;
    private final Map<String, DiscoveredTableBinding> bindings;


    public ParquetNormalizedSchema() {
        tables = new HashMap<>();
        bindings = new HashMap<>();
    }


    public void addColumns( String tableName, List<ExportedColumn> columns ) {
        tables.computeIfAbsent( tableName, k -> new ArrayList<>() ).addAll( columns );
    }


    public void addBinding( String tableName, DiscoveredTableBinding binding ) {
        bindings.put( tableName, binding );
    }


    public DiscoveredTableBinding getBinding( String tableName ) {
        return bindings.get( tableName );
    }


    /**
     * Avoids duplicate generated table names:
     * If parquetrelational1__orders__items exists it will generate parquetrelational1__orders__items_2
     *
     * @param baseName - generated table name
     * @return unique table name
     */
    public String uniqueTableName( String baseName ) {
        String name = baseName;
        int suffix = 2;
        while ( tables.containsKey( name ) ) {
            name = baseName + "_" + suffix++;
        }
        return name;
    }

}
