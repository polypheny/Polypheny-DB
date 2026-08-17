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

package org.polypheny.db.adapter.parquet.shared.io;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;

/***
 * Contains columns metadata for exposing primitive nested field as separate column in flat mode
 * @param columns - what Polypheny should expose in the catalog
 * @param columnPaths - adapter-private metadata telling the scanner how to read the real value from the Parquet group
 */
public record ExportedSchema( List<ExportedColumn> columns, Map<String, List<String>> columnPaths ) {
    public ExportedSchema {
        columns = List.copyOf( columns );
        columnPaths = columnPaths.entrySet().stream()
                .collect( LinkedHashMap::new, ( map, entry ) -> map.put( entry.getKey(), List.copyOf( entry.getValue() ) ), LinkedHashMap::putAll );
    }

}
