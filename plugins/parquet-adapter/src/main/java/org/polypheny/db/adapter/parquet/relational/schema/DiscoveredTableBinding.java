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
import java.util.Map;

/**
 *  metadata that connects a discovered logical table back to the real Parquet data
 *
 * @param sourceFiles - real Parquet files
 * @param parentTableName - generated parent table
 * @param sourcePathElements - table-level Parquet path
 * @param columnPaths - column-level Parquet path
 */
public record DiscoveredTableBinding(
        List<ParquetSourceFile> sourceFiles,
        String parentTableName,
        List<String> sourcePathElements,
        Map<String, List<String>> columnPaths ) {

}
