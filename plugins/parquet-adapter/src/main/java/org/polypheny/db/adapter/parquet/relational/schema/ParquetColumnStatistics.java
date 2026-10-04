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

import org.polypheny.db.type.PolyType;


/**
 * Compact per-file statistics for one physical Parquet column path.
 * Provides statistics for file pruning
 */
public record ParquetColumnStatistics(
        PolyType type,
        long rowCount,
        long valueCount,
        Long nullCount,
        String min,
        String max,
        boolean minMaxReliable ) {

    public boolean hasNoNulls() {
        return nullCount != null && nullCount == 0;
    }


    public boolean hasOnlyNulls() {
        return nullCount != null && nullCount >= valueCount;
    }


    public boolean hasRange() {
        return minMaxReliable && min != null && max != null;
    }

}
