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

package org.polypheny.db.adapter.parquet.shared.aggregate;


/**
 * Aggregated values for a single primitive numeric column.
 *
 * @param count number of present values.
 * @param sum sum of present values.
 * @param min minimum present value.
 * @param max maximum present value.
 */
public record ColumnAggregateResult(long count, double sum, double min, double max ) {

    public static ColumnAggregateResult empty() {
        return new ColumnAggregateResult( 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY );
    }


    public ColumnAggregateResult merge( ColumnAggregateResult other ) {
        if ( count == 0 ) {
            return other;
        }
        if ( other.count == 0 ) {
            return this;
        }
        return new ColumnAggregateResult( count + other.count, sum + other.sum, Math.min( min, other.min ), Math.max( max, other.max ) );
    }

}
