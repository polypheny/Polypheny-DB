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

package org.polypheny.db.adapter.parquet.relational.execution.aggregate;

import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Maps an input row into the grouping and aggregate values consumed by the generic row aggregate engine.
 */
interface AggregateRowProjector {

    /**
     * Evaluates condition expression if there is any.
     *
     * @param row the row to check.
     * @return true if the condition is missing or evaluates to true and false otherwise.
     */
    boolean accepts( PolyValue[] row );

    /**
     * Creates a {@link GroupKey} used for aggregation.
     *
     * @param row a row.
     * @return a {@link GroupKey}.
     */
    GroupKey groupKey( PolyValue[] row );

    /**
     * Adds values to a shared aggregate state.
     *
     * @param values an aggregate state to update.
     * @param row a row to read aggregate values from.
     */
    void add( AggregateGroupState values, PolyValue[] row );

}
