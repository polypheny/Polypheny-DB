/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable;


import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.core.AggregateCall;


/**
 * Information for a call to {@link AggImplementor#implementResult(AggContext, AggResultContext)}
 *
 * Typically, the aggregation implementation will convert {@link #accumulator()} to the resulting value of the aggregation.
 * The implementation MUST NOT destroy the contents of {@link #accumulator()}.
 */
public interface AggResultContext extends NestedBlockBuilder, AggResetContext {

    /**
     * Expression by which to reference the key upon which the values in the accumulator were aggregated. Most aggregate functions depend on only the accumulator,
     * but quasi-aggregate functions such as GROUPING access at the key.
     */
    Expression key();

    /**
     * Returns an expression that references the {@code i}th field of the key, cast to the appropriate type.
     */
    Expression keyField( int i );

    AggregateCall call();

}

