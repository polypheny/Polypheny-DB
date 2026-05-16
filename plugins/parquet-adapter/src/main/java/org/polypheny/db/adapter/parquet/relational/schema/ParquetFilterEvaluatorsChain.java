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
import org.polypheny.db.adapter.parquet.shared.filter.FilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;


/**
 * Applies file-level filter evaluators as one pruning decision.
 */
public class ParquetFilterEvaluatorsChain<T> extends FilterEvaluator<T> {

    private final List<FilterEvaluator<T>> evaluators;


    public ParquetFilterEvaluatorsChain( List<FilterEvaluator<T>> evaluators ) {
        this.evaluators = evaluators == null ? List.of() : List.copyOf( evaluators );
    }


    public static <T> ParquetFilterEvaluatorsChain<T> empty() {
        return new ParquetFilterEvaluatorsChain<>( List.of() );
    }


    public boolean matches( T context, List<ParquetAdapterFilter> filters ) {
        return filters == null || filters.isEmpty() || filters.stream().allMatch( filter -> matches( context, filter ) );
    }


    @Override
    protected Boolean evaluateLeaf( T context, ParquetAdapterFilter filter ) {
        boolean hasKnownMatch = false;
        for ( FilterEvaluator<T> evaluator : evaluators ) {
            Boolean result = evaluator.evaluate( context, filter );
            if ( Boolean.FALSE.equals( result ) ) {
                return false;
            }
            hasKnownMatch |= Boolean.TRUE.equals( result );
        }
        return hasKnownMatch ? true : null;
    }

}
