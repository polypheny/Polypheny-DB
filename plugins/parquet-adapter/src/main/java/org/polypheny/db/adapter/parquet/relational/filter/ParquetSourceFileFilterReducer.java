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

package org.polypheny.db.adapter.parquet.relational.filter;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Evaluates row filters using values known for a complete source file. If filter is applied on a column containing single value it can be removed from further usage.
 */
public final class ParquetSourceFileFilterReducer {

    private ParquetSourceFileFilterReducer() {
    }


    /**
     * Evaluates and reduces filters using file-level metadata. Residual filters still need to be evaluated while reading rows.
     *
     * @param sourceFile a source file.
     * @param evaluator a file-level evaluator.
     * @param filters filters to evaluate and reduce if possible.
     * @return whether the file matches and any residual filters.
     */
    public static ResidualFilters reduce( ParquetSourceFile sourceFile, ParquetMultiFilterEvaluator<ParquetSourceFile> evaluator, List<ParquetAdapterFilter<PolyValue>> filters ) {
        List<ParquetAdapterFilter<PolyValue>> residuals = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            EvaluatedFilter simplified = evaluateAndReduce( sourceFile, evaluator, filter );
            if ( simplified.isExactRejection() ) {
                return new ResidualFilters( false, List.of() );
            }
            if ( simplified.residual() != null ) {
                residuals.add( simplified.residual() );
            }
        }
        return new ResidualFilters( true, residuals );
    }


    private static EvaluatedFilter evaluateAndReduce( ParquetSourceFile sourceFile, ParquetMultiFilterEvaluator<ParquetSourceFile> evaluator, ParquetAdapterFilter<PolyValue> filter ) {
        if ( !filter.isLogical() ) {
            Boolean exactMatch = evaluator.evaluate( sourceFile, filter );
            return exactMatch == null ? EvaluatedFilter.residual( filter ) : EvaluatedFilter.exact( exactMatch );
        }
        if ( filter.operator() == Kind.NOT ) {
            if ( filter.operands().size() != 1 ) {
                return EvaluatedFilter.residual( filter );
            }
            EvaluatedFilter evaluatedFilter = evaluateAndReduce( sourceFile, evaluator, filter.operands().get( 0 ) );
            return evaluatedFilter.exactMatch() == null
                    ? EvaluatedFilter.residual( ParquetAdapterFilter.logical( Kind.NOT, List.of( evaluatedFilter.residual() ) ) )
                    : EvaluatedFilter.exact( !evaluatedFilter.exactMatch() );
        }

        List<ParquetAdapterFilter<PolyValue>> residuals = new ArrayList<>( filter.operands().size() );
        for ( ParquetAdapterFilter<PolyValue> operand : filter.operands() ) {
            EvaluatedFilter simplified = evaluateAndReduce( sourceFile, evaluator, operand );
            if ( (filter.operator() == Kind.AND && simplified.isExactRejection()) || (filter.operator() == Kind.OR && simplified.isExactMatch()) ) {
                return EvaluatedFilter.exact( filter.operator() == Kind.OR );
            }
            if ( simplified.residual() != null ) {
                residuals.add( simplified.residual() );
            }
        }
        if ( residuals.isEmpty() ) {
            return EvaluatedFilter.exact( filter.operator() == Kind.AND );
        }
        if ( residuals.size() == 1 ) {
            return EvaluatedFilter.residual( residuals.get( 0 ) );
        }
        return EvaluatedFilter.residual( ParquetAdapterFilter.logical( filter.operator(), residuals ) );
    }


    /**
     * Contains the result of the evaluation process.
     *
     * @param exactMatch indicates if the evaluation found an exact match.
     * @param residual if not null then this filter needs to be used in row level evaluation.
     */
    private record EvaluatedFilter( Boolean exactMatch, ParquetAdapterFilter<PolyValue> residual ) {

        private static EvaluatedFilter exact( boolean match ) {
            return new EvaluatedFilter( match, null );
        }


        private static EvaluatedFilter residual( ParquetAdapterFilter<PolyValue> filter ) {
            return new EvaluatedFilter( null, filter );
        }


        private boolean isExactMatch() {
            return Boolean.TRUE.equals( exactMatch );
        }


        private boolean isExactRejection() {
            return Boolean.FALSE.equals( exactMatch );
        }

    }

}
