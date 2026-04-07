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

package org.polypheny.db.adapter.parquet.document.planning;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocFilterTranslator;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.adapter.parquet.shared.model.AdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * Planner rule that recognizes supported document filters
 * and rewrites them into `ParquetDocFilter` plus `ParquetDocScan`
 */
public class ParquetDocFilterRule extends AlgOptRule {

    private final ParquetDocFilterTranslator translator = new ParquetDocFilterTranslator();
    private final ParquetDocument document;


    public ParquetDocFilterRule( AlgBuilderFactory algBuilderFactory, ParquetDocument document ) {
        super( operand( LogicalRelFilter.class, any() ), algBuilderFactory, ParquetDocFilterRule.class.getSimpleName() );
        this.document = document;
    }


    /**
     * On match read the matched node as LogicalRelFilter filter:
     * - split received filter information into predicates
     * - translate them into AdapterFilters
     * - produce a ParquetDocScan with those filters
     * - wrap it in ParquetDocFilter
     * @param call Rule call - passed in by query optimizer when this rule matches, re-written
     */
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalRelFilter filter = call.alg( 0 );

        // column list is needed because filter translation
        // is done against actual exported field names and physical positions
        List<ExportedColumn> columns = document.getParquetSource().getExportedColumns().get( document.name );
        // separate predicates - the rule validates each predicate individually
        List<RexNode> predicates = splitConjunctions( filter.getCondition() );
        if ( columns == null || predicates.isEmpty() ) {
            return;
        }

        List<AdapterFilter> adapterFilters = new ArrayList<>();
        for ( RexNode predicate : predicates ) {
            AdapterFilter adapterFilter = translator.translate( columns, predicate );
            if ( adapterFilter == null ) {
                return;
            }
            adapterFilters.add( adapterFilter );
        }

        // replaces the generic logical filter with a new ParquetDocScan
        // carrying the translated adapterFilter wrapped in a ParquetDocFilter
        call.transformTo(
                new ParquetDocFilter(
                        filter.getCluster(),
                        filter.getTraitSet().replace( EnumerableConvention.INSTANCE ),
                        new ParquetDocScan( filter.getCluster(), document, adapterFilters ),
                        filter.getCondition(),
                        document ) );
    }


    private List<RexNode> splitConjunctions( RexNode node ) {
        if ( node.getKind() != Kind.AND ) {
            return List.of( node );
        }
        List<RexNode> predicates = new ArrayList<>();
        RexCall call = (RexCall) node;
        for ( RexNode operand : call.getOperands() ) {
            predicates.addAll( splitConjunctions( operand ) );
        }
        return predicates;
    }

}
