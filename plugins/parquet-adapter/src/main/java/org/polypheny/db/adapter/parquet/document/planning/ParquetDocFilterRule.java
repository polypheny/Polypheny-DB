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

public class ParquetDocFilterRule extends AlgOptRule {

    private final ParquetDocFilterTranslator translator = new ParquetDocFilterTranslator();
    private final ParquetDocument document;


    public ParquetDocFilterRule( AlgBuilderFactory algBuilderFactory, ParquetDocument document ) {
        super( operand( LogicalRelFilter.class, any() ), algBuilderFactory, ParquetDocFilterRule.class.getSimpleName() );
        this.document = document;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalRelFilter filter = call.alg( 0 );

        List<ExportedColumn> columns = document.getParquetSource().getExportedColumns().get( document.name );
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
