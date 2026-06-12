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
import java.util.HashSet;
import java.util.List;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Pushes supported document Calc filters into a Parquet document scan.
 */
public class ParquetDocCalcRule extends AlgOptRule {

    private final ParquetDocFilterTranslator translator = new ParquetDocFilterTranslator();


    public ParquetDocCalcRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( EnumerableCalc.class, operand( ParquetDocScan.class, none() ) ), algBuilderFactory, ParquetDocCalcRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        EnumerableCalc calc = call.alg( 0 );
        ParquetDocScan scan = call.alg( 1 );
        RexProgram program = calc.getProgram();
        if ( program.getCondition() == null ) {
            return;
        }

        List<ExportedColumn> columns = scan.getEntity().getParquetSource().getExportedColumns().get( scan.getEntity().name );
        if ( columns == null ) {
            return;
        }

        List<RexNode> predicates = new ArrayList<>();
        AlgOptUtil.decomposeConjunction( program.expandLocalRef( program.getCondition() ), predicates );
        if ( predicates.isEmpty() ) {
            return;
        }

        List<ParquetAdapterFilter<PolyValue>> filters = new ArrayList<>();
        for ( RexNode predicate : predicates ) {
            ParquetAdapterFilter<PolyValue> filter = translator.translate( columns, predicate );
            if ( filter == null ) {
                return;
            }
            filters.add( filter );
        }

        if ( new HashSet<>( scan.getFilters() ).containsAll( filters ) ) {
            return;
        }

        call.transformTo( EnumerableCalc.create( scan.withFilters( filters ), program ) );
    }

}
