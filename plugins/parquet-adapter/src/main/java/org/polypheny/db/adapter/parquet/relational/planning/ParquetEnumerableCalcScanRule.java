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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.List;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyType;

/**
 * Converts logical calcs over Parquet scans into filtered adapter scans while
 * preserving the calc projection.
 */
public class ParquetEnumerableCalcScanRule extends ConverterRule {

    public static final ParquetEnumerableCalcScanRule INSTANCE = new ParquetEnumerableCalcScanRule( AlgFactories.LOGICAL_BUILDER );

    private final ParquetRelFilterTranslator translator = new ParquetRelFilterTranslator();


    public ParquetEnumerableCalcScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                LogicalCalc.class,
                calc -> true,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                ParquetEnumerableCalcScanRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalCalc calc = (LogicalCalc) alg;
        RexProgram program = calc.getProgram();
        if ( program.getCondition() == null ) {
            return null;
        }

        ParquetRelScan scan = ParquetRelScanRuleSupport.findProjectedRelScan( calc.getInput() );
        if ( scan == null ) {
            scan = ParquetRelScanRuleSupport.findProjectedRelScan( convert( calc.getInput(), calc.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) ) );
        }
        if ( scan == null ) {
            return null;
        }

        RexNode condition = program.expandLocalRef( program.getCondition() );
        List<PolyType> fieldTypes = ParquetRelScanRuleSupport.fieldTypes( scan );
        ParquetAdapterFilter adapterFilter = translator.translate( fieldTypes, condition );
        if ( adapterFilter == null ) {
            return null;
        }

        ParquetRelScan filteredScan = scan.withFilters( List.of( adapterFilter ) );
        List<RexNode> projects = program.getProjectList().stream()
                .map( program::expandLocalRef )
                .toList();

        RexProgram projectionOnly = RexProgram.create(
                scan.getTupleType(),
                projects,
                null,
                calc.getTupleType(),
                calc.getCluster().getRexBuilder() );

        return EnumerableCalc.create( filteredScan, projectionOnly );
    }


}
