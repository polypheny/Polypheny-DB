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
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyType;

/**
 * Converts logical filters over Parquet scans into filtered adapter scans.
 */
public class ParquetEnumerableFilterScanRule extends ConverterRule {

    public static final ParquetEnumerableFilterScanRule INSTANCE = new ParquetEnumerableFilterScanRule( AlgFactories.LOGICAL_BUILDER );

    private final ParquetRelFilterTranslator translator = new ParquetRelFilterTranslator();


    public ParquetEnumerableFilterScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                LogicalRelFilter.class,
                filter -> true,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                ParquetEnumerableFilterScanRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelFilter filter = (LogicalRelFilter) alg;
        ParquetRelScan scan = ParquetRelScanRuleSupport.findDirectRelScan( filter.getInput() );
        if ( scan == null ) {
            scan = ParquetRelScanRuleSupport.findDirectRelScan( convert( filter.getInput(), filter.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) ) );
        }
        if ( scan == null ) {
            return null;
        }

        List<PolyType> fieldTypes = ParquetRelScanRuleSupport.fieldTypes( scan );
        ParquetAdapterFilter adapterFilter = translator.translate( fieldTypes, filter.getCondition() );
        if ( adapterFilter == null ) {
            return null;
        }

        return scan.withFilters( List.of( adapterFilter ) );
    }


}
