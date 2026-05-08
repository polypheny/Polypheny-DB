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
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableFilter;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.PolyType;

/**
 * Pushes filters over a Parquet nested join into the adapter join itself.
 */
public class ParquetEnumerableFilterJoinRule extends AlgOptRule {

    public static final ParquetEnumerableFilterJoinRule INSTANCE = new ParquetEnumerableFilterJoinRule( AlgFactories.LOGICAL_BUILDER );

    private final ParquetRelFilterTranslator translator = new ParquetRelFilterTranslator();


    public ParquetEnumerableFilterJoinRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( EnumerableFilter.class, operand( ParquetRelJoin.class, none() ) ),
                algBuilderFactory,
                ParquetEnumerableFilterJoinRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        EnumerableFilter filter = call.alg( 0 );
        ParquetRelJoin join = call.alg( 1 );
        List<PolyType> fieldTypes = join.getTupleType().getFields().stream()
                .map( field -> field.getType().getPolyType() )
                .toList();

        ParquetAdapterFilter adapterFilter = translator.translate( fieldTypes, filter.getCondition() );
        if ( adapterFilter == null ) {
            return;
        }

        call.transformTo( join.withFilters( List.of( adapterFilter ) ) );
    }

}
