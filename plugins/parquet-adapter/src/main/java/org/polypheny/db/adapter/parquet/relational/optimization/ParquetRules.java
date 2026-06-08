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

package org.polypheny.db.adapter.parquet.relational.optimization;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.parquet.relational.planning.EnumerableParquet;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetConvention;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * Rules that build the Parquet adapter convention path.
 */
public final class ParquetRules {

    private static final boolean isOptimizeAggregation = true;


    private ParquetRules() {
    }


    public static List<AlgOptRule> rules( ParquetConvention out ) {
        return rules( out, AlgFactories.LOGICAL_BUILDER );
    }


    public static List<AlgOptRule> rules( ParquetConvention out, AlgBuilderFactory factory ) {
        List<AlgOptRule> rules = new ArrayList<>();
        rules.add( new ParquetAlgOptRule( PatternMatchers.joinWithScanOnLeftAndScanOnRight( out, factory ) ) );
        rules.add( new ParquetAlgOptRule( PatternMatchers.attachFilterToJoinUnderCalc( out, factory ) ) );
        rules.add( new ParquetAlgOptRule( PatternMatchers.attachFieldsAndFiltersToScanUnderCalc( out, factory ) ) );
        rules.add( new EnumerableParquetRule( out, factory ) );
        if ( isOptimizeAggregation ) {
            rules.add( new ParquetAlgOptRule( PatternMatchers.aggregateOnScan( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( PatternMatchers.aggregateOnCalcScan( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( PatternMatchers.partialAggregateOnUnion( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( PatternMatchers.partialAggregateOnCalcUnion( out, factory ) ) );
        }
        return rules;
    }


    /**
     * Rule that converts a plan from Parquet convention into Enumerable convention
     */
    public static class EnumerableParquetRule extends ConverterRule {

        public EnumerableParquetRule( ParquetConvention in, AlgBuilderFactory algBuilderFactory ) {
            super(
                    AlgNode.class,
                    alg -> true,
                    in,
                    EnumerableConvention.INSTANCE,
                    algBuilderFactory,
                    "EnumerableParquetRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            return new EnumerableParquet( alg.getCluster(), alg.getTraitSet().replace( getOutTrait() ).replace( ModelTrait.RELATIONAL ), alg );
        }

    }

}
