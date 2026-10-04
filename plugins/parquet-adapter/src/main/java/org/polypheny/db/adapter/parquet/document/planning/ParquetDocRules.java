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
import org.polypheny.db.adapter.parquet.relational.planning.EnumerableParquet;
import org.polypheny.db.adapter.parquet.shared.optimization.ParquetAggregatePatternMatchers;
import org.polypheny.db.adapter.parquet.shared.optimization.ParquetAlgOptRule;
import org.polypheny.db.adapter.parquet.shared.optimization.ParquetOptimizationSettings;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilderFactory;

/**
 * Rules that build the Parquet adapter document convention path.
 */
public final class ParquetDocRules {

    private ParquetDocRules() {
    }


    public static List<AlgOptRule> rules( ParquetDocConvention out ) {
        return rules( out, AlgFactories.LOGICAL_BUILDER );
    }


    public static List<AlgOptRule> rules(ParquetDocConvention out, AlgBuilderFactory factory ) {
        List<AlgOptRule> rules = new ArrayList<>();
        rules.add( new ParquetAlgOptRule( ParquetDocPatternMatchers.attachFiltersToScanUnderCalc( out, factory ) ) );
        rules.add( new EnumerableParquetDocumentRule( out, factory ) );
        if ( ParquetOptimizationSettings.isOptimizeAggregation() ) {
            rules.add( new ParquetAlgOptRule( ParquetDocPatternMatchers.aggregateOnScan( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( ParquetDocPatternMatchers.aggregateOnProjectScan( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( ParquetDocPatternMatchers.aggregateOnCalcScan( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( ParquetAggregatePatternMatchers.partialAggregateOnUnion( out, factory ) ) );
            rules.add( new ParquetAlgOptRule( ParquetAggregatePatternMatchers.partialAggregateOnCalcUnion( out, factory ) ) );
        }
        return rules;
    }


    /**
     * Rule that converts a plan from Parquet document convention into Enumerable convention.
     */
    public static class EnumerableParquetDocumentRule extends ConverterRule {

        public EnumerableParquetDocumentRule(ParquetDocConvention in, AlgBuilderFactory algBuilderFactory ) {
            super(
                    AlgNode.class,
                    alg -> true,
                    in,
                    EnumerableConvention.INSTANCE,
                    algBuilderFactory,
                    "EnumerableParquetDocumentRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            return new EnumerableParquet( alg.getCluster(), alg.getTraitSet().replace( getOutTrait() ).replace( ModelTrait.DOCUMENT ), alg );
        }

    }

}
