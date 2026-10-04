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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocRules;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocConvention;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelRules;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelConvention;
import org.polypheny.db.adapter.parquet.shared.optimization.ParquetOptimizationSettings;
import org.polypheny.db.adapter.parquet.shared.optimization.aggregate.AggregateDecomposition;
import org.polypheny.db.adapter.parquet.shared.optimization.aggregate.PartialAggregate;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.type.PolyType;

class ParquetAggregateOptimizationTest {

    @Test
    void aggregateDecompositionExposesPartialCallsAndFinalFunctions() {
        AggFunction countFunction = aggregateFunction( Kind.COUNT, "COUNT" );
        AggFunction sumFunction = aggregateFunction( Kind.SUM, "SUM0" );
        AggFunction maxFunction = aggregateFunction( Kind.MAX, "MAX" );
        AggregateCall count = aggregateCall( countFunction, "c" );
        AggregateCall max = aggregateCall( maxFunction, "m" );
        PartialAggregate partialCount = new PartialAggregate( count, sumFunction );
        PartialAggregate partialMax = new PartialAggregate( max, maxFunction );

        AggregateDecomposition decomposition = new AggregateDecomposition( List.of( partialCount, partialMax ) );

        assertEquals( List.of( count, max ), decomposition.partialCalls() );
        assertSame( count, partialCount.partialCall() );
        assertSame( sumFunction, partialCount.finalFunction() );
        assertSame( maxFunction, partialMax.finalFunction() );
    }


    @Test
    void aggregateOptimizationRulesCanBeDisabledWithSystemProperty() {
        String previous = System.getProperty( ParquetOptimizationSettings.OPTIMIZE_AGGREGATION_PROPERTY );
        try {
            System.setProperty( ParquetOptimizationSettings.OPTIMIZE_AGGREGATION_PROPERTY, "false" );

            List<String> relationalRules = ParquetRelRules.rules( ParquetRelConvention.INSTANCE ).stream()
                    .map( Object::toString )
                    .toList();
            List<String> documentRules = ParquetDocRules.rules( ParquetDocConvention.INSTANCE ).stream()
                    .map( Object::toString )
                    .toList();

            assertFalse( containsRule( relationalRules, "aggregateOnScan" ) );
            assertFalse( containsRule( relationalRules, "aggregateOnCalcScan" ) );
            assertFalse( containsRule( relationalRules, "partialAggregateOnUnion" ) );
            assertFalse( containsRule( relationalRules, "partialAggregateOnCalcUnion" ) );
            assertFalse( containsRule( documentRules, "documentAggregateOnScan" ) );
            assertFalse( containsRule( documentRules, "documentAggregateOnProjectScan" ) );
            assertFalse( containsRule( documentRules, "documentAggregateOnCalcScan" ) );
            assertFalse( containsRule( documentRules, "partialAggregateOnUnion" ) );
            assertFalse( containsRule( documentRules, "partialAggregateOnCalcUnion" ) );

            assertTrue( containsRule( relationalRules, "EnumerableParquetRule" ) );
            assertTrue( containsRule( documentRules, "EnumerableParquetDocumentRule" ) );
        } finally {
            if ( previous == null ) {
                System.clearProperty( ParquetOptimizationSettings.OPTIMIZE_AGGREGATION_PROPERTY );
            } else {
                System.setProperty( ParquetOptimizationSettings.OPTIMIZE_AGGREGATION_PROPERTY, previous );
            }
        }
    }


    private static boolean containsRule( List<String> rules, String name ) {
        return rules.stream().anyMatch( rule -> rule.contains( name ) );
    }


    private static AggregateCall aggregateCall( AggFunction function, String name ) {
        return AggregateCall.create(
                function,
                false,
                false,
                List.of( 0 ),
                -1,
                AlgCollations.EMPTY,
                AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.BIGINT ),
                name );
    }


    private static AggFunction aggregateFunction( Kind kind, String name ) {
        return (AggFunction) Proxy.newProxyInstance(
                ParquetAggregateOptimizationTest.class.getClassLoader(),
                new Class<?>[]{ AggFunction.class },
                ( proxy, method, args ) -> switch ( method.getName() ) {
                    case "getKind" -> kind;
                    case "getName", "toString", "getAllowedSignatures" -> name;
                    case "isAggregator", "isQuantifierAllowed", "allowsFilter" -> true;
                    case "hashCode" -> System.identityHashCode( proxy );
                    case "equals" -> proxy == args[0];
                    default -> null;
                } );
    }

}
