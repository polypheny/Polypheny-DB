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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetMultiFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFileFilterReducer;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFilePartitionFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFileStatisticsFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ResidualFilters;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class ParquetFilterPruningTest {

    private static final ParquetColumnBinding REGION = new ParquetColumnBinding( 1L, "region", ParquetColumnRole.PARTITION, List.of() );
    private static final ParquetColumnBinding AMOUNT = new ParquetColumnBinding( 2L, "amount", ParquetColumnRole.DATA, List.of( "amount" ) );
    private static final ParquetColumnBinding METADATA = new ParquetColumnBinding( 3L, "metadata", ParquetColumnRole.DATA, List.of( "metadata", "amount" ) );


    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @Test
    void partitionEvaluatorMatchesValuesAndTreatsMissingPartitionsAsNull() {
        ParquetSourceFile sourceFile = sourceFile( Map.of( "region", "EU" ), Map.of() );
        ParquetSourceFilePartitionFilterEvaluator evaluator = new ParquetSourceFilePartitionFilterEvaluator( ParquetFilterPruningTest::binding );

        assertEquals( Boolean.TRUE, evaluator.evaluate( sourceFile, filter( 1, Kind.EQUALS, PolyString.of( "EU" ) ) ) );
        assertEquals( Boolean.FALSE, evaluator.evaluate( sourceFile, filter( 1, Kind.EQUALS, PolyString.of( "US" ) ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( sourceFile( Map.of(), Map.of() ), filter( 1, Kind.IS_NULL, null ) ) );
        assertNull( evaluator.evaluate( sourceFile, filter( 2, Kind.EQUALS, PolyDouble.of( 12D ) ) ) );
    }


    @Test
    void statisticsEvaluatorPrunesByRangeAndProvesConstantFiles() {
        ParquetColumnStatistics range = new ParquetColumnStatistics( PolyType.DOUBLE, 10, 10, 0L, "10.0", "20.0", true );
        ParquetColumnStatistics constant = new ParquetColumnStatistics( PolyType.DOUBLE, 4, 4, 0L, "12.0", "12.0", true );
        ParquetColumnStatistics allNull = new ParquetColumnStatistics( PolyType.DOUBLE, 3, 3, 3L, null, null, true );
        ParquetSourceFile rangeFile = sourceFile( Map.of(), Map.of( List.of( "amount" ), range ) );
        ParquetSourceFile constantFile = sourceFile( Map.of(), Map.of( List.of( "amount" ), constant ) );
        ParquetSourceFile nullFile = sourceFile( Map.of(), Map.of( List.of( "amount" ), allNull ) );
        ParquetSourceFileStatisticsFilterEvaluator evaluator = new ParquetSourceFileStatisticsFilterEvaluator( ParquetFilterPruningTest::binding );

        assertEquals( Boolean.FALSE, evaluator.evaluate( rangeFile, filter( 2, Kind.EQUALS, PolyDouble.of( 30D ) ) ) );
        assertNull( evaluator.evaluate( rangeFile, filter( 2, Kind.EQUALS, PolyDouble.of( 15D ) ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( rangeFile, filter( 2, Kind.GREATER_THAN, PolyDouble.of( 5D ) ) ) );
        assertFalse( evaluator.supportsExactEvaluation( rangeFile, filter( 2, Kind.EQUALS, PolyDouble.of( 15D ) ) ) );

        assertEquals( Boolean.TRUE, evaluator.evaluate( constantFile, filter( 2, Kind.EQUALS, PolyDouble.of( 12D ) ) ) );
        assertTrue( evaluator.supportsExactEvaluation( constantFile, filter( 2, Kind.EQUALS, PolyDouble.of( 12D ) ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( nullFile, filter( 2, Kind.IS_NULL, null ) ) );
        assertEquals( Boolean.FALSE, evaluator.evaluate( nullFile, filter( 2, Kind.IS_NOT_NULL, null ) ) );
        assertTrue( evaluator.supportsExactEvaluation( nullFile, filter( 2, Kind.IS_NULL, null ) ) );

        assertNull( evaluator.evaluate( rangeFile, filter( 3, Kind.EQUALS, PolyDouble.of( 12D ) ) ) );
    }


    @Test
    void reducerDropsExactMatchesRejectsExactFailuresAndKeepsResiduals() {
        ParquetSourceFile sourceFile = sourceFile(
                Map.of( "region", "EU" ),
                Map.of( List.of( "amount" ), new ParquetColumnStatistics( PolyType.DOUBLE, 10, 10, 0L, "10.0", "20.0", true ) ) );
        ParquetMultiFilterEvaluator<ParquetSourceFile> evaluator = new ParquetMultiFilterEvaluator<>(
                new ParquetSourceFilePartitionFilterEvaluator( ParquetFilterPruningTest::binding ),
                new ParquetSourceFileStatisticsFilterEvaluator( ParquetFilterPruningTest::binding ) );
        ParquetAdapterFilter<PolyValue> matchingPartition = filter( 1, Kind.EQUALS, PolyString.of( "EU" ) );
        ParquetAdapterFilter<PolyValue> rejectingPartition = filter( 1, Kind.EQUALS, PolyString.of( "US" ) );
        ParquetAdapterFilter<PolyValue> unknownRange = filter( 2, Kind.EQUALS, PolyDouble.of( 15D ) );

        ResidualFilters reduced = ParquetSourceFileFilterReducer.reduce( sourceFile, evaluator, List.of( matchingPartition, unknownRange ) );
        assertTrue( reduced.matches() );
        assertEquals( List.of( unknownRange ), reduced.filters() );

        ResidualFilters rejected = ParquetSourceFileFilterReducer.reduce( sourceFile, evaluator, List.of( matchingPartition, rejectingPartition ) );
        assertFalse( rejected.matches() );
        assertTrue( rejected.filters().isEmpty() );

        ResidualFilters partialOr = ParquetSourceFileFilterReducer.reduce(
                sourceFile,
                evaluator,
                List.of( ParquetAdapterFilter.logical( Kind.OR, List.of( rejectingPartition, unknownRange ) ) ) );
        assertTrue( partialOr.matches() );
        assertEquals( List.of( unknownRange ), partialOr.filters() );

        ResidualFilters residualNot = ParquetSourceFileFilterReducer.reduce(
                sourceFile,
                evaluator,
                List.of( ParquetAdapterFilter.logical( Kind.NOT, List.of( unknownRange ) ) ) );
        assertTrue( residualNot.matches() );
        assertEquals( Kind.NOT, residualNot.filters().get( 0 ).operator() );
        assertEquals( unknownRange, residualNot.filters().get( 0 ).operands().get( 0 ) );
    }


    @Test
    void filterResolverMapsProjectionIndexesAndResolvesDynamicParameters() {
        ParquetAdapterFilter<PolyValue> projected = new ParquetAdapterFilter<>( 1, List.of( "old" ), Kind.EQUALS, null, 7L );
        ParquetAdapterFilter<PolyValue> physical = ParquetFilterResolver.toPhysicalFilter( projected, new int[]{ 2, 4 } );
        ParquetAdapterFilter<PolyValue> projection = ParquetFilterResolver.toProjectionFilter(
                new ParquetAdapterFilter<>( 4, List.of( "amount" ), Kind.GREATER_THAN, PolyDouble.of( 10D ) ),
                index -> index == 4 ? 1 : -1 );
        DataContext dataContext = new ParameterDataContext( Map.of( 7L, PolyString.of( "EU" ) ) );

        assertEquals( 4, Objects.requireNonNull( physical ).columnIndex() );
        assertEquals( List.of( "old" ), physical.pathElements() );
        assertNull( ParquetFilterResolver.toPhysicalFilter( new ParquetAdapterFilter<>( 2, Kind.EQUALS, PolyString.of( "EU" ) ), new int[]{ 2, 4 } ) );
        assertEquals( 1, Objects.requireNonNull( projection ).columnIndex() );
        assertTrue( projection.pathElements().isEmpty() );

        List<ParquetAdapterFilter<PolyValue>> resolved = ParquetFilterResolver.resolveFilters( dataContext, List.of( projected ), ignored -> AMOUNT );
        assertEquals( List.of( "amount" ), resolved.get( 0 ).pathElements() );
        assertEquals( "EU", resolved.get( 0 ).value().asString().value );
    }


    private static ParquetColumnBinding binding( ParquetAdapterFilter<PolyValue> filter ) {
        return switch ( filter.columnIndex() ) {
            case 1 -> REGION;
            case 2 -> AMOUNT;
            case 3 -> METADATA;
            default -> null;
        };
    }


    private static ParquetAdapterFilter<PolyValue> filter( int columnIndex, Kind kind, PolyValue value ) {
        return new ParquetAdapterFilter<>( columnIndex, kind, value );
    }


    private static ParquetSourceFile sourceFile( Map<String, String> partitions, Map<List<String>, ParquetColumnStatistics> statistics ) {
        return new ParquetSourceFile( "file:/tmp/source.parquet", partitions, statistics );
    }


    private static class ParameterDataContext extends DataContext.SlimDataContext {

        private final List<Map<Long, PolyValue>> values;


        private ParameterDataContext( Map<Long, PolyValue> values ) {
            this.values = List.of( new LinkedHashMap<>( values ) );
        }


        @Override
        public List<Map<Long, PolyValue>> getParameterValues() {
            return values;
        }


    }

}
