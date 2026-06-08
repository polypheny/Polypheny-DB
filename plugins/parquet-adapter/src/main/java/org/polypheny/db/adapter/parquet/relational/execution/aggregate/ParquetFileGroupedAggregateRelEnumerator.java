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

package org.polypheny.db.adapter.parquet.relational.execution.aggregate;

import static org.polypheny.db.adapter.parquet.relational.execution.ParquetRelExecutor.selectPhysicalBinding;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelExecutor;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetMultiFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetConstantColumnResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.ColumnAggregateProjection;
import org.polypheny.db.adapter.parquet.shared.aggregate.ColumnAggregateResult;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractAggregateEnumerator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.ParquetNoFilterColumnAggregateReader;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.type.entity.PolyValue;

/**
 * This is the file-aware aggregate path. It groups from file-constant values and uses the no-filter column aggregate reader for data columns.
 */
public class ParquetFileGroupedAggregateRelEnumerator extends AbstractAggregateEnumerator {

    private static final ParquetConstantColumnResolver constantColumnResolver = new ParquetConstantColumnResolver();


    /**
     * Creates a new instance of fast aggregate enumerator.
     *
     * @param table a source table.
     * @param fields an array of field indexes.
     * @param groupFields an array of group by field indexes.
     * @param aggregateKinds an array of aggregate function kinds: COUNT, SUM, MIN, MAX.
     * @param aggregateArgs column indexes of the columns passed to the aggregation function as an argument.
     * @param projection aggregate projection containing additional information.
     * @param filters a list of filters.
     * @param cancelFlag a cancellation flag.
     */
    public ParquetFileGroupedAggregateRelEnumerator( ParquetRelTable table, int[] fields, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs, ColumnAggregateProjection projection, List<ParquetAdapterFilter<PolyValue>> filters, AtomicBoolean cancelFlag ) {
        super( () -> buildEnumerator( table, fields, groupFields, aggregateKinds, aggregateArgs, projection, filters, cancelFlag ) );
    }


    /**
     * Reads data from a single source file.
     *
     * @param table a parquet table
     * @param sourceFile a source file
     * @param fields projected field indexes
     * @param groupFields group by fields indexes
     * @param projection aggregate projection
     * @param cancelFlag a cancel operation flag
     * @return aggregated data
     */
    private static Map<GroupKey, AggregateGroupState> readAll( ParquetRelTable table, ParquetSourceFile sourceFile, int[] fields, int[] groupFields, AggregateCallDescriptor[] aggregateCalls, ColumnAggregateProjection projection, AtomicBoolean cancelFlag ) {
        Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();
        GroupKey groupKey = fileGroupKey( table, sourceFile, fields, groupFields );
        AggregateGroupState groupAggregates = aggregates.computeIfAbsent( groupKey, key -> new AggregateGroupState( aggregateCalls ) );
        addCountStarRows( sourceFile, aggregateCalls, groupAggregates );

        if ( projection.fields().length > 0 ) {
            try ( ParquetNoFilterColumnAggregateReader reader = new ParquetNoFilterColumnAggregateReader( sourceFile.asSource(), cancelFlag, projection.fields() ) ) {
                addColumnResults( reader.readAggregateColumns(), aggregateCalls, projection, groupAggregates );
            } catch ( Exception e ) {
                throw new RuntimeException( "Unable to calculate streaming aggregate for " + sourceFile.asSource().path(), e );
            }
        }
        return aggregates;
    }


    /**
     * Aggregates results from all columns that were calculated separately.
     *
     * @param columnResults an array of aggregated column results.
     * @param aggregateCalls an array of aggregate functions used.
     * @param projection a projection.
     * @param groupAggregates an aggregate state to be updated with all the results.
     */
    private static void addColumnResults( ColumnAggregateResult[] columnResults, AggregateCallDescriptor[] aggregateCalls, ColumnAggregateProjection projection, AggregateGroupState groupAggregates ) {
        for ( int i = 0; i < aggregateCalls.length; i++ ) {
            if ( aggregateCalls[i].argumentIndex() < 0 ) {
                continue;
            }
            AggregateCallDescriptor.Kind kind = aggregateCalls[i].kind();
            ColumnAggregateResult result = columnResults[projection.aggregateIndex( aggregateCalls[i].argumentIndex() )];
            switch ( kind ) {
                case COUNT -> groupAggregates.addCount( i, result.count() );
                case MIN -> {
                    if ( result.count() > 0 ) {
                        groupAggregates.addMin( i, result.min() );
                    }
                }
                case MAX -> {
                    if ( result.count() > 0 ) {
                        groupAggregates.addMax( i, result.max() );
                    }
                }
                case SUM -> {
                    groupAggregates.addCount( i, result.count() );
                    groupAggregates.addSum( i, result.sum() );
                }
            }
        }
    }


    /**
     * Adds source rows to all COUNT(*) accumulators. Fast aggregation uses this only when filters and grouping are file-decidable,
     * so all rows in the source file belong to a single output group.
     *
     * @param sourceFile a source file.
     * @param aggregateCalls aggregate calls.
     * @param groupAggregates aggregate values for a single group.
     */
    private static void addCountStarRows( ParquetSourceFile sourceFile, AggregateCallDescriptor[] aggregateCalls, AggregateGroupState groupAggregates ) {
        long sourceRowCount = -1;
        for ( int i = 0; i < aggregateCalls.length; i++ ) {
            if ( aggregateCalls[i].argumentIndex() < 0 ) {
                if ( sourceRowCount < 0 ) {
                    sourceRowCount = sourceRowCount( sourceFile );
                }
                groupAggregates.addCount( i, sourceRowCount );
            }
        }
    }


    /**
     * Creates a group key based on file-constant values.
     *
     * @param table a source table.
     * @param sourceFile a source file.
     * @param fields an array of field indexes.
     * @param groupFields an array of group by field indexes.
     * @return returns a list of {@link PolyValue}s representing a key.
     */
    private static GroupKey fileGroupKey( ParquetRelTable table, ParquetSourceFile sourceFile, int[] fields, int[] groupFields ) {
        Object[] key = new Object[groupFields.length];
        for ( int i = 0; i < groupFields.length; i++ ) {
            ParquetColumnBinding binding = selectPhysicalBinding( table, fields[groupFields[i]] );
            key[i] = constantColumnResolver
                    .resolve( sourceFile, binding )
                    .orElseThrow( () -> new IllegalStateException( "Group column is not constant for " + sourceFile.fileUrl() ) );
        }
        return GroupKey.of( key );
    }


    /**
     * Checks whether all filters match the complete source file.
     *
     * @param evaluator a file-level filter evaluator.
     * @param sourceFile a source file.
     * @param filters filters to evaluate.
     * @return true when the complete source file matches.
     */
    private static boolean matchesExactly( ParquetMultiFilterEvaluator<ParquetSourceFile> evaluator, ParquetSourceFile sourceFile, List<ParquetAdapterFilter<PolyValue>> filters ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            Boolean result = evaluator.evaluate( sourceFile, filter );
            if ( result == null ) {
                throw new IllegalStateException( "File aggregate filter could not be evaluated exactly for " + sourceFile.fileUrl() );
            }
            if ( !result ) {
                return false;
            }
        }
        return true;
    }


    private static Enumerator<PolyValue[]> buildEnumerator( ParquetRelTable table, int[] fields, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs, ColumnAggregateProjection projection, List<ParquetAdapterFilter<PolyValue>> filters, AtomicBoolean cancelFlag ) {
        AggregateCallDescriptor[] aggregateCalls = aggregateCalls( aggregateKinds, aggregateArgs );
        ParquetMultiFilterEvaluator<ParquetSourceFile> fileFilterEvaluator = ParquetRelExecutor.createParquetSourceFileEvaluatorsChain(f -> selectPhysicalBinding( table, f.columnIndex() ) );

        List<ParquetSourceFile> sourceFiles = table.getBinding().sourceFiles().stream()
                .filter( sourceFile -> matchesExactly( fileFilterEvaluator, sourceFile, filters ) )
                .toList();

        if ( sourceFiles.isEmpty() ) {
            Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();
            return Linq4j.asEnumerable( buildRows( groupFields.length, aggregates, aggregateCalls ) ).enumerator();
        }

        Map<GroupKey, AggregateGroupState> aggregates = readAll( sourceFiles, ( f ) -> readAll( table, f, fields, groupFields, aggregateCalls, projection, cancelFlag ), aggregateCalls, cancelFlag );
        return Linq4j.asEnumerable( buildRows( groupFields.length, aggregates, aggregateCalls ) ).enumerator();
    }

}
