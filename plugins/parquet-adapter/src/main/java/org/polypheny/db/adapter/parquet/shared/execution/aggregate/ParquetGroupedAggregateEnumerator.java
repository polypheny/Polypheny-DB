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

package org.polypheny.db.adapter.parquet.shared.execution.aggregate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.parquet.schema.MessageType;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetMultiFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFileFilterReducer;
import org.polypheny.db.adapter.parquet.relational.filter.ResidualFilters;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractAggregateEnumerator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.ParquetGroupedAggregateReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;


/**
 * GROUP BY over real Parquet data columns.
 */
public class ParquetGroupedAggregateEnumerator extends AbstractAggregateEnumerator {

    private static final ParquetTypeConverter TYPE_CONVERTER = new ParquetTypeConverter();


    /**
     * Creates a new instance of an enumerator.
     *
     * @param source an aggregate source.
     * @param projectedFields an array of projected field indexes.
     * @param groupFieldCount a number of groups in Group By statement.
     * @param aggregateCalls an array of aggregate function descriptors.
     * @param filters a list of filters to be applied.
     * @param cancelFlag a flag indicating that the operation has been canceled.
     */
    public ParquetGroupedAggregateEnumerator( ParquetAggregateSource source, int[] projectedFields, int groupFieldCount, AggregateCallDescriptor[] aggregateCalls, List<ParquetAdapterFilter<PolyValue>> filters, AtomicBoolean cancelFlag ) {
        super( () -> buildEnumerator( source, projectedFields, groupFieldCount, aggregateCalls, filters, cancelFlag ) );
    }


    /**
     * Reads all rows from a specified source files and aggregates them.
     *
     * @param sourceFile source file to read.
     * @param groupFieldCount a number of groups in Group By statement.
     * @param aggregateCalls an array of aggregate function descriptors.
     * @param cancelFlag a flag indicating that the operation has been canceled.
     * @return aggregated rows.
     */
    private static Map<GroupKey, AggregateGroupState> readAll( ProjectedSourceFile sourceFile, int groupFieldCount, AggregateCallDescriptor[] aggregateCalls, AtomicBoolean cancelFlag ) {
        try ( ParquetGroupedAggregateReader reader = new ParquetGroupedAggregateReader( sourceFile.source(), cancelFlag, sourceFile.projection, groupFieldCount, aggregateCalls, sourceFile.readerFilters ) ) {
            return reader.readAll();
        } catch ( Exception e ) {
            throw new RuntimeException( "Unable to calculate grouped aggregate for " + sourceFile.source().path(), e );
        }
    }


    /**
     * Builds a result set from the aggregated rows.
     *
     * @param sourceFiles a list of source files.
     * @param groupFieldCount a number of groups in Group By statement.
     * @param aggregateCalls aggregate function descriptors.
     * @param aggregates aggregation results.
     * @return a list of aggregated rows.
     */
    private static List<PolyValue[]> buildRows( List<ProjectedSourceFile> sourceFiles, int groupFieldCount, AggregateCallDescriptor[] aggregateCalls, Map<GroupKey, AggregateGroupState> aggregates ) {
        int aggregateCount = aggregateCalls.length;
        if ( sourceFiles.isEmpty() ) {
            if ( groupFieldCount > 0 ) {
                return List.of();
            }
            PolyValue[] row = new PolyValue[aggregateCount];
            AggregateGroupState values = aggregates.getOrDefault( GroupKey.Empty, new AggregateGroupState( aggregateCalls ) );
            for ( int i = 0; i < aggregateCount; i++ ) {
                row[i] = values.result( i );
            }
            return List.<PolyValue[]>of( row );
        }

        List<PolyValue[]> rows = new ArrayList<>( aggregates.size() );
        ProjectedSourceFile sourceFile = sourceFiles.get( 0 );
        aggregates.forEach( ( key, values ) -> rows.add( buildRow( key, values, sourceFile.projection, groupFieldCount, aggregateCount ) ) );
        return rows;
    }


    /**
     * Builds a single row.
     *
     * @param key a group key.
     * @param values aggregated values.
     * @param projectionSchema a projected schema.
     * @param groupFieldCount a number of groups in Group By statement.
     * @param aggregateCount a number of aggregated functions used.
     * @return an array of PolyValue's representing a row.
     */
    private static PolyValue[] buildRow( GroupKey key, AggregateGroupState values, MessageType projectionSchema, int groupFieldCount, int aggregateCount ) {
        PolyValue[] row = new PolyValue[groupFieldCount + aggregateCount];
        for ( int i = 0; i < groupFieldCount; i++ ) {
            row[i] = TYPE_CONVERTER.fromObjToPolyValue( projectionSchema.getType( i ), key.value( i ) );
        }
        for ( int i = 0; i < aggregateCount; i++ ) {
            row[groupFieldCount + i] = values.result( i );
        }
        return row;
    }


    /**
     * Converts filter indexes from relative to projection to physical parquet indexes.
     *
     * @param source an aggregate source.
     * @param projectedFields a projected field indexes.
     * @param filters filters to convert.
     * @return a converted list of filters with physical parquet indexes.
     */
    private static List<ParquetAdapterFilter<PolyValue>> readerFilters( ParquetAggregateSource source, int[] projectedFields, List<ParquetAdapterFilter<PolyValue>> filters ) {
        List<ParquetAdapterFilter<PolyValue>> mapped = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            ParquetAdapterFilter<PolyValue> projectionFilter = ParquetFilterResolver.toProjectionFilter( filter, field -> readerIndex( source, projectedFields, field ) );
            if ( projectionFilter == null ) {
                throw new IllegalArgumentException( "Unable to map grouped aggregate filter to its parquet projection." );
            }
            mapped.add( projectionFilter );
        }
        return mapped;
    }


    /**
     * Converts a projected index into a physical parquet file index.
     *
     * @param source an aggregate source.
     * @param projectedFields projected field indexes.
     * @param field a projected field index to convert.
     * @return a physical parquet index.
     */
    private static int readerIndex( ParquetAggregateSource source, int[] projectedFields, int field ) {
        ParquetColumnBinding binding = source.binding( field );
        if ( binding == null || binding.sourcePathElements().size() != 1 ) {
            return -1;
        }
        int parquetField = source.parquetFieldIndex( binding.sourcePathElements().get( 0 ) );
        for ( int i = 0; i < projectedFields.length; i++ ) {
            if ( projectedFields[i] == parquetField ) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Enriches {@link ParquetSourceFile} with projection and filters if available.
     * The method also evaluates file level filters and can discard the file.
     *
     * @param source an aggregate source.
     * @param projectedFields an array of projected indexes.
     * @param sourceFile a source file to enrich and evaluate.
     * @param evaluator a filter evaluator.
     * @param filters a list of filters.
     * @return {@link Optional<ProjectedSourceFile>} if source file is valid and {@link Optional#empty()} otherwise
     */
    private static Optional<ProjectedSourceFile> projectedSourceFile( ParquetAggregateSource source, int[] projectedFields, ParquetSourceFile sourceFile, ParquetMultiFilterEvaluator<ParquetSourceFile> evaluator, List<ParquetAdapterFilter<PolyValue>> filters ) {
        ResidualFilters residualFilters = ParquetSourceFileFilterReducer.reduce( sourceFile, evaluator, filters );
        if ( !residualFilters.matches() ) {
            return Optional.empty();
        }
        ParquetSchemaReader schemaReader = new ParquetSchemaReader( sourceFile.asSource() );
        return Optional.of( new ProjectedSourceFile(
                sourceFile,
                schemaReader.buildProjectionSchema( projectedFields ),
                readerFilters( source, projectedFields, residualFilters.filters() ) ) );
    }


    private static Enumerator<PolyValue[]> buildEnumerator( ParquetAggregateSource source, int[] projectedFields, int groupFieldCount, AggregateCallDescriptor[] aggregateCalls, List<ParquetAdapterFilter<PolyValue>> filters, AtomicBoolean cancelFlag ) {
        ParquetMultiFilterEvaluator<ParquetSourceFile> fileFilterEvaluator = ParquetDataAggregateExecutor.createParquetSourceFileEvaluatorsChain( filter -> source.binding( filter.columnIndex() ) );

        List<ProjectedSourceFile> sourceFiles = source.sourceFiles().stream()
                .map( sourceFile -> projectedSourceFile( source, projectedFields, sourceFile, fileFilterEvaluator, filters ) )
                .flatMap( Optional::stream )
                .toList();

        Map<GroupKey, AggregateGroupState> aggregates = sourceFiles.isEmpty()
                ? new LinkedHashMap<>()
                : readAll( sourceFiles, f -> readAll( f, groupFieldCount, aggregateCalls, cancelFlag ), aggregateCalls, cancelFlag );

        return Linq4j.asEnumerable( buildRows( sourceFiles, groupFieldCount, aggregateCalls, aggregates ) ).enumerator();
    }


    /**
     * A source file with projection.
     *
     * @param file a source file.
     * @param projection a projection.
     * @param readerFilters native filters for a reader.
     */
    private record ProjectedSourceFile( ParquetSourceFile file, MessageType projection, List<ParquetAdapterFilter<PolyValue>> readerFilters ) {

        public Source source() {
            return file.asSource();
        }

    }

}
