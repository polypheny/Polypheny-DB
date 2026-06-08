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

package org.polypheny.db.adapter.parquet.relational.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetMultiFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFilePartitionFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFileStatisticsFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetConstantColumnResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.ImmutableBitSet;

/**
 * Executes aggregations that can be done on metadata only.
 */
public class ParquetRelMetadataAggregateExecutor extends ParquetRelExecutor {

    private final ParquetTypeConverter typeConverter;
    private final ParquetSourceFileStatisticsFilterEvaluator statisticsFilterEvaluator;
    private final ParquetConstantColumnResolver constantColumnResolver;


    public ParquetRelMetadataAggregateExecutor( ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        super( table, parquetSource, fieldIndexes, schemaReader );
        this.typeConverter = new ParquetTypeConverter();
        this.statisticsFilterEvaluator = new ParquetSourceFileStatisticsFilterEvaluator( f -> selectPhysicalBinding( table, f.columnIndex() ) );
        this.constantColumnResolver = new ParquetConstantColumnResolver();
    }


    /**
     * Calculates rows count based on file statistics. In case statistics are not presented fallback to a schema reader for actual calculation.
     *
     * @param sourceFile a source file.
     * @return a row count.
     */
    private static long sourceRowCount( ParquetSourceFile sourceFile ) {
        Optional<Long> rowCount = sourceFile.columnStatistics().values().stream()
                .map( ParquetColumnStatistics::rowCount )
                .findFirst();
        return rowCount.orElseGet( () -> new ParquetSchemaReader( sourceFile.asSource() ).getEstimatedRowCount() );
    }


    /**
     * Creates an enumerable over parquet files metadata statistics.
     *
     * @param dataContext a data context
     * @param fields an array of physical field indexes of columns used in a group by statement.
     * @param filters a list of filters to be applied
     * @param groupFields an array of group by field indexes in fields.
     * @param aggregateKinds the names of the aggregation function.
     * @param aggregateArgs input field indexes used by aggregation functions.
     * @return an enumerable.
     */
    public Enumerable<PolyValue[]> createEnumerator( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        registerAdapter( dataContext );

        List<ParquetAdapterFilter<PolyValue>> resolvedFilters = ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectPhysicalBinding( table, f.columnIndex() ) );
        ParquetMultiFilterEvaluator<ParquetSourceFile> multiFilterEvaluator = new ParquetMultiFilterEvaluator<>(
                new ParquetSourceFilePartitionFilterEvaluator( f -> selectPhysicalBinding( table, f.columnIndex() ) ),
                statisticsFilterEvaluator
        );

        Map<List<PolyValue>, MetadataAggregateAccumulator[]> aggregates = new LinkedHashMap<>();
        for ( ParquetSourceFile sourceFile : table.getBinding().sourceFiles() ) {
            if ( !matchesExactly( multiFilterEvaluator, sourceFile, resolvedFilters ) ) {
                continue;
            }
            List<PolyValue> groupKey = fileGroupKey( sourceFile, fields, groupFields );
            MetadataAggregateAccumulator[] groupAggregates = aggregates.computeIfAbsent( groupKey, key -> createAccumulators( aggregateKinds ) );
            for ( int i = 0; i < groupAggregates.length; i++ ) {
                groupAggregates[i].add( sourceFile, fields, aggregateArgs[i] );
            }
        }

        if ( groupFields.length == 0 && aggregates.isEmpty() ) {
            aggregates.put( List.of(), createAccumulators( aggregateKinds ) );
        }

        List<PolyValue[]> rows = new ArrayList<>( aggregates.size() );
        for ( Map.Entry<List<PolyValue>, MetadataAggregateAccumulator[]> entry : aggregates.entrySet() ) {
            PolyValue[] row = new PolyValue[entry.getKey().size() + aggregateKinds.length];
            for ( int i = 0; i < entry.getKey().size(); i++ ) {
                row[i] = entry.getKey().get( i );
            }
            for ( int i = 0; i < aggregateKinds.length; i++ ) {
                row[entry.getKey().size() + i] = entry.getValue()[i].result();
            }
            rows.add( row );
        }
        return Linq4j.asEnumerable( rows );
    }


    /**
     * Checks whether filters, groups and aggregate functions can be executed on metadata only.
     *
     * @param fields an array of physical field indexes of columns used in a group by statement.
     * @param filters a list of filters to be validated.
     * @param groupSet a group by set.
     * @param aggregateCalls a list of aggregate calls (aggregate functions)
     * @return {@code true} filters, groups and aggregate functions can be executed on metadata only and {@code false} otherwise.
     */
    public boolean supportsMetadataAggregate( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        for ( int groupField : groupSet.asList() ) {
            if ( groupField < 0 || groupField >= fields.length || !isFileConstantColumn( fields[groupField] ) ) {
                return false;
            }
        }
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !supportsMetadataFilter( statisticsFilterEvaluator, filter ) ) {
                return false;
            }
        }
        for ( AggregateCall aggregateCall : aggregateCalls ) {
            if ( !supportsMetadataAggregateCall( fields, aggregateCall ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Checks whether a filter can be evaluated exactly from partition values or
     * file-level statistics. Statistics filters are limited to file-constant
     * physical columns so retained files can contribute their full metadata
     * row count without reading data pages.
     *
     * @param statisticsFilterEvaluator evaluates physical-column file statistics.
     * @param filter a filter to validate.
     * @return {@code true} if the filter can be evaluated exactly for every source file.
     */
    private boolean supportsMetadataFilter( ParquetSourceFileStatisticsFilterEvaluator statisticsFilterEvaluator, ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            if ( filter.operator() == Kind.NOT ) {
                return filter.operands().size() == 1 && isPartitionFilter( table, filter );
            }
            if ( filter.operator() != Kind.AND && filter.operator() != Kind.OR ) {
                return false;
            }
            return !filter.operands().isEmpty() && filter.operands().stream().allMatch( operand -> supportsMetadataFilter( statisticsFilterEvaluator, operand ) );
        }
        if ( isPartitionColumn( table, filter.columnIndex() ) ) {
            return true;
        }
        return table.getBinding().sourceFiles().stream().allMatch( sourceFile -> statisticsFilterEvaluator.supportsExactEvaluation( sourceFile, filter ) );
    }


    /**
     * Applies filters that were proven metadata-decidable during planning.
     *
     * @param evaluator combines partition-value and file-statistics evaluators.
     * @param sourceFile a source file.
     * @param filters filters to apply.
     * @return {@code true} if the complete file matches the filters.
     */
    private boolean matchesExactly( ParquetMultiFilterEvaluator<ParquetSourceFile> evaluator, ParquetSourceFile sourceFile, List<ParquetAdapterFilter<PolyValue>> filters ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            Boolean result = evaluator.evaluate( sourceFile, filter );
            if ( result == null ) {
                throw new GenericRuntimeException( "Metadata aggregate filter could not be evaluated exactly for " + sourceFile.fileUrl() );
            }
            if ( !result ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Creates a joined key from file-constant group by fields.
     *
     * @param sourceFile a source file.
     * @param fields an array of physical field indexes of columns used in a group by statement.
     * @param groupFields an array of group by field indexes in fields.
     * @return a list of {@link PolyValue}s representing a key.
     */
    private List<PolyValue> fileGroupKey( ParquetSourceFile sourceFile, int[] fields, int[] groupFields ) {
        List<PolyValue> key = new ArrayList<>( groupFields.length );
        for ( int groupField : groupFields ) {
            int physicalField = fields[groupField];
            ParquetColumnBinding columnBinding = selectPhysicalBinding( table, physicalField );
            key.add( constantColumnResolver.resolve( sourceFile, columnBinding )
                    .orElseThrow( () -> new GenericRuntimeException( "Metadata aggregate group column is not constant for " + sourceFile.fileUrl() ) ) );
        }
        return List.copyOf( key );
    }


    /**
     * Checks whether one field has a constant value in every source file.
     *
     * @param field a physical table field.
     * @return true when each file has one value for this field.
     */
    private boolean isFileConstantColumn( int field ) {
        ParquetColumnBinding binding = selectPhysicalBinding( table, field );
        return table.getBinding().sourceFiles().stream().allMatch( sourceFile -> constantColumnResolver.resolve( sourceFile, binding ).isPresent() );
    }


    /**
     * Checks if a metadata aggregate call can be calculated from source file statistics.
     *
     * @param fields an array of physical field indexes of columns used in a group by statement.
     * @param aggregateCall an aggregate call to validate.
     * @return {@code true} if the aggregate call can be calculated from metadata and {@code false} otherwise.
     */
    private boolean supportsMetadataAggregateCall( int[] fields, AggregateCall aggregateCall ) {
        if ( aggregateCall.getAggregation().getKind() == Kind.COUNT ) {
            if ( aggregateCall.getArgList().isEmpty() ) {
                return true;
            }
            return aggregateCall.getArgList().size() == 1 && supportsCountColumn( fields, aggregateCall.getArgList().get( 0 ) );
        }
        if ( aggregateCall.getAggregation().getKind() != Kind.MIN && aggregateCall.getAggregation().getKind() != Kind.MAX ) {
            return false;
        }
        if ( aggregateCall.getArgList().size() != 1 ) {
            return false;
        }

        ParquetColumnBinding columnBinding = dataColumnBinding( fields, aggregateCall.getArgList().get( 0 ) );
        if ( columnBinding == null ) {
            return false;
        }

        for ( ParquetSourceFile sourceFile : table.getBinding().sourceFiles() ) {
            ParquetColumnStatistics statistics = sourceFile.columnStatistics().get( columnBinding.sourcePathElements() );
            if ( statistics == null || (!statistics.hasRange() && !statistics.hasOnlyNulls()) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Checks if a COUNT on a column is supported. Column aggregation is supported only if it has statistics and nulls count.
     *
     * @param fields an array of field indexes.
     * @param inputIndex a column index.
     * @return {@code true} if COUNT can be applied on the specified column and {@code false} otherwise.
     */
    private boolean supportsCountColumn( int[] fields, int inputIndex ) {
        ParquetColumnBinding columnBinding = dataColumnBinding( fields, inputIndex );
        if ( columnBinding == null ) {
            return false;
        }

        for ( ParquetSourceFile sourceFile : table.getBinding().sourceFiles() ) {
            ParquetColumnStatistics statistics = sourceFile.columnStatistics().get( columnBinding.sourcePathElements() );
            if ( statistics == null || statistics.nullCount() == null ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Gets a binding for the specified column.
     *
     * @param fields an array of field indexes.
     * @param inputIndex a column index.
     * @return {@link ParquetColumnBinding}.
     */
    private ParquetColumnBinding dataColumnBinding( int[] fields, int inputIndex ) {
        if ( inputIndex < 0 || inputIndex >= fields.length ) {
            return null;
        }

        ParquetColumnBinding columnBinding = selectPhysicalBinding( table, fields[inputIndex] );
        if ( columnBinding == null || columnBinding.role() != ParquetColumnRole.DATA || columnBinding.sourcePathElements().isEmpty() ) {
            return null;
        }
        return columnBinding;
    }


    /**
     * Creates an accumulator for each aggregation function.
     *
     * @param aggregateKinds the names of aggregation function.
     * @return an array of aggregate accumulators.
     */
    private MetadataAggregateAccumulator[] createAccumulators( String[] aggregateKinds ) {
        return Arrays.stream( aggregateKinds )
                .map( MetadataAggregateAccumulator::new )
                .toArray( MetadataAggregateAccumulator[]::new );
    }


    /**
     * Holds partial metadata aggregate values for one result group.
     */
    private class MetadataAggregateAccumulator {

        private final String aggregateKind;
        private long count;
        private ParquetColumnStatistics selectedStatistics;
        private String selectedValue;


        private MetadataAggregateAccumulator( String aggregateKind ) {
            this.aggregateKind = aggregateKind;
        }


        private void add( ParquetSourceFile sourceFile, int[] fields, int aggregateArg ) {
            if ( Kind.COUNT.name().equals( aggregateKind ) ) {
                if ( aggregateArg < 0 ) {
                    count += sourceRowCount( sourceFile );
                    return;
                }

                ParquetColumnStatistics statistics = statistics( sourceFile, fields, aggregateArg );
                if ( statistics != null && statistics.nullCount() != null ) {
                    count += Math.max( 0L, statistics.rowCount() - statistics.nullCount() );
                }
                return;
            }

            ParquetColumnStatistics statistics = statistics( sourceFile, fields, aggregateArg );
            if ( statistics == null || !statistics.hasRange() ) {
                return;
            }
            String candidate = Kind.MIN.name().equals( aggregateKind ) ? statistics.min() : statistics.max();
            if ( selectedValue == null || compare( statistics, candidate, selectedValue ) ) {
                selectedStatistics = statistics;
                selectedValue = candidate;
            }
        }


        private PolyValue result() {
            if ( Kind.COUNT.name().equals( aggregateKind ) ) {
                return PolyLong.of( count );
            }
            if ( selectedValue == null || selectedStatistics == null ) {
                return PolyNull.NULL;
            }
            PolyValue value = typeConverter.fromStringToPolyValue( selectedStatistics.type(), selectedValue );
            return value == null ? PolyNull.NULL : value;
        }


        private boolean compare( ParquetColumnStatistics candidateStatistics, String candidate, String current ) {
            int comparison = typeConverter.compareStringValues( candidateStatistics.type(), candidate, current );
            return Kind.MIN.name().equals( aggregateKind ) ? comparison < 0 : comparison > 0;
        }


        private ParquetColumnStatistics statistics( ParquetSourceFile sourceFile, int[] fields, int aggregateArg ) {
            if ( aggregateArg < 0 || aggregateArg >= fields.length ) {
                return null;
            }
            ParquetColumnBinding columnBinding = selectPhysicalBinding( table, fields[aggregateArg] );
            if ( columnBinding == null ) {
                return null;
            }
            return sourceFile.columnStatistics().get( columnBinding.sourcePathElements() );
        }

    }

}
