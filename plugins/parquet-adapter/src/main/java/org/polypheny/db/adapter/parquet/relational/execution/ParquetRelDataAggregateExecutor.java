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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetFileGroupedAggregateRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetGroupedAggregateRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetRowAggregateRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetConstantColumnResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFileStatisticsFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.ColumnAggregateProjection;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.ParquetGroupedAggregateReader;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.ParquetNoFilterColumnAggregateReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;

/**
 * An entry point for all aggregated enumerators that cannot be calculated based on metadata. The executor decides what enumerator should be executed for fastest results.
 * Supported enumerators are:
 * 1. Enumerator for COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) with numeric aggregation columns and no row level filters.
 * 2. Enumerator for COUNT(*) / COUNT(col) / SUM(col) / MIN(col) / MAX(col) with aggregation columns defined as partitions and no row level filters.
 * 3. A fallback enumerator that supports the same aggregation functions for numeric columns but this time with row level filters.
 */
public class ParquetRelDataAggregateExecutor extends ParquetRelExecutor {

    private final ParquetConstantColumnResolver constantColumnResolver;
    private final ParquetSourceFileStatisticsFilterEvaluator statisticsFilterEvaluator;


    public ParquetRelDataAggregateExecutor(ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        super( table, parquetSource, fieldIndexes, schemaReader );
        this.constantColumnResolver = new ParquetConstantColumnResolver();
        this.statisticsFilterEvaluator = new ParquetSourceFileStatisticsFilterEvaluator( f -> selectPhysicalBinding( table, f.columnIndex() ) );
    }


    /**
     * Creates an enumerator for aggregated rows.
     *
     * @param dataContext a data context.
     * @param fields an array of field indexes.
     * @param filters a list of filters to be applied.
     * @param groupFields a list of group by field indexes.
     * @param aggregateKinds an array of aggregation function kinds.
     * @param aggregateArgs an array of aggregation function arguments. There should be one argument per aggregation function.
     * @return an enumerator.
     */
    public Enumerable<PolyValue[]> createEnumerator( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        registerAdapter( dataContext );

        List<ParquetAdapterFilter<PolyValue>> resolvedFilters = ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectPhysicalBinding( table, f.columnIndex() ) );
        AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                ColumnAggregateProjection aggregateProjection = tryBuildAggregateProjection( fields, filters, groupFields, aggregateKinds, aggregateArgs );
                if ( aggregateProjection != null ) {
                    return new ParquetFileGroupedAggregateRelEnumerator(
                            table,
                            fields,
                            groupFields,
                            aggregateKinds,
                            aggregateArgs,
                            aggregateProjection,
                            resolvedFilters,
                            cancelFlag
                    );
                }

                int[] groupedCountProjection = tryBuildGroupedCountProjection( fields, filters, groupFields, aggregateKinds, aggregateArgs );
                if ( groupedCountProjection != null ) {
                    return new ParquetGroupedAggregateRelEnumerator(
                            table,
                            groupedCountProjection,
                            groupedCountProjection.length,
                            new AggregateCallDescriptor[]{ AggregateCallDescriptor.countStar() },
                            resolvedFilters,
                            cancelFlag
                    );
                }

                GroupAggregateProjection groupedAggregateProjection = tryBuildGroupedAggregateProjection( fields, filters, groupFields, aggregateKinds, aggregateArgs );
                if ( groupedAggregateProjection != null ) {
                    return new ParquetGroupedAggregateRelEnumerator(
                            table,
                            groupedAggregateProjection.fields(),
                            groupedAggregateProjection.groupFieldCount(),
                            groupedAggregateProjection.aggregateCalls(),
                            resolvedFilters,
                            cancelFlag
                    );
                }

                Enumerator<PolyValue[]> rows = new ParquetEnumeratorsFactory( table, fields, fieldIndexes, schemaReader, cancelFlag ).create( resolvedFilters );
                return new ParquetRowAggregateRelEnumerator( rows, groupFields, aggregateKinds, aggregateArgs );
            }
        };
    }


    /**
     * Converts a relative field index into a parquet physical index and adds it into projection.
     *
     * @param field a relative field index.
     * @param parquetFieldProjectionIndexes a projection.
     * @return parquet physical index if found and null otherwise.
     */
    private Integer addDataProjectionField( int field, Map<Integer, Integer> parquetFieldProjectionIndexes ) {
        ParquetColumnBinding binding = selectPhysicalBinding( table, field );
        if ( binding == null || binding.role() != ParquetColumnRole.DATA || binding.sourcePathElements().size() != 1 ) {
            return null;
        }
        int parquetField = parquetFieldIndex( binding.sourcePathElements().get( 0 ) );
        if ( parquetField < 0 ) {
            return null;
        }
        return parquetFieldProjectionIndexes.computeIfAbsent( parquetField, ignored -> parquetFieldProjectionIndexes.size() );
    }


    /**
     * Adds filter indexes to a projection. This is required in order for a filter column to be available during the reading operation.
     *
     * @param filters a list of filters.
     * @param parquetFieldProjectionIndexes a projection.
     * @return true if the index was added and false otherwise.
     */
    private boolean addFilterProjectionFields( List<ParquetAdapterFilter<PolyValue>> filters, Map<Integer, Integer> parquetFieldProjectionIndexes ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !addFilterProjectionField( filter, parquetFieldProjectionIndexes, true ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Adds one filter to the reader projection unless file metadata can evaluate it exactly.
     *
     * @param filter a filter.
     * @param parquetFieldProjectionIndexes a projection.
     * @param canSkipExact whether an exact file-level filter can be omitted in this logical context.
     * @return true if all required fields were added and false otherwise.
     */
    private boolean addFilterProjectionField( ParquetAdapterFilter<PolyValue> filter, Map<Integer, Integer> parquetFieldProjectionIndexes, boolean canSkipExact ) {
        if ( canSkipExact && supportsExactFileFilter( filter ) ) {
            return true;
        }
        if ( filter.isLogical() ) {
            boolean childCanSkipExact = canSkipExact && filter.operator() == Kind.AND;
            for ( ParquetAdapterFilter<PolyValue> operand : filter.operands() ) {
                if ( !addFilterProjectionField( operand, parquetFieldProjectionIndexes, childCanSkipExact ) ) {
                    return false;
                }
            }
            return true;
        }
        if ( isPartitionColumn( table, filter.columnIndex() ) ) {
            return true;
        }
        return addDataProjectionField( filter.columnIndex(), parquetFieldProjectionIndexes ) != null;
    }


    /**
     * Validates if all filters are supported.
     *
     * @param filters a list of filters to validate.
     * @return true if all filters are supported and false otherwise.
     */
    private boolean supportsAggregateReaderFilters( List<ParquetAdapterFilter<PolyValue>> filters ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !supportsAggregateReaderFilter( filter, false ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Validates if the filter is supported by this executor.
     *
     * @param filter a filter to validate.
     * @param insideNonAnd indicates whether a filter is not inside the logical AND filter. This is mostly relevant for partition filters.
     * Only AND logical operator is supported between the partition filters.
     * @return true if the filter is supported and false otherwise.
     */
    private boolean supportsAggregateReaderFilter( ParquetAdapterFilter<PolyValue> filter, boolean insideNonAnd ) {
        if ( filter.isLogical() ) {
            boolean childInsideNonAnd = insideNonAnd || filter.operator() != Kind.AND;
            for ( ParquetAdapterFilter<PolyValue> operand : filter.operands() ) {
                if ( !supportsAggregateReaderFilter( operand, childInsideNonAnd ) ) {
                    return false;
                }
            }
            return true;
        }
        if ( isPartitionColumn( table, filter.columnIndex() ) ) {
            return !insideNonAnd;
        }
        return isFlatDataColumn( filter.columnIndex() );
    }


    /**
     * Validates if the field is a data field.
     *
     * @param field a field index.
     * @return true if the field is a data field and false otherwise.
     */
    private boolean isFlatDataColumn( int field ) {
        ParquetColumnBinding binding = selectPhysicalBinding( table, field );
        return binding != null && binding.role() == ParquetColumnRole.DATA && binding.sourcePathElements().size() == 1;
    }


    /**
     * Validates if the aggregated function type is supported by this executor.
     *
     * @param field a field index.
     * @param aggregateKind an aggregated function type.
     * @return true if supported and false otherwise.
     */
    private boolean supportsGroupedAggregateCall( int field, String aggregateKind ) {
        if ( Kind.COUNT.name().equals( aggregateKind ) ) {
            return true;
        }
        if ( !isNumericAggregateKind( aggregateKind ) ) {
            return false;
        }
        return PolyType.NUMERIC_TYPES.contains( table.columns.get( field ).type );
    }


    /**
     * Attempts to build a grouped aggregate projection. The main criteria is:
     * 1. Filters must be applied only on data columns or partition columns. If a filter column is a partition column only AND logical filter can be applied on top of it.
     * 2. aggregation function is COUNT(*), COUNT(col), SUM(col), MIN(col) or MAX(col).
     * 3. aggregation column must be numeric non-repeated.
     *
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @param groupFields an array of group by field indexes.
     * @param aggregateKinds an array of aggregation functions types.
     * @param aggregateArgs an array of aggregation functions arguments. One argument per aggregation function and should be aligned with aggregateKinds.
     * @return a grouped aggregate projection if successful or null otherwise.
     */
    private GroupAggregateProjection tryBuildGroupedAggregateProjection( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        if ( aggregateKinds.length != aggregateArgs.length ) {
            return null;
        }
        if ( !supportsAggregateReaderFilters( filters ) ) {
            return null;
        }

        Map<Integer, Integer> parquetFieldProjectionIndexes = new LinkedHashMap<>();
        for ( int groupField : groupFields ) {
            if ( groupField < 0 || groupField >= fields.length ) {
                return null;
            }
            Integer projectionIndex = addDataProjectionField( fields[groupField], parquetFieldProjectionIndexes );
            if ( projectionIndex == null ) {
                return null;
            }
        }

        AggregateCallDescriptor[] aggregateCalls = new AggregateCallDescriptor[aggregateKinds.length];
        for ( int i = 0; i < aggregateKinds.length; i++ ) {
            int aggregateArg = aggregateArgs[i];
            if ( aggregateArg == AggregateCallDescriptor.NO_ARGUMENT ) {
                if ( !Kind.COUNT.name().equals( aggregateKinds[i] ) ) {
                    return null;
                }
                aggregateCalls[i] = AggregateCallDescriptor.of( aggregateKinds[i], AggregateCallDescriptor.NO_ARGUMENT );
                continue;
            }
            if ( aggregateArg < 0 || aggregateArg >= fields.length ) {
                return null;
            }
            if ( !supportsGroupedAggregateCall( fields[aggregateArg], aggregateKinds[i] ) ) {
                return null;
            }
            Integer projectionIndex = addDataProjectionField( fields[aggregateArg], parquetFieldProjectionIndexes );
            if ( projectionIndex == null ) {
                return null;
            }
            aggregateCalls[i] = AggregateCallDescriptor.of( aggregateKinds[i], projectionIndex );
        }

        if ( !addFilterProjectionFields( filters, parquetFieldProjectionIndexes ) ) {
            return null;
        }
        int[] projectedFields = parquetFieldProjectionIndexes.keySet().stream().mapToInt( Integer::intValue ).toArray();
        if ( projectedFields.length == 0 || !ParquetGroupedAggregateReader.supports( schemaReader.buildProjectionSchema( projectedFields ) ) ) {
            return null;
        }
        return new GroupAggregateProjection( projectedFields, groupFields.length, aggregateCalls );
    }


    /**
     * Attempts to build grouped count projection. The main criteria is:
     * 1. fields must be numeric
     * 2. aggregation function is COUNT(*)
     * 3. filters are empty or only partition filters.
     * 4. aggregation column is a simple not nested data column (not a partition, not a key).
     *
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @param groupFields an array of group by field indexes.
     * @param aggregateKinds an array of aggregation functions types.
     * @param aggregateArgs an array of aggregation functions arguments. One argument per aggregation function and should be aligned with aggregateKinds.
     * @return a grouped count projection if successful or null otherwise.
     */
    private int[] tryBuildGroupedCountProjection( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        if ( aggregateKinds.length == 0 || aggregateKinds.length != aggregateArgs.length ) {
            return null;
        }
        for ( int i = 0; i < aggregateKinds.length; i++ ) {
            if ( !Kind.COUNT.name().equals( aggregateKinds[i] ) || aggregateArgs[i] >= 0 ) {
                return null;
            }
        }
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !isPartitionFilter( table, filter ) ) {
                return null;
            }
        }

        int[] projectedFields = new int[groupFields.length];
        for ( int i = 0; i < groupFields.length; i++ ) {
            int groupField = groupFields[i];
            if ( groupField < 0 || groupField >= fields.length ) {
                return null;
            }
            ParquetColumnBinding binding = selectPhysicalBinding( table, fields[groupField] );
            if ( binding == null || binding.role() != ParquetColumnRole.DATA || binding.sourcePathElements().size() != 1 ) {
                return null;
            }
            int parquetField = parquetFieldIndex( binding.sourcePathElements().get( 0 ) );
            if ( parquetField < 0 ) {
                return null;
            }
            projectedFields[i] = parquetField;
        }
        if ( !ParquetGroupedAggregateReader.supports( schemaReader.buildProjectionSchema( projectedFields ) ) ) {
            return null;
        }
        return projectedFields;
    }


    /**
     * This method attempts to build aggregate projection. In order for the aggregate projection to be built the following conditions should apply:
     * 1. All group by fields are file-constant fields.
     * 2. All filters can be evaluated exactly for a complete source file.
     * 3. Aggregated columns are data columns with numeric type and does not belong to a nested table.
     *
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @param groupFields an array of group by field indexes.
     * @param aggregateKinds an array of aggregation functions types.
     * @param aggregateArgs an array of aggregated column indexes.
     * @return {@link ColumnAggregateProjection} if fast aggregation can be applied and {@code null} otherwise.
     */
    private ColumnAggregateProjection tryBuildAggregateProjection( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        if ( aggregateKinds.length == 0 || aggregateKinds.length != aggregateArgs.length ) {
            return null;
        }
        for ( int groupField : groupFields ) {
            if ( groupField < 0 || groupField >= fields.length || !isFileConstantColumn( fields[groupField] ) ) {
                return null;
            }
        }
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( !supportsExactFileFilter( filter ) ) {
                return null;
            }
        }

        Map<Integer, Integer> parquetFieldProjectionIndexes = new LinkedHashMap<>();
        Map<Integer, Integer> aggregateProjectionIndexes = new LinkedHashMap<>();
        for ( int i = 0; i < aggregateArgs.length; i++ ) {
            int aggregateArg = aggregateArgs[i];
            if ( aggregateArg < 0 ) {
                if ( !Kind.COUNT.name().equals( aggregateKinds[i] ) ) {
                    return null;
                }
                continue;
            }
            if ( aggregateArg >= fields.length ) {
                return null;
            }
            if ( !supportsGroupedAggregateCall( fields[aggregateArg], aggregateKinds[i] ) ) {
                return null;
            }
            ParquetColumnBinding binding = selectPhysicalBinding( table, fields[aggregateArg] );
            if ( binding == null || binding.role() != ParquetColumnRole.DATA || binding.sourcePathElements().size() != 1 ) {
                return null;
            }
            int parquetField = parquetFieldIndex( binding.sourcePathElements().get( 0 ) );
            if ( parquetField < 0 ) {
                return null;
            }
            int projectionIndex = parquetFieldProjectionIndexes.computeIfAbsent( parquetField, ignored -> parquetFieldProjectionIndexes.size() );
            aggregateProjectionIndexes.put( aggregateArg, projectionIndex );
        }
        int[] projectedFields = parquetFieldProjectionIndexes.keySet().stream().mapToInt( Integer::intValue ).toArray();
        if ( projectedFields.length > 0 && !ParquetNoFilterColumnAggregateReader.supports( schemaReader.buildProjectionSchema( projectedFields ) ) ) {
            return null;
        }
        return new ColumnAggregateProjection( projectedFields, aggregateProjectionIndexes );
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
     * Checks whether a filter can be evaluated exactly for each complete source file.
     *
     * @param filter a filter.
     * @return true when the fast file aggregate path can apply the filter.
     */
    private boolean supportsExactFileFilter( ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            if ( filter.operator() == Kind.NOT ) {
                return filter.operands().size() == 1 && supportsExactFileFilter( filter.operands().get( 0 ) );
            }
            if ( filter.operator() != Kind.AND && filter.operator() != Kind.OR ) {
                return false;
            }
            return !filter.operands().isEmpty() && filter.operands().stream().allMatch( this::supportsExactFileFilter );
        }
        if ( isPartitionColumn( table, filter.columnIndex() ) ) {
            return true;
        }
        return table.getBinding().sourceFiles().stream().allMatch( sourceFile -> statisticsFilterEvaluator.supportsExactEvaluation( sourceFile, filter ) );
    }


    /**
     * Gets a physical parquet field index.
     *
     * @param fieldName a field name for a lookup.
     * @return a physical parquet field index if found or -1 otherwise.
     */
    private int parquetFieldIndex( String fieldName ) {
        for ( int i = 0; i < schemaReader.getSchema().getFieldCount(); i++ ) {
            if ( schemaReader.getSchema().getFieldName( i ).equals( fieldName ) ) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Checks whether all provided aggregate functions are supported. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsDataAggregate( int[] fields, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        for ( int groupField : groupSet.asList() ) {
            if ( groupField < 0 || groupField >= fields.length ) {
                return false;
            }
        }
        for ( AggregateCall aggregateCall : aggregateCalls ) {
            if ( !supportsDataAggregateCall( fields, aggregateCall ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Checks whether specified aggregation function is supported. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param aggregateCall an aggregation function to validate.
     * @return {@code true} if the aggregation function is supported and {@code false} otherwise.
     */
    private boolean supportsDataAggregateCall( int[] fields, AggregateCall aggregateCall ) {
        if ( aggregateCall.isDistinct() || aggregateCall.isApproximate() || aggregateCall.filterArg >= 0 ) {
            return false;
        }
        Kind kind = aggregateCall.getAggregation().getKind();
        if ( kind == Kind.COUNT ) {
            if ( aggregateCall.getArgList().isEmpty() ) {
                return true;
            }
            if ( aggregateCall.getArgList().size() != 1 ) {
                return false;
            }
            int inputIndex = aggregateCall.getArgList().get( 0 );
            return inputIndex >= 0 && inputIndex < fields.length;
        }
        if ( !isNumericAggregateKind( kind.name() ) ) {
            return false;
        }
        if ( aggregateCall.getArgList().size() != 1 ) {
            return false;
        }
        int inputIndex = aggregateCall.getArgList().get( 0 );
        return inputIndex >= 0 && inputIndex < fields.length && PolyType.NUMERIC_TYPES.contains( table.columns.get( fields[inputIndex] ).type );
    }


    private boolean isNumericAggregateKind( String aggregateKind ) {
        return Kind.SUM.name().equals( aggregateKind )
                || Kind.MIN.name().equals( aggregateKind )
                || Kind.MAX.name().equals( aggregateKind );
    }


    private record GroupAggregateProjection( int[] fields, int groupFieldCount, AggregateCallDescriptor[] aggregateCalls ) {

    }


}
