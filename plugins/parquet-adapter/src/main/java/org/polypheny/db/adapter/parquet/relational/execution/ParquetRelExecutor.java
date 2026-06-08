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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetMultiFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFilePartitionFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetSourceFileStatisticsFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitiveRowReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;


/**
 * Represents a base class holding useful methods for all parquet relational executors.
 */
public abstract class ParquetRelExecutor {

    protected final ParquetRelTable table;
    protected final AbstractParquetSource parquetSource;
    protected final int[] fieldIndexes;
    protected final ParquetSchemaReader schemaReader;


    public ParquetRelExecutor( ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        this.table = table;
        this.parquetSource = parquetSource;
        this.fieldIndexes = fieldIndexes;
        this.schemaReader = schemaReader;
    }


    /**
     * Creates one of the three possible enumerators depending on the source file type:
     * - source file contains nested repeated fields -> {@link ParquetNestedRepeatedRelEnumerator}
     * - source file contains nested but not repeated fields -> {@link ParquetNestedNonRepeatedRelEnumerator}
     * - source file doesn't contain nested fields -> {@link ParquetRelEnumerator}
     *
     * @param table a source table
     * @param sourceFile a source file
     * @param fields a list of projected fields
     * @param allFields a list of all table fields
     * @param schemaReader a schema reader
     * @param cancelFlag a flag indicating if the execution is being canceled
     * @param filtersContainer a container for multiple lists of filters.
     * @return one of the selected enumerators.
     */
    protected static Enumerator<PolyValue[]> enumeratorForFile( ParquetRelTable table, ParquetSourceFile sourceFile, int[] fields, int[] allFields, ParquetSchemaReader schemaReader, AtomicBoolean cancelFlag, FiltersContainer filtersContainer ) {
        boolean nestedTable = isNestedTable( table );
        boolean requiresRowValidation = !filtersContainer.adapterFilters().isEmpty();
        boolean bindingScan = nestedTable || needsBindingScan( table, schemaReader, fields );
        Source fileSource = sourceFile.asSource();
        int[] readerFields = bindingScan || requiresRowValidation
                ? createReaderProjection( table, schemaReader, fields, filtersContainer.adapterFilters() )
                : fields;

        if ( !nestedTable ) {
            var projectionSchema = schemaReader.buildProjectionSchema( readerFields );
            if ( ParquetPrimitiveRowReader.supports( projectionSchema ) ) {
                List<ParquetAdapterFilter<PolyValue>> readerFilters = readerFilters( table, schemaReader, readerFields, filtersContainer.adapterFilters() );
                PrimitiveRowProjection outputProjection = primitiveRowProjection( table, schemaReader, sourceFile, readerFields, fields );
                if ( readerFilters != null && outputProjection != null ) {
                    return new ParquetRowRelEnumerator(
                            new ParquetPrimitiveRowReader( fileSource, cancelFlag, readerFields, filtersContainer.nativeFilters() ),
                            readerFilters,
                            outputProjection.outputIndexes(),
                            outputProjection.constants() );
                }
            }
        }

        ParquetSourceReader reader = new ParquetSourceReader( fileSource, cancelFlag, readerFields, filtersContainer.nativeFilters() );
        List<ParquetColumnBinding> columnBindings = projectedBindings( table, fields );
        List<ParquetColumnBinding> filterBindings = projectedBindings( table, allFields );
        if ( nestedTable ) {
            return new ParquetNestedRepeatedRelEnumerator( reader, table.getBinding(), columnBindings, filterBindings, filtersContainer );
        }
        if ( bindingScan ) {
            return new ParquetNestedNonRepeatedRelEnumerator( reader, sourceFile, columnBindings, filterBindings, filtersContainer );
        }
        return new ParquetRelEnumerator( reader, filtersContainer.withoutPathElementsInAdapterFilters() );
    }


    /**
     * Converts filters with physical parquet indexes to filters with projected indexes.
     *
     * @param table a reference o the relational table.
     * @param schemaReader a schema reader.
     * @param fields an array of projected field indexes.
     * @param filters a list of filters to convert.
     * @return a list of converted filters.
     */
    private static List<ParquetAdapterFilter<PolyValue>> readerFilters( ParquetRelTable table, ParquetSchemaReader schemaReader, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters ) {
        List<ParquetAdapterFilter<PolyValue>> mapped = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            ParquetAdapterFilter<PolyValue> readerFilter = ParquetFilterResolver.toProjectionFilter( filter, field -> projectedFieldIndex( table, schemaReader, fields, field ) );
            if ( readerFilter == null ) {
                return null;
            }
            mapped.add( readerFilter );
        }
        return mapped;
    }


    /**
     * Creates an output projection. This is required when the filter was not part of the original projection and was added to a reader projection for availability.
     *
     * @param table a relation table reference.
     * @param schemaReader a schema reader.
     * @param readerFields a reader projected fields.
     * @param fields original projected fields.
     * @return an array of projected indexes excluding filters.
     */
    private static PrimitiveRowProjection primitiveRowProjection( ParquetRelTable table, ParquetSchemaReader schemaReader, ParquetSourceFile sourceFile, int[] readerFields, int[] fields ) {
        int[] outputIndexes = new int[fields.length];
        PolyValue[] constants = new PolyValue[fields.length];
        for ( int i = 0; i < fields.length; i++ ) {
            int index = projectedFieldIndex( table, schemaReader, readerFields, fields[i] );
            if ( index < 0 ) {
                ParquetColumnBinding binding = table.getBinding().getColumnBinding( table.columns.get( fields[i] ).id );
                if ( binding == null || binding.role() != ParquetColumnRole.PARTITION ) {
                    return null;
                }
                outputIndexes[i] = -1;
                constants[i] = partitionValue( sourceFile, binding );
            } else {
                outputIndexes[i] = index;
            }
        }
        return new PrimitiveRowProjection( outputIndexes, constants );
    }


    private static PolyValue partitionValue( ParquetSourceFile sourceFile, ParquetColumnBinding binding ) {
        String value = sourceFile.partitionValues().get( binding.columnName() );
        return value == null ? PolyNull.NULL : PolyString.of( value );
    }


    /**
     * Converts physical field index into projected index.
     *
     * @param table a relational table.
     * @param schemaReader a schema reader.
     * @param fields an array of field indexes.
     * @param field an index to convert.
     * @return converted index if successful or -1 otherwise.
     */
    private static int projectedFieldIndex( ParquetRelTable table, ParquetSchemaReader schemaReader, int[] fields, int field ) {
        int parquetField = parquetFieldIndex( table, schemaReader, field );
        for ( int i = 0; i < fields.length; i++ ) {
            if ( fields[i] == parquetField ) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Gets a physical field index from the physical parquet schema.
     *
     * @param table a relational table reference.
     * @param schemaReader a schema reader.
     * @param field a field index to convert.
     * @return a physical parquet file index if converted or -1 otherwise.
     */
    public static int parquetFieldIndex( ParquetRelTable table, ParquetSchemaReader schemaReader, int field ) {
        if ( field < 0 || field >= table.columns.size() ) {
            return -1;
        }
        ParquetColumnBinding binding = table.getBinding().getColumnBinding( table.columns.get( field ).id );
        if ( binding == null || binding.sourcePathElements().size() != 1 || binding.role() != ParquetColumnRole.DATA ) {
            return -1;
        }
        return parquetFieldIndex( schemaReader, binding.sourcePathElements().get( 0 ) );
    }


    /**
     * Creates projected fields for a reader to reduce the amount of data needed to be read from the parquet file.
     *
     * @param table a source table.
     * @param schemaReader schema reader that provides access to the parquet file schema.
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @return projected fields.
     */
    private static int[] createReaderProjection( ParquetRelTable table, ParquetSchemaReader schemaReader, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters ) {
        LinkedHashSet<Integer> projected = new LinkedHashSet<>();
        for ( int field : fields ) {
            addProjectionField( table, schemaReader, projected, field );
        }
        addTablePathProjection( table, schemaReader, projected );
        addProjectionFields( table, schemaReader, projected, filters );
        addFallbackDataProjection( table, schemaReader, projected );
        return projected.stream().mapToInt( Integer::intValue ).toArray();
    }


    /**
     * Adds filters to projection.
     *
     * @param table a source table.
     * @param schemaReader a schema reader.
     * @param projected a projection.
     * @param filters a list of filters.
     */
    private static void addProjectionFields( ParquetRelTable table, ParquetSchemaReader schemaReader, LinkedHashSet<Integer> projected, List<ParquetAdapterFilter<PolyValue>> filters ) {
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            if ( filter.isLogical() ) {
                addProjectionFields( table, schemaReader, projected, filter.operands() );
            } else {
                addProjectionField( table, schemaReader, projected, filter.columnIndex() );
            }
        }
    }


    private static void addTablePathProjection( ParquetRelTable table, ParquetSchemaReader schemaReader, LinkedHashSet<Integer> projected ) {
        List<String> tablePath = table.getBinding().sourcePathElements();
        if ( tablePath.isEmpty() ) {
            return;
        }
        int parquetField = parquetFieldIndex( schemaReader, tablePath.get( 0 ) );
        if ( parquetField >= 0 ) {
            projected.add( parquetField );
        }
    }


    private static void addFallbackDataProjection( ParquetRelTable table, ParquetSchemaReader schemaReader, LinkedHashSet<Integer> projected ) {
        if ( !projected.isEmpty() ) {
            return;
        }
        for ( int field = 0; field < table.columns.size(); field++ ) {
            ParquetColumnBinding binding = table.getBinding().getColumnBinding( table.columns.get( field ).id );
            if ( binding == null || binding.role() != ParquetColumnRole.DATA || binding.sourcePathElements().size() != 1 ) {
                continue;
            }
            int parquetField = parquetFieldIndex( schemaReader, binding.sourcePathElements().get( 0 ) );
            if ( parquetField >= 0 ) {
                projected.add( parquetField );
                return;
            }
        }
        if ( schemaReader.getSchema().getFieldCount() > 0 ) {
            projected.add( 0 );
        }
    }


    /**
     * Adds a field to a projection.
     *
     * @param table a source table.
     * @param schemaReader a schema reader.
     * @param projected a projection.
     * @param field a field.
     */
    private static void addProjectionField( ParquetRelTable table, ParquetSchemaReader schemaReader, LinkedHashSet<Integer> projected, int field ) {
        if ( field < 0 || field >= table.columns.size() ) {
            return;
        }
        ParquetColumnBinding binding = table.getBinding().getColumnBinding( table.columns.get( field ).id );
        if ( binding == null || binding.sourcePathElements().isEmpty() || binding.role() != ParquetColumnRole.DATA ) {
            return;
        }
        int parquetField = parquetFieldIndex( schemaReader, binding.sourcePathElements().get( 0 ) );
        if ( parquetField >= 0 ) {
            projected.add( parquetField );
        }
    }


    /**
     * Gets physical parquet field index by the field name from parquet schema.
     *
     * @param schemaReader a schema reader.
     * @param fieldName a field name.
     * @return a physical parquet field index.
     */
    private static int parquetFieldIndex( ParquetSchemaReader schemaReader, String fieldName ) {
        for ( int i = 0; i < schemaReader.getSchema().getFieldCount(); i++ ) {
            if ( schemaReader.getSchema().getFieldName( i ).equals( fieldName ) ) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Creates a chain of source file filter evaluators. Currently, supports two filter evaluators:
     * 1. partition value based filter evaluator
     * 2. source file statistics based filter evaluator.
     * Those filter evaluators can skip files if they do not contain filtered data.
     *
     * @param selector a column binding selector.
     * @return an evaluators chain.
     */
    public static ParquetMultiFilterEvaluator<ParquetSourceFile> createParquetSourceFileEvaluatorsChain( Function<ParquetAdapterFilter<PolyValue>, ParquetColumnBinding> selector ) {
        return new ParquetMultiFilterEvaluator<>( List.of(
                new ParquetSourceFilePartitionFilterEvaluator( selector ),
                new ParquetSourceFileStatisticsFilterEvaluator( selector ) )
        );
    }


    /**
     * Selects a column binding according to a physical column index.
     *
     * @param table a table to look for a column binding in.
     * @param columnIndex a physical column index.
     * @return {@link ParquetColumnBinding}.
     */
    public static ParquetColumnBinding selectPhysicalBinding( ParquetRelTable table, int columnIndex ) {
        if ( columnIndex < 0 || columnIndex >= table.columns.size() ) {
            throw new GenericRuntimeException( "Invalid physical filter column index: " + columnIndex );
        }
        return table.getBinding().getColumnBinding( table.columns.get( columnIndex ).id );
    }


    /**
     * Checks if the specified filter is a partition filter.
     *
     * @param table a table containing the binding information.
     * @param filter a filter to check.
     * @return {@code true} if the provided filter is a partition filter and {@code false} otherwise.
     */
    protected static boolean isPartitionFilter( ParquetRelTable table, ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            return filter.operands().stream().allMatch( operand -> isPartitionFilter( table, operand ) );
        }
        return isPartitionColumn( table, filter.columnIndex() );
    }


    /**
     * Checks if the specified filter is a partition filter.
     *
     * @param table a table containing the binding information.
     * @param columnIndex a column index of the filter.
     * @return {@code true} if the provided filter is a partition filter and {@code false} otherwise.
     */
    protected static boolean isPartitionColumn( ParquetRelTable table, int columnIndex ) {
        if ( columnIndex < 0 || columnIndex >= table.columns.size() ) {
            return false;
        }
        ParquetColumnBinding columnBinding = table.getBinding().getColumnBinding( table.columns.get( columnIndex ).id );
        return columnBinding != null && columnBinding.role() == ParquetColumnRole.PARTITION;
    }


    /**
     * Gets a list of column bindings for the provided projected fields.
     *
     * @param table a table containing the binding information.
     * @param fields an array of field indexes.
     * @return a list of column bindings.
     */
    protected static List<ParquetColumnBinding> projectedBindings( ParquetRelTable table, int[] fields ) {
        ParquetTableBinding binding = table.getBinding();
        List<ParquetColumnBinding> selected = new ArrayList<>( fields.length );
        for ( int field : fields ) {
            selected.add( Objects.requireNonNull( binding.getColumnBinding( table.columns.get( field ).id ), "Missing parquet column binding" ) );
        }
        return selected;
    }


    /**
     * Checks if reading from the table requires binding information.
     * Binding information is required when:
     * 1. a field index points to a nested field.
     * 2. a field index is a logical index inside the projection and needs to be mapped to a physical parquet index.
     *
     * @param table a table to check.
     * @param schemaReader a schema reader pointing to the parquet file.
     * @param fields an array of field indexes.
     * @return {@code true} if the reading from the table requires binding information and {@code false} otherwise.
     */
    protected static boolean needsBindingScan( ParquetRelTable table, ParquetSchemaReader schemaReader, int[] fields ) {
        var schema = schemaReader.getSchema();
        for ( int field : fields ) {
            ParquetColumnBinding columnBinding = table.getBinding().getColumnBinding( table.columns.get( field ).id );
            if ( columnBinding == null ) {
                throw new GenericRuntimeException( "Missing parquet column binding for a field " + schema.getType( field ).getName() );
            }
            if ( columnBinding.sourcePathElements().isEmpty() ) {
                if ( columnBinding.role() != ParquetColumnRole.DATA ) {
                    return true;
                }
                throw new GenericRuntimeException( "Missing parquet column binding for a field " + schema.getType( field ).getName() );
            }
            if ( columnBinding.sourcePathElements().size() > 1 ) {
                return true;
            }
            if ( field >= schema.getFieldCount() || !schema.getType( field ).getName().equals( columnBinding.sourcePathElements().get( 0 ) ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Checks if a table is a nested table.
     *
     * @param table a table to check.
     * @return {@code true} if nested and {@code false} otherwise.
     */
    private static boolean isNestedTable( ParquetRelTable table ) {
        return table.getBinding().parentTableName() != null;
    }


    private record PrimitiveRowProjection( int[] outputIndexes, PolyValue[] constants ) {

    }


    /**
     * Registers adapter in the current transaction as an involved in execution adapter.
     *
     * @param dataContext a data context.
     */
    protected void registerAdapter( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
    }

}
