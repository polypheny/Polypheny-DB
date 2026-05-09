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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetMultiFileEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedJoinEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedNonRepeatedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedRepeatedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersSplitter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.adapter.parquet.shared.statistics.ParquetStatisticsReader;
import org.polypheny.db.adapter.statistics.AdapterStatisticsProvider;
import org.polypheny.db.adapter.statistics.ProvidedColumnStatistics;
import org.polypheny.db.adapter.statistics.ProvidedEntityStatistics;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Physical table wrapper for the relational model.
 * Exposes the Parquet-backed table to Polypheny and ties the planner,
 * scanner, and adapter metadata together.
 */
public class ParquetRelTable extends PhysicalTable implements FilterableEntity, ScannableEntity, TranslatableEntity, AdapterStatisticsProvider {

    private final int[] fieldIndexes;
    private final AbstractParquetSource parquetSource;
    private final ParquetRelFilterTranslator filterTranslator;
    @Getter
    private final ParquetTableBinding binding;
    private final List<PolyType> fieldTypes;
    private final ParquetSchemaReader schemaReader;
    private final ParquetStatisticsReader statisticsReader;


    /**
     * Creates a Parquet table wrapper from a physical table definition and source binding.
     */
    public ParquetRelTable( long id, PhysicalTable table, ParquetTableBinding binding, AbstractParquetSource parquetSource ) {
        super(
                id,
                table.allocationId,
                table.logicalId,
                table.name,
                table.columns,
                table.namespaceId,
                table.namespaceName,
                table.uniqueFieldIds,
                table.adapterId );
        this.binding = binding;
        this.fieldIndexes = IntStream.range( 0, table.columns.size() ).toArray();
        this.fieldTypes = columns.stream().map( c -> c.type ).toList();
        this.schemaReader = new ParquetSchemaReader( binding.sourceFiles().stream().map( ParquetSourceFile::asSource ).toList() );
        this.statisticsReader = new ParquetStatisticsReader( schemaReader, binding );
        this.parquetSource = parquetSource;
        this.filterTranslator = new ParquetRelFilterTranslator();
    }

    // Statics provider interface functions


    /**
     * Gets table statistics
     *
     * @param logicalEntityId - logical table id
     * @return statistics from parquet file
     */
    @Override
    public Optional<ProvidedEntityStatistics> getEntityStatistics( long logicalEntityId ) {
        if ( logicalId != logicalEntityId ) {
            return Optional.empty();
        }
        return statisticsReader.getEntityStatistics( isNestedTable() );
    }


    /**
     * Gets column statistics (range values for example)
     *
     * @param column - logical column
     * @param uniqueValueLimit - limit
     * @return column statistics calculated from parquet file
     */
    @Override
    public Optional<ProvidedColumnStatistics> getColumnStatistics( LogicalColumn column, int uniqueValueLimit ) {
        if ( logicalId != column.tableId ) {
            return Optional.empty();
        }
        return statisticsReader.getColumnStatistics( column, uniqueValueLimit );
    }

    // endregion


    /**
     * Returns enumerable for FilterableEntity.
     *
     * @param dataContext data context
     * @param polyFilters polyFilters to push down.
     * @return enumerable.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext, List<RexNode> polyFilters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final List<ParquetAdapterFilter> parquetAdapterFilters = new ArrayList<>();
        polyFilters.removeIf( polyFilter -> {
            var parquetFilter = filterTranslator.translate( fieldTypes, polyFilter );
            if ( parquetFilter != null ) {
                return parquetAdapterFilters.add( parquetFilter );
            }
            return false;
        } );

        // check for dynamic filters
        final List<ParquetAdapterFilter> resolvedFilters = resolveFilters( dataContext, parquetAdapterFilters, f -> binding.getColumnBinding( columns.get( f.columnIndex() ).id ) );
        final List<ParquetSourceFile> sourceFiles = new ParquetSourceFileFilterEvaluator( f -> binding.getColumnBinding( columns.get( f.columnIndex() ).id ) )
                .prune( binding.sourceFiles(), resolvedFilters );

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetMultiFileEnumerator(
                        sourceFiles,
                        sourceFile -> enumeratorForFile( sourceFile, fieldIndexes, cancelFlag, FiltersContainer.shared( resolvedFilters ) ) );
            }
        };
    }


    /**
     * Returns enumerable for ScannableEntity.
     *
     * @param dataContext data context
     * @return enumerable.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        // create parquet enumerator
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetMultiFileEnumerator(
                        binding.sourceFiles(),
                        sourceFile -> enumeratorForFile( sourceFile, fieldIndexes, cancelFlag, FiltersContainer.empty ) );
            }
        };
    }


    /**
     * Returns {@link AlgNode} as part of TranslatableEntity.
     *
     * @param cluster cluster
     * @param traitSet trial set.
     * @return {@link AlgNode}
     */
    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new ParquetRelScan( cluster, this, fieldIndexes );
    }


    /**
     * This method is called from the {@link ParquetRelScan} via reflection.
     *
     * @param dataContext data context
     * @param fields a list of fields to return.
     * @return enumerable.
     */
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        return project( dataContext, fields, List.of() );
    }


    /**
     * This method is called from the {@link ParquetRelScan} via reflection.
     *
     * @param dataContext data context
     * @param fields a list of fields to return.
     * @param filters filters to push down.
     * @return enumerable.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields, final List<ParquetAdapterFilter> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final List<ParquetAdapterFilter> resolvedFilters = resolveFilters( dataContext, filters, f -> selectProjectedBinding( f.columnIndex(), fields ) );
        final List<ParquetSourceFile> sourceFiles = new ParquetSourceFileFilterEvaluator( f -> selectProjectedBinding( f.columnIndex(), fields ) )
                .prune( binding.sourceFiles(), resolvedFilters );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetMultiFileEnumerator(
                        sourceFiles,
                        sourceFile -> enumeratorForFile( sourceFile, fields, cancelFlag, FiltersContainer.shared( resolvedFilters ) ) );
            }
        };
    }


    /**
     * Executes a supported parent/child join inside the Parquet adapter.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> nestedJoin(
            final DataContext dataContext,
            final ParquetRelTable right,
            final int[] leftFields,
            final int[] rightFields,
            final boolean leftIsParent,
            final boolean emitUnmatchedParents,
            final List<ParquetAdapterFilter> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final ParquetRelTable parent = leftIsParent ? this : right;
        final ParquetRelTable child = leftIsParent ? right : this;
        final int[] parentFields = leftIsParent ? leftFields : rightFields;
        final int[] childFields = leftIsParent ? rightFields : leftFields;
        final List<ParquetAdapterFilter> resolvedFilters = resolveFilters( dataContext, filters, f -> selectBinding( f.columnIndex(), leftFields, rightFields, right ) );
        final List<ParquetSourceFile> sourceFiles = new ParquetSourceFileFilterEvaluator( f -> selectParentBinding( f.columnIndex(), leftIsParent, parentFields, childFields, parent ) )
                .prune( parent.binding.sourceFiles(), filtersForParent( resolvedFilters, leftIsParent, parentFields.length, childFields.length ) );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                var filterContainer = new JoinFiltersSplitter().split( resolvedFilters, leftIsParent, parentFields.length, childFields.length );
                return new ParquetMultiFileEnumerator(
                        sourceFiles,
                        sourceFile -> parent.nestedJoinEnumeratorForFile( sourceFile, child, parentFields, childFields, cancelFlag, filterContainer, leftIsParent, emitUnmatchedParents ) );
            }
        };
    }


    public int columnIndexByRole( ParquetColumnRole role ) {
        for ( int i = 0; i < columns.size(); i++ ) {
            ParquetColumnBinding columnBinding = binding.getColumnBinding( columns.get( i ).id );
            if ( columnBinding != null && columnBinding.role() == role ) {
                return i;
            }
        }
        return -1;
    }


    public String getSourceUrl() {
        return binding.sourceFiles().stream().map( ParquetSourceFile::fileUrl ).sorted().toList().toString();
    }


    public int getFieldCount() {
        return columns.size();
    }


    /**
     * Support parametrized queries
     *
     * @param dataContext context
     * @param filters filters
     * @param selector a helper function for column binding selection
     * @return list of parquet filters
     */
    private List<ParquetAdapterFilter> resolveFilters(
            DataContext dataContext,
            List<ParquetAdapterFilter> filters,
            Function<ParquetAdapterFilter, ParquetColumnBinding> selector ) {
        List<ParquetAdapterFilter> resolved = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter filter : filters ) {
            resolved.add( resolveFilter( dataContext, filter, selector ) );
        }
        return resolved;
    }


    private ParquetAdapterFilter resolveFilter( DataContext dataContext, ParquetAdapterFilter filter, Function<ParquetAdapterFilter, ParquetColumnBinding> selector ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical( filter.operator(), filter.operands().stream()
                    .map( operand -> resolveFilter( dataContext, operand, selector ) )
                    .toList() );
        }

        PolyValue value = filter.dynamicParamIndex() == null
                ? filter.polyValue()
                : dataContext.getParameterValue( filter.dynamicParamIndex() );

        ParquetColumnBinding columnBinding = Objects.requireNonNull( selector.apply( filter ), "Missing parquet column binding" );
        return new ParquetAdapterFilter( filter.columnIndex(), columnBinding.sourcePathElements(), filter.operator(), value );
    }


    private List<ParquetAdapterFilter> filtersForParent( List<ParquetAdapterFilter> filters, boolean leftIsParent, int parentFieldCount, int childFieldCount ) {
        var splitFilters = new JoinFiltersSplitter().split( filters, leftIsParent, parentFieldCount, childFieldCount );
        return splitFilters.parentFilters();
    }


    private boolean isNestedTable() {
        return binding.parentTableName() != null;
    }


    private Enumerator<PolyValue[]> enumeratorForFile( ParquetSourceFile sourceFile, int[] fields, AtomicBoolean cancelFlag, FiltersContainer filtersContainer ) {
        boolean bindingScan = isNestedTable() || needsBindingScan( fields ) || binding.sourceFiles().size() > 1;
        Source fileSource = sourceFile.asSource();
        ParquetSourceReader reader = new ParquetSourceReader( fileSource, cancelFlag, bindingScan ? null : fields, filtersContainer.nativeFilters() );
        if ( isNestedTable() ) {
            return new ParquetNestedRepeatedRelEnumerator( reader, binding, projectedBindings( fields ), filtersContainer );
        }
        if ( bindingScan ) {
            return new ParquetNestedNonRepeatedRelEnumerator( reader, sourceFile, projectedBindings( fields ), filtersContainer );
        }
        return new ParquetRelEnumerator( reader, filtersContainer.withoutPathElementsInAdapterFilters() );
    }


    private Enumerator<PolyValue[]> nestedJoinEnumeratorForFile(
            ParquetSourceFile sourceFile,
            ParquetRelTable child,
            int[] parentFields,
            int[] childFields,
            AtomicBoolean cancelFlag,
            JoinFiltersContainer filterContainer,
            boolean leftIsParent,
            boolean emitUnmatchedParents ) {

        ParquetSourceReader reader = new ParquetSourceReader( sourceFile.asSource(), cancelFlag, null, filterContainer.nativeFilters() );
        return new ParquetNestedJoinEnumerator(
                reader,
                binding,
                child.binding,
                projectedBindings( parentFields ),
                child.projectedBindings( childFields ),
                filterContainer,
                leftIsParent,
                emitUnmatchedParents );
    }


    /**
     * Selects a column binding for a join. If the column index is less then left table fields count then use left table column binding otherwise use right table column biding.
     *
     * @param joinedIndex a column index
     * @param leftFields a list of left column fields
     * @param rightFields a list of right column fields
     * @param right a right table
     * @return {@link ParquetColumnBinding}
     */
    private ParquetColumnBinding selectBinding( int joinedIndex, int[] leftFields, int[] rightFields, ParquetRelTable right ) {
        if ( joinedIndex < 0 ) {
            throw new GenericRuntimeException( "Invalid joined filter column index: " + joinedIndex );
        }
        if ( joinedIndex < leftFields.length ) {
            return binding.getColumnBinding( columns.get( leftFields[joinedIndex] ).id );
        }
        int rightIndex = joinedIndex - leftFields.length;
        if ( rightIndex >= rightFields.length ) {
            throw new GenericRuntimeException( "Invalid joined filter column index: " + joinedIndex );
        }
        return right.binding.getColumnBinding( right.columns.get( rightFields[rightIndex] ).id );
    }


    private ParquetColumnBinding selectParentBinding( int joinedIndex, boolean leftIsParent, int[] parentFields, int[] childFields, ParquetRelTable parent ) {
        if ( joinedIndex < 0 ) {
            throw new GenericRuntimeException( "Invalid joined filter column index: " + joinedIndex );
        }
        int parentIndex = leftIsParent ? joinedIndex : joinedIndex - childFields.length;
        if ( parentIndex < 0 || parentIndex >= parentFields.length ) {
            return null;
        }
        return parent.binding.getColumnBinding( parent.columns.get( parentFields[parentIndex] ).id );
    }


    private ParquetColumnBinding selectProjectedBinding( int projectedIndex, int[] fields ) {
        if ( projectedIndex < 0 || projectedIndex >= fields.length ) {
            throw new GenericRuntimeException( "Invalid projected filter column index: " + projectedIndex );
        }
        return binding.getColumnBinding( columns.get( fields[projectedIndex] ).id );
    }


    /**
     * Gets a list of bindings per provided fields
     *
     * @param fields a list of fields to get the bindings for
     * @return a list of column bindings
     */
    private List<ParquetColumnBinding> projectedBindings( int[] fields ) {
        List<ParquetColumnBinding> selected = new ArrayList<>( fields.length );
        for ( int field : fields ) {
            selected.add( Objects.requireNonNull( binding.getColumnBinding( columns.get( field ).id ), "Missing parquet column binding" ) );
        }
        return selected;
    }


    private boolean needsBindingScan( int[] fields ) {
        for ( int field : fields ) {
            ParquetColumnBinding columnBinding = binding.getColumnBinding( columns.get( field ).id );
            if ( columnBinding == null || columnBinding.sourcePathElements().isEmpty() ) {
                return true;
            }
            if ( columnBinding.sourcePathElements().size() > 1 ) {
                return true;
            }
            var schema = schemaReader.getSchema();
            if ( field >= schema.getFieldCount() || !schema.getType( field ).getName().equals( columnBinding.sourcePathElements().get( 0 ) ) ) {
                return true;
            }
        }
        return false;
    }

}

