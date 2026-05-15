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
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.JoinNestedBinding;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetMultiFileEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedJoinEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedNonRepeatedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedRepeatedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetConvention;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.adapter.parquet.relational.planning.PhysicalScan;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersSplitter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetFilterResolver;
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
        final List<ParquetAdapterFilter> resolvedFilters = ParquetFilterResolver.resolveFilters( dataContext, parquetAdapterFilters, f -> binding.getColumnBinding( columns.get( f.columnIndex() ).id ) );
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
        ParquetConvention.INSTANCE.register( cluster.getPlanner() );
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
        final List<ParquetAdapterFilter> resolvedFilters = ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectPhysicalBinding( f.columnIndex() ) );
        final List<ParquetSourceFile> sourceFiles = new ParquetSourceFileFilterEvaluator( f -> selectPhysicalBinding( f.columnIndex() ) )
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
            final PhysicalScan leftScan,
            final PhysicalScan rightScan,
            final boolean leftIsParent,
            final boolean emitUnmatchedParents,
            final List<ParquetAdapterFilter> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final PhysicalScan parent = leftIsParent ? leftScan : rightScan;
        final PhysicalScan child = leftIsParent ? rightScan : leftScan;

        final List<ParquetAdapterFilter> leftScanFilters = leftScan.resolveFilters( dataContext );
        final List<ParquetAdapterFilter> rightScanFilters = rightScan.resolveFilters( dataContext );
        final JoinFiltersContainer filterContainer = getJoinFiltersContainer( dataContext, leftScan, rightScan, leftIsParent, filters, leftScanFilters, rightScanFilters, parent, child );

        final List<ParquetSourceFile> sourceFiles = new ParquetSourceFileFilterEvaluator(
                f -> parent.selectPhysicalBinding( f.columnIndex() )
        ).prune( parent.table().binding.sourceFiles(), leftIsParent ? leftScanFilters : rightScanFilters );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetMultiFileEnumerator(
                        sourceFiles,
                        sourceFile -> parent.table().nestedJoinEnumeratorForFile( sourceFile, child.table(), parent.fields(), child.fields(), cancelFlag, filterContainer, leftIsParent, emitUnmatchedParents ) );
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


    private boolean isNestedTable() {
        return binding.parentTableName() != null;
    }


    private JoinFiltersContainer getJoinFiltersContainer( DataContext dataContext, PhysicalScan leftScan, PhysicalScan rightScan, boolean leftIsParent, List<ParquetAdapterFilter> filters, List<ParquetAdapterFilter> leftScanFilters, List<ParquetAdapterFilter> rightScanFilters, PhysicalScan parent, PhysicalScan child ) {
        final List<ParquetAdapterFilter> joinFilters = ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectBinding( f.columnIndex(), leftScan.fields(), rightScan.fields(), rightScan.table() ) );
        final List<ParquetAdapterFilter> parentScanFilters = leftIsParent ? leftScanFilters : rightScanFilters;
        final List<ParquetAdapterFilter> childScanFilters = leftIsParent ? rightScanFilters : leftScanFilters;
        final JoinFiltersContainer container = new JoinFiltersSplitter().split( joinFilters, leftIsParent, parent.fields().length, child.fields().length );
        return new JoinFiltersContainer(
                container.parentFilters(),
                container.childFilters(),
                container.adapterFilters(),
                combine( container.nativeFilters(), parentScanFilters ),
                parentScanFilters,
                childScanFilters );
    }


    private Enumerator<PolyValue[]> enumeratorForFile( ParquetSourceFile sourceFile, int[] fields, AtomicBoolean cancelFlag, FiltersContainer filtersContainer ) {
        boolean bindingScan = isNestedTable() || needsBindingScan( fields ) || binding.sourceFiles().size() > 1 || !filtersContainer.adapterFilters().isEmpty();
        Source fileSource = sourceFile.asSource();
        ParquetSourceReader reader = new ParquetSourceReader( fileSource, cancelFlag, bindingScan ? null : fields, filtersContainer.nativeFilters() );
        List<ParquetColumnBinding> selectedBindings = projectedBindings( fields );
        List<ParquetColumnBinding> filterBindings = projectedBindings( fieldIndexes );
        if ( isNestedTable() ) {
            return new ParquetNestedRepeatedRelEnumerator( reader, binding, selectedBindings, filterBindings, filtersContainer );
        }
        if ( bindingScan ) {
            return new ParquetNestedNonRepeatedRelEnumerator( reader, sourceFile, selectedBindings, filterBindings, filtersContainer );
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
                new JoinNestedBinding( binding, projectedBindings( parentFields ), projectedBindings( fieldIndexes ) ),
                new JoinNestedBinding( child.binding, child.projectedBindings( childFields ), child.projectedBindings( child.fieldIndexes ) ),
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


    private ParquetColumnBinding selectPhysicalBinding( int columnIndex ) {
        if ( columnIndex < 0 || columnIndex >= columns.size() ) {
            throw new GenericRuntimeException( "Invalid physical filter column index: " + columnIndex );
        }
        return binding.getColumnBinding( columns.get( columnIndex ).id );
    }


    private static List<ParquetAdapterFilter> combine( List<ParquetAdapterFilter> left, List<ParquetAdapterFilter> right ) {
        if ( left.isEmpty() ) {
            return right;
        }
        if ( right.isEmpty() ) {
            return left;
        }
        List<ParquetAdapterFilter> combined = new ArrayList<>( left.size() + right.size() );
        combined.addAll( left );
        combined.addAll( right );
        return combined;
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

