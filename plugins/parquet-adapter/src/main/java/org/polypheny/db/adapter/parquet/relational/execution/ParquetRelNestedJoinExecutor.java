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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.planning.PhysicalScan;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetDataAggregateExecutor;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersSplitter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Executor used to run parquet enumerator for nested joins.
 */
public class ParquetRelNestedJoinExecutor extends ParquetRelExecutor {

    public ParquetRelNestedJoinExecutor( ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        super( table, parquetSource, fieldIndexes, schemaReader );
    }


    /**
     * Creates a {@link ParquetNestedJoinEnumerator} for a specified source file.
     *
     * @param parent a parent table participating in a join.
     * @param sourceFile a source file.
     * @param child a child nested table participating in a join.
     * @param parentFields a list of projected fields from the parent table.
     * @param childFields a list of projected fields from the child table.
     * @param cancelFlag indicates if the execution has been canceled and should not continue.
     * @param filterContainer a container of filters.
     * @param leftIsParent indicates if the parent table is on the left side of the join or not.
     * @param emitUnmatchedParents indicates if the parent row should be emitted if there is no child row to be joined with.
     * @return a new instance of {@link ParquetNestedJoinEnumerator}.
     */
    private static Enumerator<PolyValue[]> nestedJoinEnumeratorForFile(
            ParquetRelTable parent,
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
                new JoinNestedBinding( parent.getBinding(), projectedBindings( parent, parentFields ), projectedBindings( parent, allFields( parent ) ) ),
                new JoinNestedBinding( child.getBinding(), projectedBindings( child, childFields ), projectedBindings( child, allFields( child ) ) ),
                filterContainer,
                leftIsParent,
                emitUnmatchedParents );
    }


    /**
     * Combines two list of filters into a single filters list.
     *
     * @param left a list of filters from the left table.
     * @param right a list of filters from the right table.
     * @return a combined list of filters.
     */
    private static List<ParquetAdapterFilter<PolyValue>> combine( List<ParquetAdapterFilter<PolyValue>> left, List<ParquetAdapterFilter<PolyValue>> right ) {
        if ( left.isEmpty() ) {
            return right;
        }
        if ( right.isEmpty() ) {
            return left;
        }
        List<ParquetAdapterFilter<PolyValue>> combined = new ArrayList<>( left.size() + right.size() );
        combined.addAll( left );
        combined.addAll( right );
        return combined;
    }


    /**
     * Gets a correct {@link ParquetColumnBinding} from left or from right table according to the provided index.
     *
     * @param joinedIndex a column index to search. If the column index is within the left table fields count then the column binding from the left table is returned and from the right table otherwise.
     * @param leftFields an array of left table field indexes.
     * @param rightFields an array of right table field indexes.
     * @param left a left table.
     * @param right a right table.
     * @return {@link ParquetColumnBinding}.
     */
    private static ParquetColumnBinding selectBinding( int joinedIndex, int[] leftFields, int[] rightFields, ParquetRelTable left, ParquetRelTable right ) {
        if ( joinedIndex < 0 ) {
            throw new GenericRuntimeException( "Invalid joined filter column index: " + joinedIndex );
        }
        if ( joinedIndex < leftFields.length ) {
            return selectPhysicalBinding( left, leftFields[joinedIndex] );
        }
        int rightIndex = joinedIndex - leftFields.length;
        if ( rightIndex >= rightFields.length ) {
            throw new GenericRuntimeException( "Invalid joined filter column index: " + joinedIndex );
        }
        return selectPhysicalBinding( right, rightFields[rightIndex] );
    }


    /**
     * Creates an array of field indexes for all fields in the provided table.
     *
     * @param table a table.
     * @return an array of field indexes that correspond to the number of field count in the provided table starting from 0.
     */
    private static int[] allFields( ParquetRelTable table ) {
        return IntStream.range( 0, table.getFieldCount() ).toArray();
    }


    /**
     * Creates an enumerator to go through the nested join.
     *
     * @param dataContext a data context.
     * @param leftScan a left scan under the join.
     * @param rightScan a right scan under the join.
     * @param leftIsParent indicates if the left table in the join is a parent table.
     * @param emitUnmatchedParents indicates if the parent row needs to be emitted even though there is no corresponding child row.
     * @param filters a list of filters to be applied on both tables.
     * @return {@link ParquetMultiFileEnumerator}.
     */
    public Enumerable<PolyValue[]> createEnumerator( DataContext dataContext, PhysicalScan leftScan, PhysicalScan rightScan, boolean leftIsParent, boolean emitUnmatchedParents, List<ParquetAdapterFilter<PolyValue>> filters ) {
        registerAdapter( dataContext );

        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
                final List<ParquetAdapterFilter<PolyValue>> leftScanFilters = leftScan.resolveFilters( dataContext );
                final List<ParquetAdapterFilter<PolyValue>> rightScanFilters = rightScan.resolveFilters( dataContext );
                final JoinFiltersContainer filterContainer = buildFiltersContainer( dataContext, leftScan, rightScan, leftIsParent, filters, leftScanFilters, rightScanFilters );
                final PhysicalScan parent = leftIsParent ? leftScan : rightScan;
                final PhysicalScan child = leftIsParent ? rightScan : leftScan;
                final List<ParquetAdapterFilter<PolyValue>> parentFileFilters = leftIsParent ? leftScanFilters : rightScanFilters;
                return new ParquetMultiFileEnumerator(
                        parent.table().getBinding().sourceFiles(),
                        sourceFile -> nestedJoinEnumeratorForFile( parent.table(), sourceFile, child.table(), parent.fields(), child.fields(), cancelFlag, filterContainer, leftIsParent, emitUnmatchedParents ),
                        ParquetDataAggregateExecutor.createParquetSourceFileEvaluatorsChain( f -> parent.selectPhysicalBinding( f.columnIndex() ) ),
                        parentFileFilters );
            }
        };
    }


    /**
     * Creates a {@link JoinFiltersContainer}.
     * JoinFiltersContainer consists of multiple lists of filters:
     * 1. parent filters - these are join level filters that needs to be applied on the parent table. Filter.ColumnIndex in those filters is relative to the selected projection.
     * 2. child filters - these are join level filters that needs to be applied on the child table. Filter.ColumnIndex in those filters is relative to the selected projection.
     * 3. parent scan filters - these are parent scan level filters. The main reason for separation is that scan level filters must be related to physical column indexes and not to projection - filter column might be not a part of a projection.
     * 4. child scan filters - these are parent scan level filters. The main reason for separation is that scan level filters must be related to physical column indexes and not to projection - filter column might be not a part of a projection.
     * 5. adapter filters - these are filters that can be executed only on adapter level. Those filters are inherited from FiltersContainer class and not being used in JoinFiltersContainer - instead parent and child filters are used.
     * 6. reader filters - these are filters that can be pushed down into parquet reader.
     *
     * @param dataContext a data context.
     * @param leftScan a left scan under the join.
     * @param rightScan a right scan under the join.
     * @param leftIsParent indicates if the left table in the join is a parent table.
     * @param filters a list of join level filters. Those filters eventually will be split into parent and child filters.
     * @param leftScanFilters a list of left scan filters.
     * @param rightScanFilters a list of right scan filters.
     * @return newly created {@link JoinFiltersContainer}.
     */
    private JoinFiltersContainer buildFiltersContainer( DataContext dataContext, PhysicalScan leftScan, PhysicalScan rightScan, boolean leftIsParent, List<ParquetAdapterFilter<PolyValue>> filters, List<ParquetAdapterFilter<PolyValue>> leftScanFilters, List<ParquetAdapterFilter<PolyValue>> rightScanFilters ) {
        final PhysicalScan parent = leftIsParent ? leftScan : rightScan;
        final PhysicalScan child = leftIsParent ? rightScan : leftScan;
        final List<ParquetAdapterFilter<PolyValue>> joinFilters = ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectBinding( f.columnIndex(), leftScan.fields(), rightScan.fields(), leftScan.table(), rightScan.table() ) );
        final List<ParquetAdapterFilter<PolyValue>> parentScanFilters = leftIsParent ? leftScanFilters : rightScanFilters;
        final List<ParquetAdapterFilter<PolyValue>> childScanFilters = leftIsParent ? rightScanFilters : leftScanFilters;
        final JoinFiltersContainer container = new JoinFiltersSplitter().split( joinFilters, leftIsParent, parent.fields().length, child.fields().length );
        return new JoinFiltersContainer(
                container.parentFilters(),
                container.childFilters(),
                container.adapterFilters(),
                combine( container.nativeFilters(), parentScanFilters ),
                parentScanFilters,
                childScanFilters );
    }

}
