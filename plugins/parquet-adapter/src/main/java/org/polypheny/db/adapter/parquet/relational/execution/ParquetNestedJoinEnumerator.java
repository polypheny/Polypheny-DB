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
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.shared.execution.CombinedGroup;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetGroupFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetNestedFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetNestedJoinFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Executes parent/child joins for normalized nested Parquet tables in one pass over
 * the backing file.
 */
public class ParquetNestedJoinEnumerator extends ParquetNestedRepeatedRelEnumerator {

    private final JoinNestedBinding parentBinding;
    private final JoinNestedBinding childBinding;
    private final JoinFiltersContainer filterContainer;
    private final ParquetGroupFilterEvaluator parentScanFilterEvaluator;
    private final ParquetGroupFilterEvaluator childScanFilterEvaluator;
    private final boolean leftIsParent;
    private final boolean emitUnmatchedParents;
    private final List<String> childPathFromParent;


    public ParquetNestedJoinEnumerator(
            ParquetSourceReader reader,
            JoinNestedBinding parentBinding,
            JoinNestedBinding childBinding,
            JoinFiltersContainer filterContainer,
            boolean leftIsParent,
            boolean emitUnmatchedParents ) {
        this( reader, parentBinding, childBinding, filterContainer, leftIsParent, emitUnmatchedParents, true );
    }


    public ParquetNestedJoinEnumerator(
            ParquetSourceReader reader,
            JoinNestedBinding parentBinding,
            JoinNestedBinding childBinding,
            JoinFiltersContainer filterContainer,
            boolean leftIsParent,
            boolean emitUnmatchedParents,
            boolean readerOwner ) {
        super(
                reader,
                parentBinding.table(),
                parentBinding.columns(),
                filterContainer,
                new ParquetNestedJoinFilterEvaluator( reader.getProjectionSchema(), new ParquetPathValueExtractor(), parentBinding.table(), parentBinding.columns(), leftIsParent ),
                true,
                readerOwner
        );
        this.parentBinding = parentBinding;
        this.childBinding = childBinding;
        this.filterContainer = filterContainer;
        this.parentScanFilterEvaluator = new ParquetNestedFilterEvaluator( reader.getProjectionSchema(), new ParquetPathValueExtractor(), parentBinding.table().sourcePathElements(), parentBinding.filterColumns() );
        this.childScanFilterEvaluator = new ParquetNestedFilterEvaluator( reader.getProjectionSchema(), new ParquetPathValueExtractor(), childBinding.table().sourcePathElements(), childBinding.filterColumns() );
        this.leftIsParent = leftIsParent;
        this.emitUnmatchedParents = emitUnmatchedParents;
        this.childPathFromParent = childBinding.table().sourcePathElements().subList( parentBinding.table().sourcePathElements().size(), childBinding.table().sourcePathElements().size() );
    }


    @Override
    protected List<Group> expandRow( Group group ) {
        List<Group> rows = new ArrayList<>();
        VirtualGroup rootGroup = new VirtualGroup( group, String.valueOf( reader.getCurrentRowNumber() ), null, 0 );
        for ( Group parent : resolveNested( rootGroup, group.getType(), parentBinding.table().sourcePathElements(), 0 ) ) {
            if ( !matchesFilters( parent, filterContainer.parentScanFilters(), parentScanFilterEvaluator ) ) {
                continue;
            }
            if ( !matchesFilters( combinedGroup( (VirtualGroup) parent, null ), filterContainer.parentFilters(), filterEvaluator ) ) {
                continue;
            }

            List<Group> childRows = resolveNested( (VirtualGroup) parent, parent.getType(), childPathFromParent, 0 );
            if ( childRows.isEmpty() && emitUnmatchedParents ) {
                if ( filterContainer.childFilters().isEmpty() && filterContainer.childScanFilters().isEmpty() ) {
                    rows.add( new CombinedGroup(
                            (VirtualGroup) parent,
                            parentBinding.columns(),
                            parentBinding.table().sourcePathElements(),
                            null,
                            childBinding.columns(),
                            childBinding.table().sourcePathElements() ) );
                }
                continue;
            }

            rows.addAll( childRows.stream()
                    .map( VirtualGroup.class::cast )
                    .filter( child -> matchesFilters( child, filterContainer.childScanFilters(), childScanFilterEvaluator ) )
                    .map( child -> combinedGroup( (VirtualGroup) parent, child ) )
                    .filter( child -> matchesFilters( child, filterContainer.childFilters(), filterEvaluator ) )
                    .toList() );
        }
        return rows;
    }


    @Override
    protected PolyValue[] extractRow( Group group ) {
        CombinedGroup combinedGroup = (CombinedGroup) group;
        PolyValue[] row = new PolyValue[combinedGroup.fieldCount()];
        for ( int i = 0; i < row.length; i++ ) {
            if ( combinedGroup.isNullField( i, leftIsParent ) ) {
                row[i] = PolyNull.NULL;
                continue;
            }
            row[i] = pathValueExtractor().extractValue(
                    combinedGroup.groupForField( i, leftIsParent ),
                    combinedGroup.bindingForField( i, leftIsParent ),
                    combinedGroup.tablePathForField( i, leftIsParent )
            );
        }
        return row;
    }


    /**
     * Extracts value from the group for the provided filter.
     *
     * @param group - parquet group
     * @param filter - ParquetAdapterFilter
     * @return an extracted value
     */
    protected PolyValue extractValue( Group group, ParquetAdapterFilter<PolyValue> filter ) {
        if ( group instanceof CombinedGroup combinedGroup ) {
            return pathValueExtractor().extractValue(
                    combinedGroup.groupForField( filter.columnIndex(), leftIsParent ),
                    combinedGroup.bindingForField( filter.columnIndex(), leftIsParent ),
                    combinedGroup.tablePathForField( filter.columnIndex(), leftIsParent )
            );
        }
        // If this is not a combined group then the filtering is being done on parent table only.
        return pathValueExtractor().extractValue(
                (VirtualGroup) group,
                parentBinding.columns().get( filter.columnIndex() ),
                parentBinding.table().sourcePathElements()
        );
    }


    private CombinedGroup combinedGroup( VirtualGroup parent, VirtualGroup child ) {
        return new CombinedGroup(
                parent,
                parentBinding.columns(),
                parentBinding.table().sourcePathElements(),
                child,
                childBinding.columns(),
                childBinding.table().sourcePathElements() );
    }


    private boolean matchesFilters( Group group, List<ParquetAdapterFilter<PolyValue>> filters, ParquetGroupFilterEvaluator evaluator ) {
        return filters.stream().allMatch( filter -> evaluator.matches( group, filter ) );
    }


    private ParquetPathValueExtractor pathValueExtractor() {
        return (ParquetPathValueExtractor) valueExtractor();
    }

}
