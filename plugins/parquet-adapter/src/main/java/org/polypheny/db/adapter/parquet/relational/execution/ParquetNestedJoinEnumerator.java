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
import org.apache.parquet.example.data.Group;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.execution.CombinedGroup;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.JoinFiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Executes parent/child joins for normalized nested Parquet tables in one pass over
 * the backing file.
 */
public class ParquetNestedJoinEnumerator extends ParquetNestedRepeatedRelEnumerator {

    private final ParquetTableBinding parentBinding;
    private final ParquetTableBinding childBinding;
    private final List<ParquetColumnBinding> parentColumns;
    private final List<ParquetColumnBinding> childColumns;
    private final List<ParquetAdapterFilter> parentFilters;
    private final List<ParquetAdapterFilter> childFilters;
    private final boolean leftIsParent;
    private final boolean emitUnmatchedParents;
    private final List<String> childPathFromParent;


    public ParquetNestedJoinEnumerator(
            Source source,
            AtomicBoolean cancelFlag,
            ParquetTableBinding parentBinding,
            ParquetTableBinding childBinding,
            List<ParquetColumnBinding> parentColumns,
            List<ParquetColumnBinding> childColumns,
            JoinFiltersContainer filterContainer,
            boolean leftIsParent,
            boolean emitUnmatchedParents ) {
        super( source, cancelFlag, parentBinding, parentColumns, filterContainer, true );
        this.parentBinding = parentBinding;
        this.childBinding = childBinding;
        this.parentColumns = List.copyOf( parentColumns );
        this.childColumns = List.copyOf( childColumns );
        this.parentFilters = filterContainer.parentFilters();
        this.childFilters = filterContainer.childFilters();
        this.leftIsParent = leftIsParent;
        this.emitUnmatchedParents = emitUnmatchedParents;
        this.childPathFromParent = childBinding.sourcePathElements().subList( parentBinding.sourcePathElements().size(), childBinding.sourcePathElements().size() );
    }


    @Override
    protected List<Group> expandRow( Group group ) {
        List<Group> rows = new ArrayList<>();
        VirtualGroup rootGroup = new VirtualGroup( group, String.valueOf( reader.getCurrentRowNumber() ), null, 0 );
        for ( Group parent : resolveNested( rootGroup, group.getType(), parentBinding.sourcePathElements(), 0 ) ) {
            if ( !matchesFilters( combinedGroup( (VirtualGroup) parent, null ), parentFilters ) ) {
                continue;
            }

            List<Group> childRows = resolveNested( (VirtualGroup) parent, parent.getType(), childPathFromParent, 0 );
            if ( childRows.isEmpty() && emitUnmatchedParents ) {
                if ( childFilters.isEmpty() ) {
                    rows.add( new CombinedGroup(
                            (VirtualGroup) parent,
                            parentColumns,
                            parentBinding.sourcePathElements(),
                            null,
                            childColumns,
                            childBinding.sourcePathElements() ) );
                }
                continue;
            }

            rows.addAll( childRows.stream()
                    .map( VirtualGroup.class::cast )
                    .map( child -> combinedGroup( (VirtualGroup) parent, child ) )
                    .filter( child -> matchesFilters( child, childFilters ) )
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
            row[i] = extractValueByColumnRole(
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
    @Override
    protected PolyValue extractValue( Group group, ParquetAdapterFilter filter ) {
        if ( group instanceof CombinedGroup combinedGroup ) {
            return extractValueByColumnRole(
                    combinedGroup.groupForField( filter.columnIndex(), leftIsParent ),
                    combinedGroup.bindingForField( filter.columnIndex(), leftIsParent ),
                    combinedGroup.tablePathForField( filter.columnIndex(), leftIsParent )
            );
        }
        // If this is not a combined group then the filtering is being done on parent table only.
        return extractValueByColumnRole(
                (VirtualGroup) group,
                parentColumns.get( filter.columnIndex() ),
                parentBinding.sourcePathElements()
        );
    }


    /**
     * Checks if a filter is valid and can be applied on the group.
     *
     * @param group a group to check
     * @param filter a filter to validate
     * @return true if the filter is valid and false otherwise.
     */
    @Override
    protected boolean canApplyFilter( Group group, ParquetAdapterFilter filter ) {
        if ( filter.polyValue() == null || filter.columnIndex() < 0 ) {
            return false;
        }
        if ( group instanceof CombinedGroup combinedGroup ) {
            return filter.columnIndex() < combinedGroup.fieldCount();
        }
        return filter.columnIndex() < parentColumns.size();
    }


    /**
     * Checks if the group contains a value.
     *
     * @param group a group to check.
     * @param filter a filter containing column index which value is checked
     * @return true if the group has a value at the filter column index and false otherwise.
     */
    @Override
    protected boolean filterHasValue( Group group, ParquetAdapterFilter filter ) {
        CombinedGroup combinedGroup = (CombinedGroup) group;
        return !combinedGroup.isNullField( filter.columnIndex(), leftIsParent );
    }


    private CombinedGroup combinedGroup( VirtualGroup parent, VirtualGroup child ) {
        return new CombinedGroup(
                parent,
                parentColumns,
                parentBinding.sourcePathElements(),
                child,
                childColumns,
                childBinding.sourcePathElements() );
    }


    private boolean matchesFilters( Group group, List<ParquetAdapterFilter> filters ) {
        return filters.stream().allMatch( filter -> matches( group, filter ) );
    }

}
