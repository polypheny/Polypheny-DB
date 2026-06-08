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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetEnumerator;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetGroupFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetNestedFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Used for tables that do not have their own Parquet file, but are created from a nested group inside a Parquet file
 */
public class ParquetNestedRepeatedRelEnumerator extends AbstractParquetEnumerator {

    private final List<String> tablePath;
    private final List<ParquetColumnBinding> columnBindings;


    public ParquetNestedRepeatedRelEnumerator( ParquetSourceReader reader, ParquetTableBinding binding, List<ParquetColumnBinding> columnBindings, List<ParquetColumnBinding> filterColumnBindings, FiltersContainer filtersContainer ) {
        this( reader, binding, columnBindings, filtersContainer, new ParquetNestedFilterEvaluator( reader.getProjectionSchema(), new ParquetPathValueExtractor(), binding.sourcePathElements(), filterColumnBindings ), false, true );
    }


    protected ParquetNestedRepeatedRelEnumerator( ParquetSourceReader reader, ParquetTableBinding binding, List<ParquetColumnBinding> columnBindings, FiltersContainer filtersContainer, ParquetGroupFilterEvaluator filterEvaluator, boolean allowRootTablePath, boolean readerOwner ) {
        super( reader, filtersContainer, filterEvaluator, readerOwner );

        this.tablePath = binding.sourcePathElements();
        this.columnBindings = List.copyOf( columnBindings );
        if ( !allowRootTablePath && tablePath.isEmpty() ) {
            throw new GenericRuntimeException( "Nested parquet table binding does not contain a table source path." );
        }
    }


    /**
     * Recursive function that finds which nested Parquet groups should become rows for a repeated generated table
     * Follow tablePath inside the current Parquet row and return all nested group occurrences that represent rows of the virtual repeated table.
     *
     * @param group - parquet group/row
     * @return List<Group>
     */
    @Override
    protected List<Group> expandRow( Group group ) {
        var virtualGroup = new VirtualGroup( group, String.valueOf( reader.getCurrentRowNumber() ), null, 0 );
        return resolveNested( virtualGroup, group.getType(), tablePath, 0 );
    }


    @Override
    protected PolyValue[] extractRow( Group group ) {
        PolyValue[] row = new PolyValue[columnBindings.size()];
        for ( int i = 0; i < columnBindings.size(); i++ ) {
            row[i] = pathValueExtractor().extractValue( (VirtualGroup) group, columnBindings.get( i ), tablePath );
        }
        return row;
    }


    protected PolyValue extractValue( Group group, ParquetAdapterFilter<PolyValue> filter ) {
        var virtualGroup = (VirtualGroup) group;
        var binding = columnBindings.get( filter.columnIndex() );
        return pathValueExtractor().extractValue( virtualGroup, binding, tablePath );
    }


    protected List<Group> resolveNested( VirtualGroup virtualGroup, GroupType groupType, List<String> path, int pathIndex ) {
        if ( pathIndex >= path.size() ) {
            return List.of( virtualGroup );
        }

        int fieldIndex = valueExtractor().fieldIndex( groupType, path.get( pathIndex ) );
        if ( fieldIndex < 0 || virtualGroup.getFieldRepetitionCount( fieldIndex ) == 0 ) {
            return List.of();
        }

        Type fieldType = groupType.getType( fieldIndex );
        if ( fieldType.isPrimitive() ) {
            return List.of();
        }

        List<Group> groups = new ArrayList<>();
        int count = virtualGroup.getFieldRepetitionCount( fieldIndex );
        for ( int occurrence = 0; occurrence < count; occurrence++ ) {
            Group child = virtualGroup.getGroup( fieldIndex, occurrence );
            String childRowId = virtualGroup.getMetadata().getRowId() + "/" + path.get( pathIndex ) + "[" + occurrence + "]";
            VirtualGroup childGroup = new VirtualGroup( child, childRowId, virtualGroup.getMetadata().getRowId(), occurrence );
            groups.addAll( resolveNested( childGroup, fieldType.asGroupType(), path, pathIndex + 1 ) );
        }
        return groups;
    }


    private ParquetPathValueExtractor pathValueExtractor() {
        return (ParquetPathValueExtractor) valueExtractor();
    }

}
