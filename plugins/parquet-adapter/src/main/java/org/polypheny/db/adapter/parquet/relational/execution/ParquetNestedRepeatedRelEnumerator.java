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
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetEnumerator;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Source;

/**
 * Used for tables that do not have their own Parquet file, but are created from a nested group inside a Parquet file
 */
public class ParquetNestedRepeatedRelEnumerator extends AbstractParquetEnumerator {

    private final List<String> tablePath;
    private final List<ParquetColumnBinding> columnBindings;


    public ParquetNestedRepeatedRelEnumerator( Source source, AtomicBoolean cancelFlag, ParquetTableBinding binding, List<ParquetColumnBinding> columnBindings ) {
        this( source, cancelFlag, binding, columnBindings, List.of() );
    }


    public ParquetNestedRepeatedRelEnumerator( Source source, AtomicBoolean cancelFlag, ParquetTableBinding binding, List<ParquetColumnBinding> columnBindings, List<ParquetAdapterFilter> filters ) {
        super( source, cancelFlag, null, filters, new ParquetPathValueExtractor() );

        this.tablePath = binding.sourcePathElements();
        this.columnBindings = List.copyOf( columnBindings );
        if ( tablePath.isEmpty() ) {
            throw new GenericRuntimeException( "Nested parquet table binding does not contain a table source path." );
        }
    }


    /**
     *  Recursive function that finds which nested Parquet groups should become rows for a repeated generated table
     *  Follow tablePath inside the current Parquet row and return all nested group occurrences that represent rows of the virtual repeated table.
     * @param group - parquet group/row
     * @return List<Group>
     */
    @Override
    protected List<Group> expandRow( Group group ) {
        var virtualGroup = new VirtualGroup( group, String.valueOf( reader.getCurrentRowNumber() ), null, 0 );
        return resolveNested( virtualGroup, group.getType(), 0 );
    }


    @Override
    protected PolyValue[] extractRow( Group group ) {
        PolyValue[] row = new PolyValue[columnBindings.size()];
        for ( int i = 0; i < columnBindings.size(); i++ ) {
            row[i] = extractValueByColumnRole( (VirtualGroup) group, columnBindings.get( i ) );
        }
        return row;
    }


    @Override
    protected PolyValue extractValue( Group group, ParquetAdapterFilter filter ) {
        var virtualGroup = (VirtualGroup) group;
        var binding = columnBindings.get( filter.columnIndex() );
        return extractValueByColumnRole(virtualGroup, binding);
    }


    private List<Group> resolveNested( VirtualGroup virtualGroup, GroupType groupType, int pathIndex ) {
        if ( pathIndex >= tablePath.size() ) {
            return List.of( virtualGroup );
        }

        int fieldIndex = fieldIndex( groupType, tablePath.get( pathIndex ) );
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

            String childRowId = virtualGroup.getMetadata().getRowId() + "/" + tablePath.get( pathIndex ) + "[" + occurrence + "]";
            VirtualGroup childGroup = new VirtualGroup( child, childRowId, virtualGroup.getMetadata().getRowId(), occurrence );
            groups.addAll( resolveNested( childGroup, fieldType.asGroupType(), pathIndex + 1 ) );
        }
        return groups;
    }


    private PolyValue extractValueByColumnRole( VirtualGroup virtualGroup, ParquetColumnBinding binding ) {
        return switch ( binding.role() ) {
            case DATA -> {
                var path = binding.sourcePathElements();
                yield valueExtractor.extractValue( virtualGroup, path.subList( tablePath.size(), path.size() ) );
            }
            case PRIMARY_KEY -> PolyString.of( virtualGroup.getMetadata().getRowId() );
            case PARENT_KEY -> virtualGroup.getMetadata().getParentRowId() == null ? PolyNull.NULL : PolyString.of( virtualGroup.getMetadata().getParentRowId() );
            case ORDINAL -> PolyLong.of( virtualGroup.getMetadata().getOrdinal() );
        };
    }


    private int fieldIndex( GroupType groupType, String fieldName ) {
        for ( int i = 0; i < groupType.getFieldCount(); i++ ) {
            if ( groupType.getType( i ).getName().equals( fieldName ) ) {
                return i;
            }
        }
        return -1;
    }

}
