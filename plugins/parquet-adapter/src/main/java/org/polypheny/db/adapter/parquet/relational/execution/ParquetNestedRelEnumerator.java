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

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 *  Used for tables that do not have their own Parquet file, but are created from a nested group inside a Parquet file
 */
public class ParquetNestedRelEnumerator implements Enumerator<PolyValue[]> {

    private final ParquetSourceReader reader;
    private final List<String> tablePath;
    private final List<ParquetColumnBinding> columnBindings;
    private final ParquetRelValueExtractor valueExtractor = new ParquetRelValueExtractor();
    private final Queue<PolyValue[]> rows = new ArrayDeque<>();
    private PolyValue[] current;


    public ParquetNestedRelEnumerator( Source source, AtomicBoolean cancelFlag, ParquetTableBinding binding, List<ParquetColumnBinding> columnBindings ) {
        this.reader = new ParquetSourceReader( source, cancelFlag, null, List.of() );
        this.tablePath = binding.sourcePathElements();
        this.columnBindings = List.copyOf( columnBindings );
        if ( tablePath.isEmpty() ) {
            throw new GenericRuntimeException( "Nested parquet table binding does not contain a table source path." );
        }
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            while ( rows.isEmpty() ) {
                Group root = reader.next();
                if ( root == null ) {
                    current = null;
                    return false;
                }
                enqueueNestedRows( root );
            }
            current = rows.remove();
            return true;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading nested parquet data", e );
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        try {
            reader.close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }


    private void enqueueNestedRows( Group root ) {
        for ( Group nested : resolveTableGroups( root, root.getType(), 0 ) ) {
            PolyValue[] row = new PolyValue[columnBindings.size()];
            for ( int i = 0; i < columnBindings.size(); i++ ) {
                row[i] = extractNestedValue( nested, columnBindings.get( i ).sourcePathElements() );
            }
            rows.add( row );
        }
    }


    private PolyValue extractNestedValue( Group nested, List<String> sourcePath ) {
        if ( sourcePath.size() < tablePath.size() || !sourcePath.subList( 0, tablePath.size() ).equals( tablePath ) ) {
            return PolyNull.NULL;
        }

        Group currentGroup = nested;
        GroupType currentType = nested.getType();
        for ( int i = tablePath.size(); i < sourcePath.size(); i++ ) {
            int index = fieldIndex( currentType, sourcePath.get( i ) );
            if ( index < 0 || currentGroup.getFieldRepetitionCount( index ) == 0 ) {
                return PolyNull.NULL;
            }

            Type fieldType = currentType.getType( index );
            if ( i == sourcePath.size() - 1 ) {
                return valueExtractor.extractValue( currentGroup, index, fieldType );
            }

            if ( fieldType.isPrimitive() ) {
                return PolyNull.NULL;
            }
            currentGroup = currentGroup.getGroup( index, 0 );
            currentType = fieldType.asGroupType();
        }
        return PolyNull.NULL;
    }


    private List<Group> resolveTableGroups( Group group, GroupType groupType, int pathIndex ) {
        if ( pathIndex >= tablePath.size() ) {
            return List.of( group );
        }

        int fieldIndex = fieldIndex( groupType, tablePath.get( pathIndex ) );
        if ( fieldIndex < 0 || group.getFieldRepetitionCount( fieldIndex ) == 0 ) {
            return List.of();
        }

        Type fieldType = groupType.getType( fieldIndex );
        if ( fieldType.isPrimitive() ) {
            return List.of();
        }

        List<Group> groups = new java.util.ArrayList<>();
        int count = group.getFieldRepetitionCount( fieldIndex );
        for ( int occurrence = 0; occurrence < count; occurrence++ ) {
            Group child = group.getGroup( fieldIndex, occurrence );
            groups.addAll( resolveTableGroups( child, fieldType.asGroupType(), pathIndex + 1 ) );
        }
        return groups;
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
