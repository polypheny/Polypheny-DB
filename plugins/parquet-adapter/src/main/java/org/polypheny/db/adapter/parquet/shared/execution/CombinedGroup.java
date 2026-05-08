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

package org.polypheny.db.adapter.parquet.shared.execution;

import java.util.List;
import lombok.Getter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;

@Getter
public class CombinedGroup extends Group {

    private final VirtualGroup parent;
    private final VirtualGroup child;
    private final List<ParquetColumnBinding> parentColumns;
    private final List<String> parentPath;
    private final List<ParquetColumnBinding> childColumns;
    private final List<String> childPath;


    public CombinedGroup(
            VirtualGroup parent,
            List<ParquetColumnBinding> parentColumns,
            List<String> parentPath,
            VirtualGroup child,
            List<ParquetColumnBinding> childColumns,
            List<String> childPath ) {
        this.parent = parent;
        this.child = child;
        this.parentColumns = List.copyOf( parentColumns );
        this.parentPath = List.copyOf( parentPath );
        this.childColumns = List.copyOf( childColumns );
        this.childPath = List.copyOf( childPath );
    }


    public int fieldCount() {
        return parentColumns.size() + childColumns.size();
    }


    public VirtualGroup groupForField( int fieldIndex, boolean leftIsParent ) {
        int parentFieldCount = parentColumns.size();
        if ( leftIsParent ) {
            return fieldIndex < parentFieldCount ? parent : child;
        }
        return fieldIndex < childColumns.size() ? child : parent;
    }


    public ParquetColumnBinding bindingForField( int fieldIndex, boolean leftIsParent ) {
        int parentFieldCount = parentColumns.size();
        int childFieldCount = childColumns.size();
        if ( leftIsParent ) {
            return fieldIndex < parentFieldCount ? parentColumns.get( fieldIndex ) : childColumns.get( fieldIndex - parentFieldCount );
        }
        return fieldIndex < childFieldCount ? childColumns.get( fieldIndex ) : parentColumns.get( fieldIndex - childFieldCount );
    }


    public List<String> tablePathForField( int fieldIndex, boolean leftIsParent ) {
        int parentFieldCount = parentColumns.size();
        int childFieldCount = childColumns.size();
        if ( leftIsParent ) {
            return fieldIndex < parentFieldCount ? parentPath : childPath;
        }
        return fieldIndex < childFieldCount ? childPath : parentPath;
    }


    public boolean isNullField( int fieldIndex, boolean leftIsParent ) {
        return groupForField( fieldIndex, leftIsParent ) == null;
    }


    @Override
    public void add( int fieldIndex, int value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, long value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, String value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, boolean value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, NanoTime value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, Binary value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, float value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, double value ) {
        throw unsupported();
    }


    @Override
    public void add( int fieldIndex, Group value ) {
        throw unsupported();
    }


    @Override
    public Group addGroup( int fieldIndex ) {
        throw unsupported();
    }


    @Override
    public int getFieldRepetitionCount( int fieldIndex ) {
        throw unsupported();
    }


    @Override
    public Group getGroup( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public String getString( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public int getInteger( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public long getLong( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public double getDouble( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public float getFloat( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public boolean getBoolean( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public Binary getBinary( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public Binary getInt96( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public String getValueToString( int fieldIndex, int index ) {
        throw unsupported();
    }


    @Override
    public GroupType getType() {
        return parent.getType();
    }


    @Override
    public void writeValue( int field, int index, RecordConsumer recordConsumer ) {
        throw unsupported();
    }


    private UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException( "CombinedGroup is a virtual joined row and does not expose Parquet field access by index." );
    }

}
