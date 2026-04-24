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

import lombok.Getter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;

/**
 * Class contains metadata information for virtual table rows
 * created from nested fields
 */
@Getter
public class VirtualGroup extends Group {

    private final Group source;
    private final GroupMetadata metadata;

    public VirtualGroup( Group source, String rowId, String parentRowId, long ordinal) {
        this.source = source;
        this.metadata = new GroupMetadata(rowId, parentRowId, ordinal);
    }

    // region Group methods delegation
    @Override
    public void add( int fieldIndex, int value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, long value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, String value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, boolean value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, NanoTime value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, Binary value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, float value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, double value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public void add( int fieldIndex, Group value ) {
        source.add( fieldIndex, value );
    }


    @Override
    public Group addGroup( int fieldIndex ) {
        return source.addGroup( fieldIndex );
    }


    @Override
    public int getFieldRepetitionCount( int fieldIndex ) {
        return source.getFieldRepetitionCount( fieldIndex );
    }


    @Override
    public Group getGroup( int fieldIndex, int index ) {
        return source.getGroup( fieldIndex, index );
    }


    @Override
    public String getString( int fieldIndex, int index ) {
        return source.getString( fieldIndex, index );
    }


    @Override
    public int getInteger( int fieldIndex, int index ) {
        return source.getInteger( fieldIndex, index );
    }


    @Override
    public long getLong( int fieldIndex, int index ) {
        return source.getLong( fieldIndex, index );
    }


    @Override
    public double getDouble( int fieldIndex, int index ) {
        return source.getDouble( fieldIndex, index );
    }


    @Override
    public float getFloat( int fieldIndex, int index ) {
        return source.getFloat( fieldIndex, index );
    }


    @Override
    public boolean getBoolean( int fieldIndex, int index ) {
        return source.getBoolean( fieldIndex, index );
    }


    @Override
    public Binary getBinary( int fieldIndex, int index ) {
        return source.getBinary( fieldIndex, index );
    }


    @Override
    public Binary getInt96( int fieldIndex, int index ) {
        return source.getInt96( fieldIndex, index );
    }


    @Override
    public String getValueToString( int fieldIndex, int index ) {
        return source.getValueToString( fieldIndex, index );
    }


    @Override
    public GroupType getType() {
        return source.getType();
    }


    @Override
    public void writeValue( int field, int index, RecordConsumer recordConsumer ) {
        source.writeValue( field, index, recordConsumer );
    }
    // endregion
}
