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

package org.polypheny.db.adapter.parquet.shared.aggregate;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Maps one input column to one or more aggregate indexes and reads/adds that column for a row.
 */
public class AggregateInputColumn {

    private final int columnIndex;
    private final int maxDefinitionLevel;
    private final boolean required;
    private final PrimitiveType.PrimitiveTypeName type;
    private final int singleCountIndex;
    private final int[] countIndexes;
    private final int singleDoubleIndex;
    private final int[] doubleIndexes;
    private final int singleMinIndex;
    private final int[] minIndexes;
    private final int singleMaxIndex;
    private final int[] maxIndexes;


    public AggregateInputColumn( int columnIndex, int[] countIndexes, int[] doubleIndexes, int[] minIndexes, int[] maxIndexes, int maxDefinitionLevel, PrimitiveTypeName type ) {
        this.columnIndex = columnIndex;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.required = maxDefinitionLevel == 0;
        this.type = type;
        this.singleCountIndex = AggregateRowAccumulator.singleIndex( countIndexes );
        this.countIndexes = AggregateRowAccumulator.additionalIndexes( countIndexes );
        this.singleDoubleIndex = AggregateRowAccumulator.singleIndex( doubleIndexes );
        this.doubleIndexes = AggregateRowAccumulator.additionalIndexes( doubleIndexes );
        this.singleMinIndex = AggregateRowAccumulator.singleIndex( minIndexes );
        this.minIndexes = AggregateRowAccumulator.additionalIndexes( minIndexes );
        this.singleMaxIndex = AggregateRowAccumulator.singleIndex( maxIndexes );
        this.maxIndexes = AggregateRowAccumulator.additionalIndexes( maxIndexes );
    }


    public void add( AggregateGroupState values, ColumnReader[] readers ) {
        ColumnReader reader = readers[columnIndex];
        if ( required || reader.getCurrentDefinitionLevel() == maxDefinitionLevel ) {
            double value = doubleValue( reader, type );
            addPresentValue( values, value );
        }
        reader.consume();
    }


    public void add( AggregateGroupState values, ColumnReader[] readers, boolean[] present, double[] numericValues ) {
        read( readers, present, numericValues );
        if ( !present[columnIndex] ) {
            return;
        }
        addPresentValue( values, numericValues[columnIndex] );
    }


    public void add( AggregateGroupState values, ColumnReader[] readers, boolean[] consumed, boolean[] present, double[] numericValues ) {
        read( readers, consumed, present, numericValues );
        if ( !present[columnIndex] ) {
            return;
        }
        addPresentValue( values, numericValues[columnIndex] );
    }


    private void read( ColumnReader[] readers, boolean[] present, double[] numericValues ) {
        ColumnReader reader = readers[columnIndex];
        boolean valuePresent = required || reader.getCurrentDefinitionLevel() == maxDefinitionLevel;
        present[columnIndex] = valuePresent;
        if ( valuePresent ) {
            numericValues[columnIndex] = doubleValue( reader, type );
        }
        reader.consume();
    }


    private void read( ColumnReader[] readers, boolean[] consumed, boolean[] present, double[] numericValues ) {
        if ( consumed[columnIndex] ) {
            return;
        }
        read( readers, present, numericValues );
        consumed[columnIndex] = true;
    }


    private void addPresentValue( AggregateGroupState values, double value ) {
        AggregateRowAccumulator.addIndexes( values, singleCountIndex, countIndexes, 1 );
        AggregateRowAccumulator.addDoubleIndexes( values, singleDoubleIndex, doubleIndexes, value );
        AggregateRowAccumulator.addMinIndexes( values, singleMinIndex, minIndexes, value );
        AggregateRowAccumulator.addMaxIndexes( values, singleMaxIndex, maxIndexes, value );
    }


    private double doubleValue( ColumnReader reader, PrimitiveType.PrimitiveTypeName type ) {
        return switch ( type ) {
            case INT32 -> reader.getInteger();
            case INT64 -> reader.getLong();
            case FLOAT -> reader.getFloat();
            case DOUBLE -> reader.getDouble();
            case BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> throw new IllegalArgumentException( "Unsupported aggregate column type: " + type );
        };
    }

}
