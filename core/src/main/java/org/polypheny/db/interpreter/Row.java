/*
 * Copyright 2019-2024 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.interpreter;


import java.lang.reflect.Array;
import java.util.Arrays;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Row.
 */
public class Row<T> {

    private final T[] values;

    public final Class<T> clazz;

    /**
     * Creates a Row.
     */
    // must stay package-protected, because does not copy
    Row( T[] values, Class<T> clazz ) {
        this.values = values;
        this.clazz = clazz;
    }


    /**
     * Creates a Row.
     *
     * Makes a defensive copy of the array, so the Row is immutable. (If you're worried about the extra copy, call {@link #of(PolyValue)}. But the JIT probably avoids the copy.)
     */
    public static Row<PolyValue> asCopy( PolyValue... values ) {
        return new Row<>( values.clone(), PolyValue.class );
    }


    /**
     * Creates a Row with one column value.
     */
    public static Row<PolyValue> of( PolyValue value0 ) {
        return new Row<>( new PolyValue[]{ value0 }, PolyValue.class );
    }


    /**
     * Creates a Row with two column values.
     */
    public static Row<PolyValue> of( PolyValue value0, PolyValue value1 ) {
        return new Row<>( new PolyValue[]{ value0, value1 }, PolyValue.class );
    }


    /**
     * Creates a Row with three column values.
     */
    public static Row<PolyValue> of( PolyValue value0, PolyValue value1, PolyValue value2 ) {
        return new Row<>( new PolyValue[]{ value0, value1, value2 }, PolyValue.class );
    }


    /**
     * Creates a Row with variable number of values.
     */
    public static Row<PolyValue> of( PolyValue... values ) {
        return new Row<>( values, PolyValue.class );
    }


    @Override
    public int hashCode() {
        return Arrays.hashCode( values );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof Row
                && Arrays.equals( values, ((Row) obj).values );
    }


    @Override
    public String toString() {
        return Arrays.toString( values );
    }


    public T getObject( int index ) {
        return values[index];
    }


    // must stay package-protected
    T[] getValues() {
        return values;
    }


    /**
     * Returns a copy of the values.
     */
    public T[] copyValues() {
        return values.clone();
    }


    public int size() {
        return values.length;
    }


    /**
     * Create a RowBuilder object that eases creation of a new row.
     *
     * @param size Number of columns in output data.
     * @return New RowBuilder object.
     */
    public static <T> RowBuilder<T> newBuilder( int size, Class<T> clazz ) {
        return new RowBuilder<>( size, clazz );
    }



    /**
     * Utility class to build row objects.
     */
    public static class RowBuilder<T> {

        T[] values;

        public Class<T> clazz;


        private RowBuilder( int size, Class<T> clazz ) {
            this.values = (T[]) Array.newInstance( clazz, size );
            this.clazz = clazz;
        }


        /**
         * Set the value of a particular column.
         *
         * @param index Zero-indexed position of value.
         * @param value Desired column value.
         */
        public void set( int index, T value ) {
            values[index] = value;
        }


        /**
         * Return a Row object
         **/
        public Row<T> build() {
            return new Row<>( values, clazz );
        }


        /**
         * Allocates a new internal array.
         */
        public void reset() {
            values = (T[]) Array.newInstance( clazz, values.length );
        }


        public int size() {
            return values.length;
        }

    }

}
