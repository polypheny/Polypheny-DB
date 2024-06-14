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
package org.polypheny.db.util.avatica;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.util.avatica.ColumnMetaData.ArrayType;
import org.polypheny.db.util.avatica.ColumnMetaData.AvaticaType;
import org.polypheny.db.util.avatica.ColumnMetaData.Rep;

@Getter
public class ArrayImpl implements Array {

    final List<Object> list;
    final ArrayType array;
    final AvaticaType elementType;


    public ArrayImpl( final List<Object> list, AvaticaType elementType ) {
        this.elementType = elementType;
        this.array = ColumnMetaData.array( elementType, elementType.name, Rep.ARRAY );
        this.list = list;
    }


    @Override
    public String getBaseTypeName() {
        return elementType.getName();
    }


    @Override
    public int getBaseType() {
        return Types.ARRAY;
    }


    public Object getArray() throws SQLException {
        return list.toArray();
    }


    @Override
    public String toString() {
        return list.stream().map( Object::toString ).collect( Collectors.joining( ", ", "[", "]" ) );
    }


    @Override
    public Object getArray( Map<String, Class<?>> map ) throws SQLException {
        throw new UnsupportedOperationException();
    }


    @Override
    public Object getArray( long index, int count ) throws SQLException {
        throw new UnsupportedOperationException();
    }


    @Override
    public Object getArray( long index, int count, Map<String, Class<?>> map ) throws SQLException {
        throw new UnsupportedOperationException();
    }


    @Override
    public ResultSet getResultSet() {
        throw new UnsupportedOperationException();
    }


    @Override
    public ResultSet getResultSet( Map<String, Class<?>> map ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public ResultSet getResultSet( long index, int count ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public ResultSet getResultSet(
            long index, int count,
            Map<String, Class<?>> map ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void free() throws SQLException {
        // nothing to do
    }


    /**
     * Factory that can create a ResultSet or Array based on a stream of values.
     */
    public interface Factory {

        /**
         * Creates a {@link ResultSet} from the given list of values per {@link Array#getResultSet()}.
         *
         * @param elementType The type of the elements
         * @param iterable The elements
         * @throws SQLException on error
         */
        ResultSet create( ColumnMetaData.AvaticaType elementType, Iterable<Object> iterable )
                throws SQLException;

    }

}
