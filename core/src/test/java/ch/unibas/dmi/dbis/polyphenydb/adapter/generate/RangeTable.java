/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.generate;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.AbstractQueryableTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTableQueryable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;


/**
 * Table that returns a range of integers.
 */
public class RangeTable extends AbstractQueryableTable {

    private final String columnName;
    private final int start;
    private final int end;


    protected RangeTable( Class<?> elementType, String columnName, int start, int end ) {
        super( elementType );
        this.columnName = columnName;
        this.start = start;
        this.end = end;
    }


    /**
     * Creates a RangeTable.
     */
    public static RangeTable create( Class<?> elementType, String columnName, int start, int end ) {
        return new RangeTable( elementType, columnName, start, end );
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return typeFactory.builder()
                .add( columnName, SqlTypeName.INTEGER )
                .build();
    }


    @Override
    public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
        return new AbstractTableQueryable<T>( queryProvider, schema, this, tableName ) {
            @Override
            public Enumerator<T> enumerator() {
                //noinspection unchecked
                return (Enumerator<T>) RangeTable.this.enumerator();
            }
        };
    }


    public Enumerator<Integer> enumerator() {
        return new Enumerator<Integer>() {
            int current = start - 1;


            @Override
            public Integer current() {
                if ( current >= end ) {
                    throw new NoSuchElementException();
                }
                return current;
            }


            @Override
            public boolean moveNext() {
                ++current;
                return current < end;
            }


            @Override
            public void reset() {
                current = start - 1;
            }


            @Override
            public void close() {
            }
        };
    }


    /**
     * Implementation of {@link TableFactory} that allows a {@link RangeTable} to be included as a custom table in a Polypheny-DB model file.
     */
    public static class Factory implements TableFactory<RangeTable> {

        @Override
        public RangeTable create( SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType ) {
            final String columnName = (String) operand.get( "column" );
            final int start = (Integer) operand.get( "start" );
            final int end = (Integer) operand.get( "end" );
            final String elementType = (String) operand.get( "elementType" );
            Class<?> type;
            if ( "array".equals( elementType ) ) {
                type = Object[].class;
            } else if ( "object".equals( elementType ) ) {
                type = Object.class;
            } else if ( "integer".equals( elementType ) ) {
                type = Integer.class;
            } else {
                throw new IllegalArgumentException( "Illegal 'elementType' value: " + elementType );
            }
            return RangeTable.create( type, columnName, start, end );
        }
    }
}
