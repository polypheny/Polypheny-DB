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
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;


/**
 * Enumerator that reads from a Cassandra column family.
 */
class CassandraEnumerator implements Enumerator<Object> {

    private final Iterator<Row> iterator;
//    private final List<RelDataTypeField> fieldTypes;
    private final ColumnDefinitions columnDefinitions;
    private Row current;

    /**
     * Creates a CassandraEnumerator.
     *
     * @param results Cassandra result set ({@link com.datastax.oss.driver.api.core.cql.ResultSet})
     */
    CassandraEnumerator( ResultSet results ) {
        this.iterator = results.iterator();
        this.current = null;
        this.columnDefinitions = results.getColumnDefinitions();

//        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
//        this.fieldTypes = protoRowType.apply( typeFactory ).getFieldList();
    }


    /**
     * Produce the next row from the results
     *
     * @return A new row from the results
     */
    @Override
    public Object current() {
        if ( columnDefinitions.size() == 1 ) {
            // If we just have one field, produce it directly
            return currentRowField( 0 );
        } else {
            // Build an array with all fields in this row
            Object[] row = new Object[columnDefinitions.size()];
            for ( int i = 0; i < columnDefinitions.size(); i++ ) {
                row[i] = currentRowField( i );
            }
            return row;
        }
    }


    /**
     * Get a field for the current row from the underlying object.
     *
     * @param index Index of the field within the Row object
     */
    private Object currentRowField( int index ) {
        DataType type = this.columnDefinitions.get( index ).getType();
        if ( type == DataTypes.ASCII || type == DataTypes.TEXT ) {
            return current.getString( index );
        } else if ( type == DataTypes.INT || type == DataTypes.VARINT ) {
            return current.getInt( index );
        } else if ( type == DataTypes.BIGINT ) {
            return current.getLong( index );
        } else if ( type == DataTypes.DOUBLE ) {
            return current.getDouble( index );
        } else if ( type == DataTypes.FLOAT ) {
            return current.getFloat( index );
        } else if ( type == DataTypes.UUID || type == DataTypes.TIMEUUID ) {
            return current.getUuid( index ).toString();
        } else {
            return null;
        }
    }


    @Override
    public boolean moveNext() {
        if ( iterator.hasNext() ) {
            current = iterator.next();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        // Nothing to do here
    }
}

