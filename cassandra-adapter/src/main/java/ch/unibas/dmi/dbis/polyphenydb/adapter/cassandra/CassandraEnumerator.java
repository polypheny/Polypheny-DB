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


import org.apache.calcite.linq4j.Enumerator;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.Iterator;
import java.util.List;


/**
 * Enumerator that reads from a Cassandra column family.
 */
class CassandraEnumerator implements Enumerator<Object> {

    private Iterator<Row> iterator;
    private Row current;
    private List<RelDataTypeField> fieldTypes;


    /**
     * Creates a CassandraEnumerator.
     *
     * @param results Cassandra result set ({@link com.datastax.driver.core.ResultSet})
     * @param protoRowType The type of resulting rows
     */
    CassandraEnumerator( ResultSet results, RelProtoDataType protoRowType ) {
        this.iterator = results.iterator();
        this.current = null;

        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        this.fieldTypes = protoRowType.apply( typeFactory ).getFieldList();
    }


    /**
     * Produce the next row from the results
     *
     * @return A new row from the results
     */
    public Object current() {
        if ( fieldTypes.size() == 1 ) {
            // If we just have one field, produce it directly
            return currentRowField( 0, fieldTypes.get( 0 ).getType().getSqlTypeName() );
        } else {
            // Build an array with all fields in this row
            Object[] row = new Object[fieldTypes.size()];
            for ( int i = 0; i < fieldTypes.size(); i++ ) {
                row[i] = currentRowField( i, fieldTypes.get( i ).getType().getSqlTypeName() );
            }
            return row;
        }
    }


    /**
     * Get a field for the current row from the underlying object.
     *
     * @param index Index of the field within the Row object
     * @param typeName Type of the field in this row
     */
    private Object currentRowField( int index, SqlTypeName typeName ) {
        DataType type = current.getColumnDefinitions().getType( index );
        if ( type == DataType.ascii() || type == DataType.text() || type == DataType.varchar() ) {
            return current.getString( index );
        } else if ( type == DataType.cint() || type == DataType.varint() ) {
            return current.getInt( index );
        } else if ( type == DataType.bigint() ) {
            return current.getLong( index );
        } else if ( type == DataType.cdouble() ) {
            return current.getDouble( index );
        } else if ( type == DataType.cfloat() ) {
            return current.getFloat( index );
        } else if ( type == DataType.uuid() || type == DataType.timeuuid() ) {
            return current.getUUID( index ).toString();
        } else {
            return null;
        }
    }


    public boolean moveNext() {
        if ( iterator.hasNext() ) {
            current = iterator.next();
            return true;
        } else {
            return false;
        }
    }


    public void reset() {
        throw new UnsupportedOperationException();
    }


    public void close() {
        // Nothing to do here
    }
}

