/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.util.Iterator;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils;


/**
 * Enumerator that reads from a Cassandra column family.
 */
class CassandraEnumerator implements Enumerator<Object> {

    private final Iterator<Row> iterator;
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
    }


    /**
     * Produce the next row from the results
     *
     * @return A new row from the results
     */
    @Override
    public Object current() {
        if ( columnDefinitions.size() == 0 ) {
            return 0;
        } else if ( columnDefinitions.size() == 1 ) {
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

        if ( type instanceof UserDefinedType ) {
            return CassandraTypesUtils.unparseArrayContainerUdt( current.getUdtValue( index ) );
        }

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
        } else if ( type == DataTypes.DATE ) {
            return (int) current.getLocalDate( index ).toEpochDay();
        } else if ( type == DataTypes.TIME ) {
            // Time is represented in Polypheny-DB as an integer counting the number of milliseconds since the start of the day.
            return ((int) current.getLocalTime( index ).toNanoOfDay()) / 1000000;
        } else if ( type == DataTypes.TIMESTAMP ) {
            // Timestamp is represented in Polypheny-DB as a long counting the number of milliseconds since 1970-01-01T00:00:00+0000
            return current.getInstant( index ).getEpochSecond() * 1000L + current.getInstant( index ).getNano() / 1000000L;
        } else if ( type == DataTypes.BOOLEAN ) {
            return current.getBoolean( index );
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

