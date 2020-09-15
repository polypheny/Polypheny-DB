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
 */

package org.polypheny.db.adapter.cassandra.util;


import static org.junit.Assert.assertEquals;
import static org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils.getDataType;
import static org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils.getPolyType;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;
import org.polypheny.db.type.PolyType;


public class CassandraTypesUtilsTest {

    {
        // Booleans

        // Integers

        // Floating points

        // Date

        // Time

        // Timestamp

        // Intervals

        // Char

        // Binary
    }


    @Test
    public void simplePolyTypeToCassandraType() {
        // Booleans
        assertEquals( DataTypes.BOOLEAN, getDataType( PolyType.BOOLEAN, null ) );

        // Integers
        assertEquals( DataTypes.TINYINT, getDataType( PolyType.TINYINT, null ) );
        assertEquals( DataTypes.SMALLINT, getDataType( PolyType.SMALLINT, null ) );
        assertEquals( DataTypes.INT, getDataType( PolyType.INTEGER, null ) );
        assertEquals( DataTypes.BIGINT, getDataType( PolyType.BIGINT, null ) );

        // Floating points
        assertEquals( DataTypes.FLOAT, getDataType( PolyType.FLOAT, null ) );
        assertEquals( DataTypes.DOUBLE, getDataType( PolyType.DOUBLE, null ) );
//        assertEquals( DataTypes., getDataType( PolyType.REAL ) );
        assertEquals( DataTypes.DECIMAL, getDataType( PolyType.DECIMAL, null ) );

        // Date
        assertEquals( DataTypes.DATE, getDataType( PolyType.DATE, null ) );

        // Time
        assertEquals( DataTypes.TIME, getDataType( PolyType.TIME, null ) );
        assertEquals( DataTypes.TIME, getDataType( PolyType.TIME_WITH_LOCAL_TIME_ZONE, null ) );

        // Timestamp
        assertEquals( DataTypes.TIMESTAMP, getDataType( PolyType.TIMESTAMP, null ) );
        assertEquals( DataTypes.TIMESTAMP, getDataType( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, null ) );

        // Intervals

        // Char
        assertEquals( DataTypes.TEXT, getDataType( PolyType.CHAR, null ) );
        assertEquals( DataTypes.TEXT, getDataType( PolyType.VARCHAR, null ) );

        // Binary
        assertEquals( DataTypes.BLOB, getDataType( PolyType.BINARY, null ) );
        assertEquals( DataTypes.BLOB, getDataType( PolyType.VARBINARY, null ) );

    }


    @Test
    public void name() {
        // Booleans
        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.BOOLEAN ) );

        // Integers
        assertEquals( PolyType.TINYINT, getPolyType( DataTypes.TINYINT ) );
        assertEquals( PolyType.SMALLINT, getPolyType( DataTypes.SMALLINT ) );
        assertEquals( PolyType.INTEGER, getPolyType( DataTypes.INT ) );
        assertEquals( PolyType.BIGINT, getPolyType( DataTypes.BIGINT ) );

        // Floating points
        assertEquals( PolyType.FLOAT, getPolyType( DataTypes.FLOAT ) );
        assertEquals( PolyType.DOUBLE, getPolyType( DataTypes.DOUBLE ) );
//        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.BOOLEAN ) );
        assertEquals( PolyType.DECIMAL, getPolyType( DataTypes.DECIMAL ) );

        // Date
        assertEquals( PolyType.DATE, getPolyType( DataTypes.DATE ) );
//        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.D ) );

        // Time
        assertEquals( PolyType.TIME, getPolyType( DataTypes.TIME ) );
//        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.BOOLEAN ) );

        // Timestamp
        assertEquals( PolyType.TIMESTAMP, getPolyType( DataTypes.TIMESTAMP ) );
//        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.BOOLEAN ) );

        // Intervals

        // Char
        assertEquals( PolyType.VARCHAR, getPolyType( DataTypes.TEXT ) );
//        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.BOOLEAN ) );

        // Binary
        assertEquals( PolyType.VARBINARY, getPolyType( DataTypes.BLOB ) );
//        assertEquals( PolyType.BOOLEAN, getPolyType( DataTypes.BOOLEAN ) );
    }


    @Test
    public void doubleTest() {
        // Booleans
        assertEquals( PolyType.BOOLEAN, getPolyType( getDataType( PolyType.BOOLEAN, null ) ) );

        // Integers
        assertEquals( PolyType.TINYINT, getPolyType( getDataType( PolyType.TINYINT, null ) ) );
        assertEquals( PolyType.SMALLINT, getPolyType( getDataType( PolyType.SMALLINT, null ) ) );
        assertEquals( PolyType.INTEGER, getPolyType( getDataType( PolyType.INTEGER, null ) ) );
        assertEquals( PolyType.BIGINT, getPolyType( getDataType( PolyType.BIGINT, null ) ) );

        // Floating points
        assertEquals( PolyType.FLOAT, getPolyType( getDataType( PolyType.FLOAT, null ) ) );
        assertEquals( PolyType.DOUBLE, getPolyType( getDataType( PolyType.DOUBLE, null ) ) );
        // There is no REAL type in cassandra, so we use FLOAT instead.
        assertEquals( PolyType.FLOAT, getPolyType( getDataType( PolyType.REAL, null ) ) );
        assertEquals( PolyType.DECIMAL, getPolyType( getDataType( PolyType.DECIMAL, null ) ) );

        // Date
        assertEquals( PolyType.DATE, getPolyType( getDataType( PolyType.DATE, null ) ) );

        // Time
        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME, null ) ) );
        // Cassandra has no TIME with timezone support, so we just use TIME
        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME_WITH_LOCAL_TIME_ZONE, null ) ) );

        // Timestamp
        assertEquals( PolyType.TIMESTAMP, getPolyType( getDataType( PolyType.TIMESTAMP, null ) ) );
        // Cassandra has no TIMESTAMP with local timezone support, so we just use TIMESTAMP
        assertEquals( PolyType.TIMESTAMP, getPolyType( getDataType( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, null ) ) );

        // FIXME: Intervals

        // Char
        // No char support, both char and varchar are represented as text in cassandra.
        assertEquals( PolyType.VARCHAR, getPolyType( getDataType( PolyType.CHAR, null ) ) );
        assertEquals( PolyType.VARCHAR, getPolyType( getDataType( PolyType.VARCHAR, null ) ) );

        // Binary
        // No binary or varbinary support, only blob, so we use it for both
        assertEquals( PolyType.VARBINARY, getPolyType( getDataType( PolyType.BINARY, null ) ) );
        assertEquals( PolyType.VARBINARY, getPolyType( getDataType( PolyType.VARBINARY, null ) ) );

//        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME ) ) );
//        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME ) ) );

    }
}
