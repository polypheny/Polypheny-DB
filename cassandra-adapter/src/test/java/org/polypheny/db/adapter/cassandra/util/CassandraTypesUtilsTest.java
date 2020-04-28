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
        assertEquals( DataTypes.BOOLEAN, getDataType( PolyType.BOOLEAN ) );

        // Integers
        assertEquals( DataTypes.TINYINT, getDataType( PolyType.TINYINT ) );
        assertEquals( DataTypes.SMALLINT, getDataType( PolyType.SMALLINT ) );
        assertEquals( DataTypes.INT, getDataType( PolyType.INTEGER ) );
        assertEquals( DataTypes.BIGINT, getDataType( PolyType.BIGINT ) );

        // Floating points
        assertEquals( DataTypes.FLOAT, getDataType( PolyType.FLOAT ) );
        assertEquals( DataTypes.DOUBLE, getDataType( PolyType.DOUBLE ) );
//        assertEquals( DataTypes., getDataType( PolyType.REAL ) );
        assertEquals( DataTypes.DECIMAL, getDataType( PolyType.DECIMAL ) );

        // Date
        assertEquals( DataTypes.DATE, getDataType( PolyType.DATE ) );

        // Time
        assertEquals( DataTypes.TIME, getDataType( PolyType.TIME ) );
        assertEquals( DataTypes.TIME, getDataType( PolyType.TIME_WITH_LOCAL_TIME_ZONE ) );

        // Timestamp
        assertEquals( DataTypes.TIMESTAMP, getDataType( PolyType.TIMESTAMP ) );
        assertEquals( DataTypes.TIMESTAMP, getDataType( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) );

        // Intervals

        // Char
        assertEquals( DataTypes.TEXT, getDataType( PolyType.CHAR ) );
        assertEquals( DataTypes.TEXT, getDataType( PolyType.VARCHAR ) );

        // Binary
        assertEquals( DataTypes.BLOB, getDataType( PolyType.BINARY ) );
        assertEquals( DataTypes.BLOB, getDataType( PolyType.VARBINARY ) );

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
        assertEquals( PolyType.BOOLEAN, getPolyType( getDataType( PolyType.BOOLEAN ) ) );

        // Integers
        assertEquals( PolyType.TINYINT, getPolyType( getDataType( PolyType.TINYINT ) ) );
        assertEquals( PolyType.SMALLINT, getPolyType( getDataType( PolyType.SMALLINT ) ) );
        assertEquals( PolyType.INTEGER, getPolyType( getDataType( PolyType.INTEGER ) ) );
        assertEquals( PolyType.BIGINT, getPolyType( getDataType( PolyType.BIGINT ) ) );

        // Floating points
        assertEquals( PolyType.FLOAT, getPolyType( getDataType( PolyType.FLOAT ) ) );
        assertEquals( PolyType.DOUBLE, getPolyType( getDataType( PolyType.DOUBLE ) ) );
        // There is no REAL type in cassandra, so we use FLOAT instead.
        assertEquals( PolyType.FLOAT, getPolyType( getDataType( PolyType.REAL ) ) );
        assertEquals( PolyType.DECIMAL, getPolyType( getDataType( PolyType.DECIMAL ) ) );

        // Date
        assertEquals( PolyType.DATE, getPolyType( getDataType( PolyType.DATE ) ) );

        // Time
        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME ) ) );
        // Cassandra has no TIME with timezone support, so we just use TIME
        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME_WITH_LOCAL_TIME_ZONE ) ) );

        // Timestamp
        assertEquals( PolyType.TIMESTAMP, getPolyType( getDataType( PolyType.TIMESTAMP ) ) );
        // Cassandra has no TIMESTAMP with local timezone support, so we just use TIMESTAMP
        assertEquals( PolyType.TIMESTAMP, getPolyType( getDataType( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) ) );

        // FIXME: Intervals

        // Char
        // No char support, both char and varchar are represented as text in cassandra.
        assertEquals( PolyType.VARCHAR, getPolyType( getDataType( PolyType.CHAR ) ) );
        assertEquals( PolyType.VARCHAR, getPolyType( getDataType( PolyType.VARCHAR ) ) );

        // Binary
        // No binary or varbinary support, only blob, so we use it for both
        assertEquals( PolyType.VARBINARY, getPolyType( getDataType( PolyType.BINARY ) ) );
        assertEquals( PolyType.VARBINARY, getPolyType( getDataType( PolyType.VARBINARY ) ) );

//        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME ) ) );
//        assertEquals( PolyType.TIME, getPolyType( getDataType( PolyType.TIME ) ) );

    }
}
