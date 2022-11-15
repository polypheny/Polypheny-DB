/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql.language.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.Types;
import org.junit.Test;
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.type.PolyType;


/**
 * Tests types supported by {@link PolyType}.
 */
public class PolyTypeTest {

    @Test
    public void testBit() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BIT );
        assertEquals( "BIT did not map to BOOLEAN", PolyType.BOOLEAN, tn );
    }


    @Test
    public void testTinyint() {
        PolyType tn = PolyType.getNameForJdbcType( Types.TINYINT );
        assertEquals( "TINYINT did not map to TINYINT", PolyType.TINYINT, tn );
    }


    @Test
    public void testSmallint() {
        PolyType tn = PolyType.getNameForJdbcType( Types.SMALLINT );
        assertEquals( "SMALLINT did not map to SMALLINT", PolyType.SMALLINT, tn );
    }


    @Test
    public void testInteger() {
        PolyType tn = PolyType.getNameForJdbcType( Types.INTEGER );
        assertEquals( "INTEGER did not map to INTEGER", PolyType.INTEGER, tn );
    }


    @Test
    public void testBigint() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BIGINT );
        assertEquals( "BIGINT did not map to BIGINT", PolyType.BIGINT, tn );
    }


    @Test
    public void testFloat() {
        PolyType tn = PolyType.getNameForJdbcType( Types.FLOAT );
        assertEquals( "FLOAT did not map to FLOAT", PolyType.FLOAT, tn );
    }


    @Test
    public void testReal() {
        PolyType tn = PolyType.getNameForJdbcType( Types.REAL );
        assertEquals( "REAL did not map to REAL", PolyType.REAL, tn );
    }


    @Test
    public void testDouble() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DOUBLE );
        assertEquals( "DOUBLE did not map to DOUBLE", PolyType.DOUBLE, tn );
    }


    @Test
    public void testNumeric() {
        PolyType tn = PolyType.getNameForJdbcType( Types.NUMERIC );
        assertEquals( "NUMERIC did not map to DECIMAL", PolyType.DECIMAL, tn );
    }


    @Test
    public void testDecimal() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DECIMAL );
        assertEquals( "DECIMAL did not map to DECIMAL", PolyType.DECIMAL, tn );
    }


    @Test
    public void testChar() {
        PolyType tn = PolyType.getNameForJdbcType( Types.CHAR );
        assertEquals( "CHAR did not map to CHAR", PolyType.CHAR, tn );
    }


    @Test
    public void testVarchar() {
        PolyType tn = PolyType.getNameForJdbcType( Types.VARCHAR );
        assertEquals( "VARCHAR did not map to VARCHAR", PolyType.VARCHAR, tn );
    }


    @Test
    public void testLongvarchar() {
        PolyType tn = PolyType.getNameForJdbcType( Types.LONGVARCHAR );
        assertNull( "LONGVARCHAR did not map to null", tn );
    }


    @Test
    public void testDate() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DATE );
        assertEquals( "DATE did not map to DATE", PolyType.DATE, tn );
    }


    @Test
    public void testTime() {
        PolyType tn = PolyType.getNameForJdbcType( Types.TIME );
        assertEquals( "TIME did not map to TIME", PolyType.TIME, tn );
    }


    @Test
    public void testTimestamp() {
        PolyType tn = PolyType.getNameForJdbcType( Types.TIMESTAMP );
        assertEquals( "TIMESTAMP did not map to TIMESTAMP", PolyType.TIMESTAMP, tn );
    }


    @Test
    public void testBinary() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BINARY );
        assertEquals( "BINARY did not map to BINARY", PolyType.BINARY, tn );
    }


    @Test
    public void testVarbinary() {
        PolyType tn = PolyType.getNameForJdbcType( Types.VARBINARY );
        assertEquals( "VARBINARY did not map to VARBINARY", PolyType.VARBINARY, tn );
    }


    @Test
    public void testLongvarbinary() {
        PolyType tn = PolyType.getNameForJdbcType( Types.LONGVARBINARY );
        assertNull( "LONGVARBINARY did not map to null", tn );
    }


    @Test
    public void testNull() {
        PolyType tn = PolyType.getNameForJdbcType( Types.NULL );
        assertNull( "NULL did not map to null", tn );
    }


    @Test
    public void testOther() {
        PolyType tn = PolyType.getNameForJdbcType( Types.OTHER );
        assertNull( "OTHER did not map to null", tn );
    }


    @Test
    public void testJavaobject() {
        PolyType tn = PolyType.getNameForJdbcType( Types.JAVA_OBJECT );
        assertNull( "JAVA_OBJECT did not map to null", tn );
    }


    @Test
    public void testDistinct() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DISTINCT );
        assertEquals( "DISTINCT did not map to DISTINCT", PolyType.DISTINCT, tn );
    }


    @Test
    public void testStruct() {
        PolyType tn = PolyType.getNameForJdbcType( Types.STRUCT );
        assertEquals( "STRUCT did not map to null", PolyType.STRUCTURED, tn );
    }


    @Test
    public void testArray() {
        PolyType tn = PolyType.getNameForJdbcType( Types.ARRAY );
        assertEquals( "ARRAY did not map to ARRAY", PolyType.ARRAY, tn );
    }


    @Test
    public void testBlob() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BLOB );
        assertNull( "BLOB did not map to null", tn );
    }


    @Test
    public void testClob() {
        PolyType tn = PolyType.getNameForJdbcType( Types.CLOB );
        assertNull( "CLOB did not map to null", tn );
    }


    @Test
    public void testRef() {
        PolyType tn = PolyType.getNameForJdbcType( Types.REF );
        assertNull( "REF did not map to null", tn );
    }


    @Test
    public void testDatalink() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DATALINK );
        assertNull( "DATALINK did not map to null", tn );
    }


    @Test
    public void testBoolean() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BOOLEAN );
        assertEquals( "BOOLEAN did not map to BOOLEAN", PolyType.BOOLEAN, tn );
    }


    @Test
    public void testRowid() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.ROWID );

        // ROWID not supported yet
        assertNull( "ROWID maps to non-null type", tn );
    }


    @Test
    public void testNchar() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.NCHAR );

        // NCHAR not supported yet, currently maps to CHAR
        assertEquals( "NCHAR did not map to CHAR", PolyType.CHAR, tn );
    }


    @Test
    public void testNvarchar() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.NVARCHAR );

        // NVARCHAR not supported yet, currently maps to VARCHAR
        assertEquals( "NVARCHAR did not map to VARCHAR", PolyType.VARCHAR, tn );
    }


    @Test
    public void testLongnvarchar() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.LONGNVARCHAR );

        // LONGNVARCHAR not supported yet
        assertNull( "LONGNVARCHAR maps to non-null type", tn );
    }


    @Test
    public void testNclob() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.NCLOB );

        // NCLOB not supported yet
        assertNull( "NCLOB maps to non-null type", tn );
    }


    @Test
    public void testSqlxml() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.SQLXML );

        // SQLXML not supported yet
        assertNull( "SQLXML maps to non-null type", tn );
    }

}

