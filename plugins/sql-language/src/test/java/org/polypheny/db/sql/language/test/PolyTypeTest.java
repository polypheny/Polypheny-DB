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
 */

package org.polypheny.db.sql.language.test;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.type.PolyType;


/**
 * Tests types supported by {@link PolyType}.
 */
public class PolyTypeTest {

    @Test
    public void testBit() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BIT );
        assertEquals( PolyType.BOOLEAN, tn, "BIT did not map to BOOLEAN" );
    }


    @Test
    public void testTinyint() {
        PolyType tn = PolyType.getNameForJdbcType( Types.TINYINT );
        assertEquals( PolyType.TINYINT, tn, "TINYINT did not map to TINYINT" );
    }


    @Test
    public void testSmallint() {
        PolyType tn = PolyType.getNameForJdbcType( Types.SMALLINT );
        assertEquals( PolyType.SMALLINT, tn, "SMALLINT did not map to SMALLINT" );
    }


    @Test
    public void testInteger() {
        PolyType tn = PolyType.getNameForJdbcType( Types.INTEGER );
        assertEquals( PolyType.INTEGER, tn, "INTEGER did not map to INTEGER" );
    }


    @Test
    public void testBigint() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BIGINT );
        assertEquals( PolyType.BIGINT, tn, "BIGINT did not map to BIGINT" );
    }


    @Test
    public void testFloat() {
        PolyType tn = PolyType.getNameForJdbcType( Types.FLOAT );
        assertEquals( PolyType.FLOAT, tn, "FLOAT did not map to FLOAT" );
    }


    @Test
    public void testReal() {
        PolyType tn = PolyType.getNameForJdbcType( Types.REAL );
        assertEquals( PolyType.REAL, tn, "REAL did not map to REAL" );
    }


    @Test
    public void testDouble() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DOUBLE );
        assertEquals( PolyType.DOUBLE, tn, "DOUBLE did not map to DOUBLE" );
    }


    @Test
    public void testNumeric() {
        PolyType tn = PolyType.getNameForJdbcType( Types.NUMERIC );
        assertEquals( PolyType.DECIMAL, tn, "NUMERIC did not map to DECIMAL" );
    }


    @Test
    public void testDecimal() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DECIMAL );
        assertEquals( PolyType.DECIMAL, tn, "DECIMAL did not map to DECIMAL" );
    }


    @Test
    public void testChar() {
        PolyType tn = PolyType.getNameForJdbcType( Types.CHAR );
        assertEquals( PolyType.CHAR, tn, "CHAR did not map to CHAR" );
    }


    @Test
    public void testVarchar() {
        PolyType tn = PolyType.getNameForJdbcType( Types.VARCHAR );
        assertEquals( PolyType.VARCHAR, tn, "VARCHAR did not map to VARCHAR" );
    }


    @Test
    public void testLongvarchar() {
        PolyType tn = PolyType.getNameForJdbcType( Types.LONGVARCHAR );
        assertNull( tn, "LONGVARCHAR did not map to null" );
    }


    @Test
    public void testDate() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DATE );
        assertEquals( PolyType.DATE, tn, "DATE did not map to DATE" );
    }


    @Test
    public void testTime() {
        PolyType tn = PolyType.getNameForJdbcType( Types.TIME );
        assertEquals( PolyType.TIME, tn, "TIME did not map to TIME" );
    }


    @Test
    public void testTimestamp() {
        PolyType tn = PolyType.getNameForJdbcType( Types.TIMESTAMP );
        assertEquals( PolyType.TIMESTAMP, tn, "TIMESTAMP did not map to TIMESTAMP" );
    }


    @Test
    public void testBinary() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BINARY );
        assertEquals( PolyType.BINARY, tn, "BINARY did not map to BINARY" );
    }


    @Test
    public void testVarbinary() {
        PolyType tn = PolyType.getNameForJdbcType( Types.VARBINARY );
        assertEquals( PolyType.VARBINARY, tn, "VARBINARY did not map to VARBINARY" );
    }


    @Test
    public void testLongvarbinary() {
        PolyType tn = PolyType.getNameForJdbcType( Types.LONGVARBINARY );
        assertNull( tn, "LONGVARBINARY did not map to null" );
    }


    @Test
    public void testNull() {
        PolyType tn = PolyType.getNameForJdbcType( Types.NULL );
        assertNull( tn, "NULL did not map to null" );
    }


    @Test
    public void testOther() {
        PolyType tn = PolyType.getNameForJdbcType( Types.OTHER );
        assertNull( tn, "OTHER did not map to null" );
    }


    @Test
    public void testJavaobject() {
        PolyType tn = PolyType.getNameForJdbcType( Types.JAVA_OBJECT );
        assertNull( tn, "JAVA_OBJECT did not map to null" );
    }


    @Test
    public void testDistinct() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DISTINCT );
        assertEquals( PolyType.DISTINCT, tn, "DISTINCT did not map to DISTINCT" );
    }


    @Test
    public void testStruct() {
        PolyType tn = PolyType.getNameForJdbcType( Types.STRUCT );
        assertEquals( PolyType.STRUCTURED, tn, "STRUCT did not map to null" );
    }


    @Test
    public void testArray() {
        PolyType tn = PolyType.getNameForJdbcType( Types.ARRAY );
        assertEquals( PolyType.ARRAY, tn, "ARRAY did not map to ARRAY" );
    }


    @Test
    public void testBlob() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BLOB );
        assertNull( tn, "BLOB did not map to null" );
    }


    @Test
    public void testClob() {
        PolyType tn = PolyType.getNameForJdbcType( Types.CLOB );
        assertNull( tn, "CLOB did not map to null" );
    }


    @Test
    public void testRef() {
        PolyType tn = PolyType.getNameForJdbcType( Types.REF );
        assertNull( tn, "REF did not map to null" );
    }


    @Test
    public void testDatalink() {
        PolyType tn = PolyType.getNameForJdbcType( Types.DATALINK );
        assertNull( tn, "DATALINK did not map to null" );
    }


    @Test
    public void testBoolean() {
        PolyType tn = PolyType.getNameForJdbcType( Types.BOOLEAN );
        assertEquals( PolyType.BOOLEAN, tn, "BOOLEAN did not map to BOOLEAN" );
    }


    @Test
    public void testRowid() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.ROWID );

        // ROWID not supported yet
        assertNull( tn, "ROWID maps to non-null type" );
    }


    @Test
    public void testNchar() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.NCHAR );

        // NCHAR not supported yet, currently maps to CHAR
        assertEquals( PolyType.CHAR, tn, "NCHAR did not map to CHAR" );
    }


    @Test
    public void testNvarchar() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.NVARCHAR );

        // NVARCHAR not supported yet, currently maps to VARCHAR
        assertEquals( PolyType.VARCHAR, tn, "NVARCHAR did not map to VARCHAR" );
    }


    @Test
    public void testLongnvarchar() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.LONGNVARCHAR );

        // LONGNVARCHAR not supported yet
        assertNull( tn, "LONGNVARCHAR maps to non-null type" );
    }


    @Test
    public void testNclob() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.NCLOB );

        // NCLOB not supported yet
        assertNull( tn, "NCLOB maps to non-null type" );
    }


    @Test
    public void testSqlxml() {
        PolyType tn = PolyType.getNameForJdbcType( ExtraPolyTypes.SQLXML );

        // SQLXML not supported yet
        assertNull( tn, "SQLXML maps to non-null type" );
    }

}

