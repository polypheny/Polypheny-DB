/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.postgres;


import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.PGobject;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


public class PostgresqlVectorHelperTest {

    @BeforeAll
    static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void parseVectorAsDouble_parsesCorrectly() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        obj.setValue( "[1.0,2.5,3.0]" );
        List<PolyDouble> result = PostgresqlVectorHelper.parseVectorAsDouble( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( 1.0, result.get(0).doubleValue() );
        assertEquals( 2.5, result.get(1).doubleValue() );
        assertEquals( 3.0, result.get(2).doubleValue() );
    }


    @Test
    void parseVectorAsFloat_parsesCorrectly() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        obj.setValue( "[1,2.5,3]" );
        List<PolyFloat> result = PostgresqlVectorHelper.parseVectorAsFloat( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( 1.0f, result.get(0).floatValue() );
        assertEquals( 2.5f, result.get(1).floatValue() );
        assertEquals( 3.0f, result.get(2).floatValue() );
    }


    @Test
    void parseVector_nullReturn() {
        assertNull( PostgresqlVectorHelper.parseVectorAsDouble( null ) );
    }


    @Test
    void parseVector_nonPgObject() throws SQLException {
        assertNull( PostgresqlVectorHelper.parseVectorAsDouble( new PgArray( null, 0, "" ) ) );
    }


    @Test
    void parseVectorAsDouble_parsesNegativeCorrectly() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        obj.setValue( "[-1.0,-2.5,-3.0]" );
        List<PolyDouble> result = PostgresqlVectorHelper.parseVectorAsDouble( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( -1.0, result.get(0).doubleValue() );
        assertEquals( -2.5, result.get(1).doubleValue() );
        assertEquals( -3.0, result.get(2).doubleValue() );
    }


    @Test
    void parseVectorAsFloat_parsesNegativeCorrectly() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        obj.setValue( "[-1.0,-2.5,-3.0]" );
        List<PolyFloat> result = PostgresqlVectorHelper.parseVectorAsFloat( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( -1.0f, result.get(0).floatValue() );
        assertEquals( -2.5f, result.get(1).floatValue() );
        assertEquals( -3.0f, result.get(2).floatValue() );
    }


    @Test
    void parseVector_parseSingleEntry() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        obj.setValue( "[-1.0]" );
        List<PolyFloat> result = PostgresqlVectorHelper.parseVectorAsFloat( obj );
        assertNotNull( result );
        assertEquals( 1, result.size() );
        assertEquals( -1.0f, result.get(0).floatValue() );
    }


    @Test
    void parseVector_parsesWhiteSpaceCorrectly() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        obj.setValue( "[1, 2.5, 3]" );
        List<PolyFloat> result = PostgresqlVectorHelper.parseVectorAsFloat( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( 1.0f, result.get(0).floatValue() );
        assertEquals( 2.5f, result.get(1).floatValue() );
        assertEquals( 3.0f, result.get(2).floatValue() );
    }


    @Test
    void parseVector_pgObjectWithNullValue() throws SQLException {
        PGobject obj = new PGobject();
        obj.setType( "vector" );
        assertNull( PostgresqlVectorHelper.parseVectorAsDouble( obj ) );
    }
}
