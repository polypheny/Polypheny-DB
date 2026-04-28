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

package org.polypheny.db.adapter.postgres.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.pgvector.PGbit;
import com.pgvector.PGhalfvec;
import com.pgvector.PGsparsevec;
import com.pgvector.PGvector;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.postgres.PostgresqlVectorHelper;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.postgresql.jdbc.PgArray;

public class PostgresqlVectorHelperTest {

    @BeforeAll
    static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    // ---- float vectors ----------------------------------------------------------------

    @Test
    void parsesVectorCorrectly() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGvector( new float[]{ 1f, 2.5f, 3f } ) );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertFloatValues( result, 1f, 2.5f, 3f );
    }


    @Test
    void parsesNegativeVectorCorrectly() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGvector( new float[]{ -1f, -2.5f, -3f } ) );
        assertNotNull( result );
        assertFloatValues( result, -1f, -2.5f, -3f );
    }


    @Test
    void parsesSingleEntryVector() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGvector( new float[]{ -1f } ) );
        assertNotNull( result );
        assertEquals( 1, result.size() );
        assertFloatValues( result, -1f );
    }


    @Test
    void parsesEmptyFloatVectorReturnsEmptyList() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGvector( new float[]{} ) );
        assertNotNull( result );
        assertEquals( 0, result.size() );
    }


    // ---- halfvec ----------------------------------------------------------------

    @Test
    void parsesHalfvecCorrectly() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGhalfvec( new float[]{ 1f, 2.5f, 3f } ) );
        assertNotNull( result );
        assertFloatValues( result, 1f, 2.5f, 3f );
    }


    @Test
    void parsesNegativeHalfvecCorrectly() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGhalfvec( new float[]{ -1f, -2.5f, -3f } ) );
        assertNotNull( result );
        assertFloatValues( result, -1f, -2.5f, -3f );
    }


    // ---- sparsevec ----------------------------------------------------------------

    @Test
    void parsesSparsevecCorrectly() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGsparsevec( new float[]{ 1f, 0f, 2.5f } ) );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertFloatValues( result, 1f, 0f, 2.5f );
    }


    // ---- bitvector ----------------------------------------------------------------

    @Test
    void parsesBitVectorCorrectly() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGbit( new boolean[]{ true, false, false } ) );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertInstanceOf( PolyBoolean.class, result.get( 0 ) );
        assertEquals( true, result.get( 0 ).asBoolean().getValue() );
        assertEquals( false, result.get( 1 ).asBoolean().getValue() );
        assertEquals( false, result.get( 2 ).asBoolean().getValue() );
    }


    @Test
    void parsesEmptyBitVectorReturnsEmptyList() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( new PGbit( new boolean[]{} ) );
        assertNotNull( result );
        assertEquals( 0, result.size() );
    }


    @Test
    void parsesBitVectorFromStringRepresentation() {
        // PostgreSQL's getString() on a bit(n) column returns e.g. "101"
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( "101" );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( true, result.get( 0 ).asBoolean().getValue() );
        assertEquals( false, result.get( 1 ).asBoolean().getValue() );
        assertEquals( true, result.get( 2 ).asBoolean().getValue() );
    }


    @Test
    void parsesAllZeroStringBitVector() {
        List<PolyValue> result = PostgresqlVectorHelper.parseVector( "000" );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        result.forEach( v -> assertEquals( false, v.asBoolean().getValue() ) );
    }


    @Test
    void nullObjectReturnsNull() {
        assertNull( PostgresqlVectorHelper.parseVector( (Object) null ) );
    }


    @Test
    void unknownObjectTypeReturnsNull() throws SQLException {
        assertNull( PostgresqlVectorHelper.parseVector( new PgArray( null, 0, "" ) ) );
    }

    // ---- helpers ----------------------------------------------------------------

    private static void assertFloatValues( List<PolyValue> result, float... expected ) {
        assertEquals( expected.length, result.size() );
        for ( int i = 0; i < expected.length; i++ ) {
            assertInstanceOf( PolyFloat.class, result.get( i ) );
            assertEquals( expected[i], ((PolyFloat) result.get( i )).floatValue() );
        }
    }

}
