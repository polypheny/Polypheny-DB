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
import com.pgvector.PGhalfvec;
import com.pgvector.PGvector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgArray;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.type.entity.numerical.PolyFloat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class PostgresqlVectorHelperTest {

    @BeforeAll
    static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void parsesVectorCorrectly() throws SQLException {
        PGvector obj = new PGvector(new float[]{1f, 2.5f, 3f});
        List<PolyFloat> result = PostgresqlVectorHelper.parseVector( obj );
        assertNotNull( result );
        assertEquals( 1.0, result.get(0).floatValue() );
        assertEquals( 2.5, result.get(1).floatValue() );
        assertEquals( 3.0, result.get(2).floatValue() );
    }


    @Test
    void parsesHalfvecCorrectly() throws SQLException {
        PGhalfvec obj = new PGhalfvec(new float[]{1f, 2.5f, 3f});
        List<PolyFloat> result = PostgresqlVectorHelper.parseVector( obj );
        assertNotNull( result );
        assertEquals( 1.0f, result.get(0).floatValue() );
        assertEquals( 2.5f, result.get(1).floatValue() );
        assertEquals( 3.0f, result.get(2).floatValue() );
    }


    @Test
    void nullReturn() {
        assertNull( PostgresqlVectorHelper.parseVector( null ) );
    }


    @Test
    void nonPgVectorObject() throws SQLException {
        assertNull( PostgresqlVectorHelper.parseVector( new PgArray( null, 0, "" ) ) );
    }


    @Test
    void parsesNegativeVectorCorrectly() throws SQLException {
        PGvector obj = new PGvector(new float[]{-1f, -2.5f, -3f});
        List<PolyFloat> result = PostgresqlVectorHelper.parseVector( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( -1.0, result.get(0).floatValue() );
        assertEquals( -2.5, result.get(1).floatValue() );
        assertEquals( -3.0, result.get(2).floatValue() );
    }


    @Test
    void parsesNegativeHalfvecCorrectly() throws SQLException {
        PGhalfvec obj = new PGhalfvec(new float[]{-1f, -2.5f, -3f});
        List<PolyFloat> result = PostgresqlVectorHelper.parseVector( obj );
        assertNotNull( result );
        assertEquals( 3, result.size() );
        assertEquals( -1.0f, result.get(0).floatValue() );
        assertEquals( -2.5f, result.get(1).floatValue() );
        assertEquals( -3.0f, result.get(2).floatValue() );
    }


    @Test
    void parseSingleEntryVector() throws SQLException {
        PGvector vector = new PGvector( new float[]{-1f} );
        List<PolyFloat> result = PostgresqlVectorHelper.parseVector( vector );
        assertNotNull( result );
        assertEquals( 1, result.size() );
        assertTrue( result.get(0) instanceof PolyFloat );
        assertEquals( -1.0f, result.get(0).floatValue() );
    }

}
