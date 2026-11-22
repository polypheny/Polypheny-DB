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

package org.polypheny.db.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.crossmodel.CrossModelTestTemplate;

class JsonValueReturningTest extends CrossModelTestTemplate {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @ParameterizedTest
    @CsvSource({
            "DATE, 2147483648.25, -2147483649.25",
            "TIMESTAMP, 9223372036854775808.9, -9223372036854775809.9"
    })
    void temporalOverflowHonorsOnError( String type, String aboveMaximum, String belowMinimum ) {
        executeStatements( ( statement, connection ) -> {
            checkTemporalOverflow( statement, type, aboveMaximum );
            checkTemporalOverflow( statement, type, belowMinimum );
        } );
    }


    private static void checkTemporalOverflow( Statement statement, String type, String decimal ) throws SQLException {
        String call = "JSON_VALUE('{\"n\":{\"$numberDecimal\":\"%s\"}}', 'lax $.n' RETURNING %s".formatted( decimal, type );
        String fallback = type.equals( "DATE" ) ? "DATE '2000-01-01'" : "TIMESTAMP '2000-01-01 00:00:00'";
        String query = "SELECT " + call + "), " + call + " NULL ON ERROR) IS NULL, " + call + " DEFAULT " + fallback + " ON ERROR)";
        Calendar utc = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
        try ( ResultSet result = statement.executeQuery( query ) ) {
            assertTrue( result.next() );
            assertNull( result.getObject( 1 ) );
            assertTrue( result.getBoolean( 2 ) );
            long fallbackMillis = type.equals( "DATE" ) ? result.getDate( 3, utc ).getTime() : result.getTimestamp( 3, utc ).getTime();
            assertEquals( 946684800000L, fallbackMillis );
            assertFalse( result.next() );
        }

        assertThrows( SQLException.class, () -> {
            try ( ResultSet result = statement.executeQuery( "SELECT " + call + " ERROR ON ERROR)" ) ) {
                result.next();
            }
        }, decimal );
    }


    @Test
    void preservesDistinctReturningTypesAndOnEmpty() {
        executeStatements( ( statement, connection ) -> {
            String query = """
                    SELECT JSON_VALUE('{"n":"abcd"}', 'lax $.n' RETURNING VARCHAR(2)),
                           JSON_VALUE('{"n":"abcd"}', 'lax $.n' RETURNING VARCHAR(3)),
                           JSON_VALUE('{}', 'lax $.n' RETURNING VARCHAR(2) DEFAULT 'xyz' ON EMPTY ERROR ON ERROR),
                           JSON_VALUE('{"n":"abcd"}', 'lax $.n')
                    """;
            try ( ResultSet result = statement.executeQuery( query ) ) {
                assertTrue( result.next() );
                assertEquals( "ab", result.getString( 1 ) );
                assertEquals( "abc", result.getString( 2 ) );
                assertEquals( "xy", result.getString( 3 ) );
                assertEquals( "abcd", result.getString( 4 ) );
                assertFalse( result.next() );
            }
        } );
    }

}
