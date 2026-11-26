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

package org.polypheny.db.cypher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;


class CypherUnicodeTest extends CypherTestTemplate {

    @Test
    void decodesUtf16EscapesWithinStringDelimiters() {
        GraphResult result = execute( """
                RETURN '\\u0027' AS singleQuote, "\\u0022" AS doubleQuote,
                       '\\u005c' AS backslash, '\\uD83D\\uDE00' AS supplementary,
                       '\\\\u0027' AS escapedBackslash
                """ );

        assertEquals( "'", value( result, 0, 0 ).asString().value );
        assertEquals( "\"", value( result, 0, 1 ).asString().value );
        assertEquals( "\\", value( result, 0, 2 ).asString().value );
        assertEquals( Character.toString( 0x1F600 ), value( result, 0, 3 ).asString().value );
        assertEquals( "\\u0027", value( result, 0, 4 ).asString().value );
    }


    @Test
    void rejectsUnknownStringEscapes() {
        for ( String literal : List.of( "'\\q'", "\"\\q\"" ) ) {
            String error = CypherConnection.executeGetResponse( "RETURN " + literal ).getError();
            assertNotNull( error, literal );
            assertTrue( error.contains( "UnexpectedSyntax" ), error );
        }
    }


    @Test
    void rejectsMalformedUtf16Escapes() {
        for ( String escape : List.of( "\\uH", "\\u002", "\\uu0027" ) ) {
            String error = CypherConnection.executeGetResponse( "RETURN '" + escape + "'" ).getError();
            assertNotNull( error, escape );
            assertTrue( error.contains( "InvalidUnicodeLiteral" ), error );
        }
    }


    @Test
    void preservesLineFeedThenCarriageReturnWhenEvaluatingEscapes() {
        GraphResult result = execute( """
                RETURN '\\n\\r' AS classicEscapes, '\\u000A\\u000D\\\\r' AS unicodeEscapes
                """ );

        assertEquals( "\n\r", value( result, 0, 0 ).asString().value );
        assertEquals( "\n\r\\r", value( result, 0, 1 ).asString().value );
    }


    @Test
    void decodesUtf32EscapesInBothQuoteStyles() {
        GraphResult result = execute( """
                RETURN 'a\\U00010000b' AS singleQuoted, "\\U0001f600" AS doubleQuoted,
                       '\\U00000041' AS bmp, size('\\U0010FFFF') AS highestScalarLength,
                       '\\\\U00010000' AS escapedBackslash
                """ );

        assertEquals( "a" + Character.toString( 0x10000 ) + "b", value( result, 0, 0 ).asString().value );
        assertEquals( Character.toString( 0x1F600 ), value( result, 0, 1 ).asString().value );
        assertEquals( "A", value( result, 0, 2 ).asString().value );
        assertEquals( 1, value( result, 0, 3 ).asNumber().intValue() );
        assertEquals( "\\U00010000", value( result, 0, 4 ).asString().value );
    }


    @Test
    void rejectsMalformedOrNonScalarUtf32Escapes() {
        for ( String escape : List.of( "\\U0001000", "\\U0001000Z", "\\U00110000", "\\U0000D800", "\\UFFFFFFFF" ) ) {
            String error = CypherConnection.executeGetResponse( "RETURN '" + escape + "'" ).getError();
            assertNotNull( error, escape );
            assertTrue( error.contains( "InvalidUnicodeLiteral" ), error );
        }
    }


    @Test
    void comparesAndSortsStringsByUnicodeCodePoint() {
        String bmp = Character.toString( 0xE000 );
        String supplementary = Character.toString( 0x10000 );
        GraphResult comparison = execute( """
                WITH '%s' AS bmp, '%s' AS supplementary
                RETURN bmp < supplementary AS compared,
                       [bmp] < [supplementary] AS nested,
                       supplementary < supplementary + 'a' AS prefix
                """.formatted( bmp, supplementary ) );
        for ( int i = 0; i < comparison.getHeader().length; i++ ) {
            assertTrue( value( comparison, 0, i ).asBoolean().value );
        }

        for ( String ordering : List.of( "text", "[text]", "{value: text}" ) ) {
            GraphResult sorted = execute( """
                    UNWIND ['%s', '%s'] AS text
                    RETURN text ORDER BY %s
                    """.formatted( supplementary, bmp, ordering ) );
            assertEquals( bmp, value( sorted, 0, 0 ).asString().value );
            assertEquals( supplementary, value( sorted, 1, 0 ).asString().value );
        }
    }


    private static PolyValue value( GraphResult result, int row, int column ) {
        return PolyValue.fromTypedJson( result.getData()[row][column], PolyValue.class );
    }

}
