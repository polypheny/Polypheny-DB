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

package org.polypheny.db.mql;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)hsqldb|mongodb")
class MqlDecimal128NegationTest {

    private static final String NAMESPACE = "mql_decimal128_negation_tests";


    @BeforeAll
    static void createNamespace() {
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( NAMESPACE );
    }


    @AfterAll
    static void dropNamespace() {
        MqlTestTemplate.dropDatabase( NAMESPACE );
    }


    @ParameterizedTest
    @ValueSource(strings = { "$ne", "$nin" })
    void negatedDoubleFilterKeepsEqualDecimal128( String operator ) {
        String collection = "ordinary_double_" + operator.substring( 1 );
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        // Both the double literal and the exact Decimal128 equal 1 + 2^-30.
        String doubleLiteral = "1.0000000009313226";
        execute( collection, """
                insertMany([
                    {"_id":"exact","n":{"$numberDecimal":"1.000000000931322574615478515625"}},
                    {"_id":"double","n":%s},
                    {"_id":"adjacent","n":{"$numberDecimal":"1.000000000931322574615478515626"}}
                ])
                """.formatted( doubleLiteral ) );

        assertIds( collection, "{\"n\":{\"$eq\":" + doubleLiteral + "}}", "double", "exact" );
        String operand = operator.equals( "$nin" ) ? "[" + doubleLiteral + "]" : doubleLiteral;
        String filter = "{\"n\":{\"%s\":%s}}".formatted( operator, operand );
        assertIds( collection, filter, "adjacent" );
        assertArrayEquals( new String[]{ "1" }, execute( collection, "deleteMany(" + filter + ")" ).getData() );
        assertIds( collection, "{}", "double", "exact" );
    }


    @Test
    void norOfDecimal128InequalityDeletesOnlyTheExactMatch() {
        String collection = "decimal_nor";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        String decimal = "{\"$numberDecimal\":\"0.1\"}";
        execute( collection, """
                insertMany([
                    {"_id":"decimal","n":%s},
                    {"_id":"double","n":0.1},
                    {"_id":"missing"},
                    {"_id":"null","n":null}
                ])
                """.formatted( decimal ) );

        String inequality = "{\"n\":{\"$ne\":" + decimal + "}}";
        assertIds( collection, inequality, "double", "missing", "null" );
        String filter = "{\"$nor\":[" + inequality + "]}";
        assertIds( collection, filter, "decimal" );
        assertArrayEquals( new String[]{ "1" }, execute( collection, "deleteMany(" + filter + ")" ).getData() );
        assertIds( collection, "{}", "double", "missing", "null" );
    }


    @Test
    void mixedNinKeepsArraysWithAMatchingDoubleElement() {
        String collection = "mixed_nin";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"keep","n":[1.0]},
                    {"_id":"remove","n":[1.1]}
                ])
                """ );

        String filter = "{\"n\":{\"$nin\":[{\"$numberDecimal\":\"5\"},1.0]}}";
        assertIds( collection, filter, "remove" );
        assertArrayEquals( new String[]{ "1" }, execute( collection, "deleteMany(" + filter + ")" ).getData() );
        assertIds( collection, "{}", "keep" );
    }


    private static void assertIds( String collection, String filter, String... expected ) {
        List<String> ids = Arrays.stream( execute( collection, "find(" + filter + ")" ).getData() )
                .map( row -> JsonParser.parseString( row ).getAsJsonObject().get( "_id" ).getAsString() )
                .sorted()
                .toList();
        assertEquals( List.of( expected ), ids, filter );
    }


    private static DocResult execute( String collection, String operation ) {
        return MqlTestTemplate.execute( "db." + collection + "." + operation, NAMESPACE );
    }

}
