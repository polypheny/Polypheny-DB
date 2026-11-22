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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.polypheny.db.TestHelper;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)(hsqldb|mongodb|file)")
class MqlDecimal128ArrayFilterTest {

    private static final String NAMESPACE = "mql_decimal128_array_filter_tests";


    @BeforeAll
    static void createNamespace() {
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( NAMESPACE );
    }


    @AfterAll
    static void dropNamespace() {
        MqlTestTemplate.dropDatabase( NAMESPACE );
    }


    @Test
    void negatedRangeKeepsArraysWithADirectMatchingElement() {
        String collection = "numeric_ranges";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"array","n":[{"$numberLong":"1"},2]},
                    {"_id":"bigger","n":2},
                    {"_id":"boundary","n":[{"$numberDecimal":"1"}]},
                    {"_id":"less","n":0},
                    {"_id":"missing"},
                    {"_id":"nested","n":[[2]]},
                    {"_id":"string","n":["2"]}
                ])
                """ );

        assertIds( collection, "{\"n\":{\"$eq\":1.9}}" );

        String range = "{\"n\":{\"%s\":{\"$numberDecimal\":\"1\"}}}";
        String greaterThan = range.formatted( "$gt" );
        assertIds( collection, greaterThan, "array", "bigger" );
        assertIds( collection, range.formatted( "$gte" ), "array", "bigger", "boundary" );
        assertIds( collection, range.formatted( "$lt" ), "less" );
        assertIds( collection, range.formatted( "$lte" ), "array", "boundary", "less" );

        String negatedRange = "{\"$nor\":[" + greaterThan + "]}";
        assertIds( collection, negatedRange, "boundary", "less", "missing", "nested", "string" );
        String[] deleteCounts = execute( collection, "deleteMany(" + negatedRange + ")" ).getData();
        assertEquals( 5L, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", "array", "bigger" );
    }


    @Test
    void nestedArrayMatchesItsWholeValueAndItsDirectElements() {
        String collection = "nested_equality";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        String decimalArray = "[{\"$numberDecimal\":\"0.1\"}]";
        execute( collection, """
                insertMany([
                    {"_id":"element","n":[%1$s]},
                    {"_id":"whole","n":%1$s},
                    {"_id":"deeper","n":[[%1$s]]},
                    {"_id":"double","n":[[0.1]]},
                    {"_id":"different","n":[[{"$numberDecimal":"0.100000000000000000000000000001"}]]}
                ])
                """.formatted( decimalArray ) );

        assertIds( collection, "{\"n\":{\"$eq\":" + decimalArray + "}}", "element", "whole" );
        assertIds( collection, "{\"n\":{\"$in\":[" + decimalArray + "]}}", "element", "whole" );

        String notIn = "{\"n\":{\"$nin\":[" + decimalArray + "]}}";
        assertIds( collection, notIn, "deeper", "different", "double" );
        String[] deleteCounts = execute( collection, "deleteMany(" + notIn + ")" ).getData();
        assertEquals( 3L, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", "element", "whole" );
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
