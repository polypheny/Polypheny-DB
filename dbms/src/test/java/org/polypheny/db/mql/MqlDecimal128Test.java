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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.polypheny.db.TestHelper;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)mongodb")
class MqlDecimal128Test {

    private static final String NAMESPACE = "mql_decimal128_tests";
    private static final String COLLECTION = "amounts";


    @BeforeAll
    static void createCollection() {
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( NAMESPACE );
        MqlTestTemplate.createCollection( COLLECTION, NAMESPACE );
    }


    @AfterAll
    static void dropNamespace() {
        MqlTestTemplate.dropDatabase( NAMESPACE );
    }


    @Test
    void filtersAndWritesPreserveAdjacentDecimal128Values() {
        String first = decimal( "1.00000000000000000000000000000001" );
        String second = decimal( "1.00000000000000000000000000000002" );
        String replacement = decimal( "1.00000000000000000000000000000003" );
        execute( """
                insertMany([
                    {"_id":"first","n":%s},
                    {"_id":"second","n":%s},
                    {"_id":"double","n":1.0}
                ])
                """.formatted( first, second ) );

        assertMatch( first, "first" );
        assertMatch( second, "second" );
        assertMatch( "1.0", "double" );

        assertArrayEquals( new String[]{ "1" }, execute( """
                updateOne({"n":%s},{"$set":{"n":%s}})
                """.formatted( first, replacement ) ).getData() );
        assertEquals( 0, find( first ).length );
        assertMatch( second, "second" );
        assertMatch( replacement, "first" );

        assertArrayEquals( new String[]{ "1" }, execute( "deleteOne({\"n\":" + replacement + "})" ).getData() );
        assertEquals( 0, find( replacement ).length );
        assertMatch( second, "second" );
    }


    private static String decimal( String value ) {
        return "{\"$numberDecimal\":\"" + value + "\"}";
    }


    private static void assertMatch( String value, String id ) {
        String[] data = find( value );
        assertEquals( 1, data.length );
        assertEquals( id, JsonParser.parseString( data[0] ).getAsJsonObject().get( "_id" ).getAsString() );
    }


    private static String[] find( String value ) {
        return execute( "find({\"n\":" + value + "})" ).getData();
    }


    private static DocResult execute( String operation ) {
        return MqlTestTemplate.execute( "db." + COLLECTION + "." + operation, NAMESPACE );
    }

}
