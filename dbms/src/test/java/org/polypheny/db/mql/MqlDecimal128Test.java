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
        assertEquals( 1, execute( """
                find({"_id":"second","n":{"$ne":%s}})
                """.formatted( first ) ).getData().length );
        assertEquals( 0, execute( """
                find({"_id":"first","n":{"$nin":[%s]}})
                """.formatted( first ) ).getData().length );

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


    @Test
    void incrementPreservesBsonNumericTypes() {
        String precise = decimal( "1.00000000000000000000000000000004" );
        String amount = decimal( "0.00000000000000000000000000000001" );
        String incremented = decimal( "1.00000000000000000000000000000005" );
        execute( """
                insertMany([
                    {"_id":"incDouble","n":0.1},
                    {"_id":"incInt","n":1},
                    {"_id":"incLong","n":1},
                    {"_id":"incDecimal","n":%s}
                ])
                """.formatted( precise ) );

        assertIncrement( "incDouble", "0.5", "0.6", "double" );
        assertIncrement( "incInt", "2", "3", "int" );
        assertIncrement( "incLong", "{\"$numberLong\":\"3\"}", "4", "long" );
        assertIncrement( "incDecimal", amount, incremented, "decimal" );
    }


    private static void assertIncrement( String id, String operand, String result, String bsonType ) {
        assertArrayEquals( new String[]{ "1" }, execute( """
                updateOne({"_id":"%s"},{"$inc":{"n":%s}})
                """.formatted( id, operand ) ).getData() );
        assertMatch( result, id );
        assertEquals( 1, execute( """
                find({"_id":"%s","n":{"$type":"%s"}})
                """.formatted( id, bsonType ) ).getData().length );
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
