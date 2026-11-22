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
import org.polypheny.db.TestHelper;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)hsqldb")
class MqlDecimal128HsqldbTest {

    private static final String NAMESPACE = "mql_decimal128_hsqldb_tests";
    private static final String COLLECTION = "amounts";
    private static final String DECIMAL = "{\"$numberDecimal\":\"1.00000000000000000000000000000001\"}";


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
    void exactDecimalFilterDoesNotDeleteRoundedDouble() {
        execute( """
                insertMany([
                    {"_id":"decimal","n":%1$s,"list":[%1$s],"nested":{"n":%1$s}},
                    {"_id":"double","n":1.0,"list":[1.0],"nested":{"n":1.0}}
                ])
                """.formatted( DECIMAL ) );

        assertIds( "{\"n\":" + DECIMAL + "}", "decimal" );
        assertIds( "{\"n\":{\"$in\":[" + DECIMAL + "]}}", "decimal" );
        assertIds( "{\"n\":{\"$ne\":" + DECIMAL + "}}", "double" );
        assertIds( "{\"n\":{\"$nin\":[" + DECIMAL + "]}}", "double" );
        assertIds( "{\"n\":{\"$lt\":" + DECIMAL + "}}", "double" );
        assertIds( "{\"n\":{\"$gte\":" + DECIMAL + "}}", "decimal" );
        assertIds( "{\"list\":" + DECIMAL + "}", "decimal" );
        assertIds( "{\"list\":{\"$eq\":[" + DECIMAL + "]}}", "decimal" );
        assertIds( "{\"nested\":{\"$eq\":{\"n\":" + DECIMAL + "}}}", "decimal" );
        assertIds( "{\"nested\":{\"$ne\":{\"n\":" + DECIMAL + "}}}", "double" );

        assertArrayEquals( new String[]{ "1" }, execute( "deleteMany({\"n\":" + DECIMAL + "})" ).getData() );
        assertIds( "{}", "double" );

        execute( """
                insertMany([{"_id":"missing"},{"_id":"null","n":null}])
                """ );
        assertIds( "{\"n\":{\"$ne\":" + DECIMAL + "}}", "double", "missing", "null" );
        assertIds( "{\"n\":{\"$nin\":[" + DECIMAL + "]}}", "double", "missing", "null" );
    }


    private static void assertIds( String filter, String... expected ) {
        List<String> ids = Arrays.stream( execute( "find(" + filter + ")" ).getData() )
                .map( row -> JsonParser.parseString( row ).getAsJsonObject().get( "_id" ).getAsString() )
                .sorted()
                .toList();
        assertEquals( List.of( expected ), ids, filter );
    }


    private static DocResult execute( String operation ) {
        return MqlTestTemplate.execute( "db." + COLLECTION + "." + operation, NAMESPACE );
    }

}
