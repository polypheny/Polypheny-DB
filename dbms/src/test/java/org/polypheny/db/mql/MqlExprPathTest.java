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
class MqlExprPathTest {

    private static final String NAMESPACE = "mql_expr_path_tests";


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
    void expressionRangeKeepsDocumentWhoseDottedPathYieldsAnArray() {
        String collection = "path_range";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"array","n":[{"x":0}]},
                    {"_id":"higher","n":{"x":2}},
                    {"_id":"lower","n":{"x":0}},
                    {"_id":"missing"}
                ])
                """ );

        String filter = "{\"$expr\":{\"$lt\":[\"$n.x\",1]}}";
        assertIds( collection, filter, "lower", "missing" );
        assertDeleted( collection, filter, 2 );
        assertIds( collection, "{}", "array", "higher" );
    }


    @Test
    void expressionPathPreservesNestedArraysAndSkipsMissingElements() {
        String collection = "nested_path";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"arrayvalues","n":[{"x":null},{"x":1},{},2,null,{"x":[0]}]},
                    {"_id":"empty","n":[]},
                    {"_id":"missing"},
                    {"_id":"nested","n":[{"x":[{"y":1},{"y":null}]},{"x":{"y":2}},{},{"x":[[{"y":3}]]}]},
                    {"_id":"nonobject","n":[0,["x"],null]},
                    {"_id":"numeric","n":[{"0":{"y":1}},{"y":2}]}
                ])
                """ );

        assertIds( collection, "{\"$expr\":{\"$eq\":[\"$n.x\",[null,1,[0]]]}}", "arrayvalues" );
        assertIds( collection, "{\"$expr\":{\"$eq\":[\"$n.x\",[]]}}", "empty", "nonobject", "numeric" );
        assertIds( collection, "{\"$expr\":{\"$eq\":[\"$n.0.y\",[1]]}}", "numeric" );

        String filter = "{\"$expr\":{\"$eq\":[\"$n.x.y\",[[1,null],2,[]]]}}";
        assertIds( collection, filter, "nested" );
        assertDeleted( collection, filter, 1 );
        assertIds( collection, "{}", "arrayvalues", "empty", "missing", "nonobject", "numeric" );
    }


    private static void assertIds( String collection, String filter, String... expected ) {
        List<String> ids = Arrays.stream( execute( collection, "find(" + filter + ")" ).getData() )
                .map( row -> JsonParser.parseString( row ).getAsJsonObject().get( "_id" ).getAsString() )
                .sorted()
                .toList();
        assertEquals( List.of( expected ), ids, filter );
    }


    private static void assertDeleted( String collection, String filter, long expected ) {
        String[] deleteCounts = execute( collection, "deleteMany(" + filter + ")" ).getData();
        assertEquals( expected, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
    }


    private static DocResult execute( String collection, String operation ) {
        return MqlTestTemplate.execute( "db." + collection + "." + operation, NAMESPACE );
    }

}
