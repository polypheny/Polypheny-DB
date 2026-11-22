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
class MqlExprBsonOrderTest {

    private static final String NAMESPACE = "mql_expr_bson_order_tests";


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
    void expressionRangeOrdersNaNBeforeFiniteNumbers() {
        String collection = "nan_order";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"finite","n":2,"rhs":0.0},
                    {"_id":"lower","n":0,"rhs":1.0},
                    {"_id":"nan","n":{"$numberDouble":"NaN"},"rhs":{"$numberDouble":"-Infinity"}}
                ])
                """ );

        String expression = "{\"$expr\":{\"$lt\":[\"$n\",{\"$numberDecimal\":\"1\"}]}}";
        assertIds( collection, expression, "lower", "nan" );
        assertIds( collection, "{\"$expr\":{\"$lt\":[\"$n\",\"$rhs\"]}}", "lower", "nan" );
        assertIds( collection, "{\"n\":{\"$lt\":{\"$numberDecimal\":\"1\"}}}", "lower" );

        String negated = "{\"$nor\":[" + expression + "]}";
        assertIds( collection, negated, "finite" );
        assertDeleted( collection, negated, 1 );
        assertIds( collection, "{}", "lower", "nan" );
    }


    @Test
    void expressionRangeUsesBsonTypeOrder() {
        String collection = "type_order";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"adjacent","n":{"$numberDecimal":"1.000000000000000000000000000001"}},
                    {"_id":"array","n":[0]},
                    {"_id":"boolean","n":false},
                    {"_id":"document","n":{"x":0}},
                    {"_id":"equal","n":1.0},
                    {"_id":"lower","n":0},
                    {"_id":"missing"},
                    {"_id":"null","n":null},
                    {"_id":"string","n":"abc"}
                ])
                """ );

        String expression = "{\"$expr\":{\"$gt\":[\"$n\",{\"$numberDecimal\":\"1\"}]}}";
        assertIds( collection, expression, "adjacent", "array", "boolean", "document", "string" );
        assertIds( collection, "{\"n\":{\"$gt\":{\"$numberDecimal\":\"1\"}}}", "adjacent" );

        String negated = "{\"$nor\":[" + expression + "]}";
        assertIds( collection, negated, "equal", "lower", "missing", "null" );
        assertDeleted( collection, negated, 4 );
        assertIds( collection, "{}", "adjacent", "array", "boolean", "document", "string" );
    }


    @Test
    void expressionRangeComparesArrayElementsAndDocumentFieldsInOrder() {
        String collection = "compound_order";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"arrayfirst","n":[2],"rhs":[1,99]},
                    {"_id":"arrayprefix","n":[{"x":[1]}],"rhs":[{"x":[1,0]}]},
                    {"_id":"fieldfirst","n":{"z":1,"a":2},"rhs":{"a":1,"z":2}},
                    {"_id":"fieldprefix","n":{"x":1},"rhs":{"x":1,"y":0}},
                    {"_id":"fieldtype","n":{"z":2},"rhs":{"a":"1"}},
                    {"_id":"missingvsnull","rhs":null},
                    {"_id":"unicodefield","n":{"\uE000":1},"rhs":{"\uD800\uDC00":1}},
                    {"_id":"unicodestring","n":"\uE000","rhs":"\uD800\uDC00"}
                ])
                """ );

        String expression = "{\"$expr\":{\"$lt\":[\"$n\",\"$rhs\"]}}";
        assertIds( collection, expression, "arrayprefix", "fieldprefix", "fieldtype", "missingvsnull", "unicodefield", "unicodestring" );
        assertDeleted( collection, expression, 6 );
        assertIds( collection, "{}", "arrayfirst", "fieldfirst" );
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
