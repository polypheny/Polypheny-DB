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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)(hsqldb|mongodb|file)")
class MqlWholeValueFilterTest {

    private static final String NAMESPACE = "mql_whole_value_filter_tests";
    private static final String DECIMAL = "{\"$numberDecimal\":\"0.1\"}";


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
    @ValueSource(booleans = { false, true })
    void expressionEqualityComparesWholeValues( boolean fieldReference ) {
        String collection = fieldReference ? "expression_field" : "expression_literal";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"array","n":[1.0],"rhs":1.0},
                    {"_id":"decimal","n":{"$numberDecimal":"1.0"},"rhs":1.0},
                    {"_id":"adjacent","n":{"$numberDecimal":"1.00000000000000000001"},"rhs":1.0},
                    {"_id":"other","n":[2.0],"rhs":1.0},
                    {"_id":"scalar","n":1.0,"rhs":1.0}
                ])
                """ );

        String operand = fieldReference ? "\"$rhs\"" : "1.0";
        String eq = "{\"$expr\":{\"$eq\":[\"$n\"," + operand + "]}}";
        String ne = "{\"$expr\":{\"$ne\":[\"$n\"," + operand + "]}}";
        String nor = "{\"$nor\":[" + ne + "]}";
        assertIds( collection, eq, "decimal", "scalar" );
        assertIds( collection, ne, "adjacent", "array", "other" );
        assertIds( collection, nor, "decimal", "scalar" );

        String[] deleteCounts = execute( collection, "deleteMany(" + nor + ")" ).getData();
        assertEquals( 2L, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", "adjacent", "array", "other" );
    }


    @ParameterizedTest
    @CsvSource({
            "$lt, false, false",
            "$lte, false, true",
            "$gt, true, false",
            "$gte, true, true"
    })
    void expressionRangeDoesNotMatchNumericArrayElements( String operator, boolean reverse, boolean inclusive ) {
        String collection = "expression_range_" + operator.substring( 1 );
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"array","n":[0]},
                    {"_id":"lower","n":0},
                    {"_id":"equal","n":1},
                    {"_id":"higher","n":2}
                ])
                """ );

        String bound = "{\"$numberDecimal\":\"1\"}";
        String fieldOperator = inclusive ? "$lte" : "$lt";
        String field = "{\"n\":{\"" + fieldOperator + "\":" + bound + "}}";
        String operands = reverse ? bound + ",\"$n\"" : "\"$n\"," + bound;
        String expression = "{\"$expr\":{\"" + operator + "\":[" + operands + "]}}";
        String[] matches = inclusive ? new String[]{ "equal", "lower" } : new String[]{ "lower" };
        String[] survivors = inclusive ? new String[]{ "array", "higher" } : new String[]{ "array", "equal", "higher" };
        assertIds( collection, field, inclusive ? new String[]{ "array", "equal", "lower" } : new String[]{ "array", "lower" } );
        assertIds( collection, expression, matches );
        assertIds( collection, "{\"$nor\":[" + expression + "]}", survivors );

        String[] deleteCounts = execute( collection, "deleteMany(" + expression + ")" ).getData();
        assertEquals( matches.length, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", survivors );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shorthandEqualityMatchesTheEntireOrderedDocument( boolean array ) {
        String collection = array ? "shorthand_array" : "shorthand_document";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        String original = "{\"z\":1,\"a\":2,\"n\":" + DECIMAL + "}";
        String reordered = "{\"a\":2,\"z\":1,\"n\":" + DECIMAL + "}";
        String extra = "{\"z\":1,\"a\":2,\"n\":" + DECIMAL + ",\"extra\":4}";
        if ( array ) {
            original = "[" + original + "]";
            reordered = "[" + reordered + "]";
            extra = "[" + extra + "]";
        }
        execute( collection, """
                insertMany([
                    {"_id":"exact","p":%s},
                    {"_id":"reordered","p":%s},
                    {"_id":"extra","p":%s}
                ])
                """.formatted( original, reordered, extra ) );

        String shorthand = "{\"p\":" + original + "}";
        assertIds( collection, "{\"p\":{\"$eq\":" + original + "}}", "exact" );
        assertIds( collection, shorthand, "exact" );
        assertIds( collection, "{\"p\":" + reordered + "}", "reordered" );

        String[] deleteCounts = execute( collection, "deleteMany(" + shorthand + ")" ).getData();
        assertEquals( 1L, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", "extra", "reordered" );
    }


    @Test
    void expressionDocumentMatchesTheEntireOrderedValue() {
        String collection = "expression_document";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"exact","n":{"x":1,"d":%1$s}},
                    {"_id":"reordered","n":{"d":%1$s,"x":1}},
                    {"_id":"extra","n":{"x":1,"d":%1$s,"y":2}},
                    {"_id":"scalar","n":1}
                ])
                """.formatted( DECIMAL ) );

        String operand = "{\"x\":1,\"d\":" + DECIMAL + "}";
        String expression = "{\"$expr\":{\"$eq\":[\"$n\"," + operand + "]}}";
        assertIds( collection, expression, "exact" );
        assertIds( collection, "{\"$expr\":{\"$eq\":[\"$n\",{\"$literal\":" + operand + "}]}}", "exact" );

        String[] deleteCounts = execute( collection, "deleteMany(" + expression + ")" ).getData();
        assertEquals( 1L, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", "extra", "reordered", "scalar" );
    }


    @Test
    void expressionDocumentEvaluatesNestedOperands() {
        String collection = "expression_recursive_document";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"calculated","n":{"x":[1],"nested":{"result":3,"ok":true},"text":"$source"},"source":1},
                    {"_id":"literal","n":{"x":["$source"],"nested":{"result":3,"ok":true},"text":"$source"},"source":1}
                ])
                """ );

        String expression = """
                {"$expr":{"$eq":["$n",{
                    "x":["$source"],
                    "nested":{"result":{"$add":["$source",2]},"ok":{"$gt":["$source",0]}},
                    "text":{"$literal":"$source"}
                }]}}
                """;
        assertIds( collection, expression, "calculated" );
        assertIds( collection, """
                {"$expr":{"$eq":["$n",{"$literal":{"x":["$source"],"nested":{"result":3,"ok":true},"text":"$source"}}]}}
                """, "literal" );

        String[] deleteCounts = execute( collection, "deleteMany(" + expression + ")" ).getData();
        assertEquals( 1L, Arrays.stream( deleteCounts ).mapToLong( Long::parseLong ).sum() );
        assertIds( collection, "{}", "literal" );
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
