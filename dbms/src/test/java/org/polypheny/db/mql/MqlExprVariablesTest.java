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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)(hsqldb|mongodb|file)")
class MqlExprVariablesTest {

    private static final String NAMESPACE = "mql_expr_variables_tests";


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
    @ValueSource(strings = { "$$ROOT", "$$CURRENT" })
    void expressionVariableResolvesDottedPath( String variable ) {
        String collection = variable.substring( 2 ).toLowerCase( Locale.ROOT ) + "_path";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"array","n":[{"value":0}]},
                    {"_id":"higher","n":{"value":2}},
                    {"_id":"lower","n":{"value":0}}
                ])
                """ );

        String filter = "{\"$expr\":{\"$lt\":[\"%s.n.value\",1]}}".formatted( variable );
        assertIds( collection, filter, "lower" );
        String nested = "{\"$expr\":{\"$eq\":[\"$n\",{\"value\":\"%s.n.value\"}]}}".formatted( variable );
        assertIds( collection, nested, "higher", "lower" );
        assertDeleted( collection, filter, 1 );
        assertIds( collection, "{}", "array", "higher" );
    }


    @ParameterizedTest
    @ValueSource(strings = { "$$ROOT", "$$CURRENT" })
    void bareExpressionVariableResolvesWholeDocument( String variable ) {
        String collection = variable.substring( 2 ).toLowerCase( Locale.ROOT ) + "_document";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([{"_id":"keep"},{"_id":"remove"}])
                """ );

        String filter = "{\"$expr\":{\"$eq\":[\"%s\",{\"$literal\":{\"_id\":\"remove\"}}]}}".formatted( variable );
        assertIds( collection, filter, "remove" );
        assertDeleted( collection, filter, 1 );
        assertIds( collection, "{}", "keep" );
    }


    @Test
    void invalidExpressionVariablesFailBeforeDeletingAndLiteralValuesRemainData() {
        String collection = "invalid_variable";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        execute( collection, """
                insertMany([
                    {"_id":"literal","n":0,"text":"$$UNKNOWN.n"},
                    {"_id":"other","n":2,"text":"other"}
                ])
                """ );

        String unknown = "{\"$expr\":{\"$lt\":[\"$$UNKNOWN.n\",1]}}";
        RuntimeException error = assertThrows( RuntimeException.class, () -> execute( collection, "deleteMany(" + unknown + ")" ) );
        assertTrue( error.getMessage().contains( "$$UNKNOWN" ), error.getMessage() );

        String malformed = "{\"$expr\":{\"$lt\":[\"$$ROOT.\",1]}}";
        RuntimeException malformedPath = assertThrows( RuntimeException.class, () -> execute( collection, "deleteMany(" + malformed + ")" ) );
        assertTrue( malformedPath.getMessage().contains( "$$ROOT." ), malformedPath.getMessage() );

        assertIds( collection, "{}", "literal", "other" );
        assertIds( collection, "{\"$expr\":{\"$eq\":[\"$text\",{\"$literal\":\"$$UNKNOWN.n\"}]}}", "literal" );
    }


    private static void assertIds( String collection, String filter, String... expected ) {
        List<String> ids = Arrays.stream( execute( collection, "find(" + filter + ")" ).getData() )
                .map( row -> JsonParser.parseString( row ).getAsJsonObject().get( "_id" ).getAsString() )
                .sorted()
                .toList();
        assertEquals( List.of( expected ), ids, filter );
    }


    private static void assertDeleted( String collection, String filter, long expected ) {
        String[] counts = execute( collection, "deleteMany(" + filter + ")" ).getData();
        assertEquals( expected, Arrays.stream( counts ).mapToLong( Long::parseLong ).sum() );
    }


    private static DocResult execute( String collection, String operation ) {
        return MqlTestTemplate.execute( "db." + collection + "." + operation, NAMESPACE );
    }

}
