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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.gson.JsonParser;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)(hsqldb|mongodb|file)")
class MqlExprDocumentDigestTest {

    private static final String NAMESPACE = "mql_expr_document_digest_tests";


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
    @ValueSource(booleans = { true, false })
    void fieldNameBoundariesKeepDocumentComparisonsDistinct( boolean parameterizeDml ) {
        String collection = parameterizeDml ? "prepared_documents" : "literal_documents";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        boolean previousMode = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        RuntimeConfig.PARAMETERIZE_DML.setBoolean( parameterizeDml );
        try {
            execute( collection, """
                    insertMany([
                        {"_id":"first","n":{"a, b":1,"c":2},"s":1,"t":2},
                        {"_id":"other","n":{"a":1,"c":2},"s":1,"t":2},
                        {"_id":"second","n":{"a":1,"b, c":2},"s":1,"t":2}
                    ])
                    """ );

            String first = """
                    {"$expr":{"$eq":["$n",{"a, b":"$s","c":"$t"}]}}
                    """.strip();
            String second = """
                    {"$expr":{"$eq":["$n",{"a":"$s","b, c":"$t"}]}}
                    """.strip();
            assertDistinctComparisons( collection, first, second );
        } finally {
            RuntimeConfig.PARAMETERIZE_DML.setBoolean( previousMode );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void fieldPathBoundariesKeepComparisonsDistinct( boolean parameterizeDml ) {
        AlgDataType type = DocumentType.ofDoc();
        Operator access = OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_EXPR_QUERY_VALUE );
        RexCall firstPath = new RexCall( type, access, new RexNameRef( List.of( "a, b", "c" ), null, type ) );
        RexCall secondPath = new RexCall( type, access, new RexNameRef( List.of( "a", "b, c" ), null, type ) );
        assertNotEquals( firstPath.toString(), secondPath.toString() );

        String collection = parameterizeDml ? "prepared_paths" : "literal_paths";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        boolean previousMode = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        RuntimeConfig.PARAMETERIZE_DML.setBoolean( parameterizeDml );
        try {
            execute( collection, """
                    insertMany([
                        {"_id":"first","a, b":{"c":1},"a":{"b, c":0},"value":1},
                        {"_id":"other","a, b":{"c":0},"a":{"b, c":0},"value":1},
                        {"_id":"second","a, b":{"c":0},"a":{"b, c":1},"value":1}
                    ])
                    """ );

            String first = """
                    {"$expr":{"$eq":["$a, b.c","$value"]}}
                    """.strip();
            String second = """
                    {"$expr":{"$eq":["$a.b, c","$value"]}}
                    """.strip();
            assertDistinctComparisons( collection, first, second );
        } finally {
            RuntimeConfig.PARAMETERIZE_DML.setBoolean( previousMode );
        }
    }


    private static void assertDistinctComparisons( String collection, String first, String second ) {
        String and = "{\"$and\":[" + first + "," + second + "]}";
        String or = "{\"$or\":[" + first + "," + second + "]}";
        String nor = "{\"$nor\":[" + first + "," + second + "]}";

        assertIds( collection, first, "first" );
        assertIds( collection, second, "second" );
        assertIds( collection, or, "first", "second" );
        assertIds( collection, and );
        assertDeleted( collection, and, 0 );
        assertIds( collection, "{}", "first", "other", "second" );

        assertIds( collection, nor, "other" );
        assertDeleted( collection, nor, 1 );
        assertIds( collection, "{}", "first", "second" );
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
