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
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.webui.models.results.DocResult;

@Tag("adapter")
@EnabledIfSystemProperty(named = "store.default", matches = "(?i)(hsqldb|mongodb|file)")
class MqlDocumentFieldOrderTest {

    private static final String NAMESPACE = "mql_document_field_order_tests";
    private static final String DECIMAL = "{\"$numberDecimal\":\"1.00000000000000000000000000000001\"}";
    private String collection;


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
    void embeddedDocumentEqualityAndDeletionRespectFieldOrder( boolean parameterizeDml ) {
        collection = parameterizeDml ? "prepared_documents" : "literal_documents";
        MqlTestTemplate.createCollection( collection, NAMESPACE );
        String original = "{\"a\":1,\"z\":2,\"n\":" + DECIMAL + "}";
        String reordered = "{\"z\":2,\"a\":1,\"n\":" + DECIMAL + "}";
        execute( """
                insertMany([
                    {"_id":"original","obj":%1$s,"array":[%1$s],"plain":{"a":1,"z":2},"empty":{}},
                    {"_id":"reordered","obj":%2$s,"array":[%2$s],"plain":{"z":2,"a":1}}
                ])
                """.formatted( original, reordered ) );

        Map<String, List<String>> storedFieldOrder = Arrays.stream( execute( "find({})" ).getData() )
                .map( row -> JsonParser.parseString( row ).getAsJsonObject() )
                .collect( Collectors.toMap(
                        row -> row.get( "_id" ).getAsString(),
                        row -> List.copyOf( row.getAsJsonObject( "obj" ).keySet() ) ) );
        assertEquals( Map.of(
                "original", List.of( "a", "z", "n" ),
                "reordered", List.of( "z", "a", "n" ) ), storedFieldOrder );

        String originalFilter = "{\"obj\":{\"$eq\":" + original + "}}";
        assertIds( originalFilter, "original" );
        assertIds( "{\"obj\":{\"$eq\":" + reordered + "}}", "reordered" );
        assertIds( "{\"array\":{\"$eq\":[" + original + "]}}", "original" );
        assertIds( "{\"plain\":{\"$eq\":{\"z\":2,\"a\":1}}}", "reordered" );
        assertIds( "{\"empty\":{\"$eq\":{}}}", "original" );

        boolean previousParameterizeDml = RuntimeConfig.PARAMETERIZE_DML.getBoolean();
        RuntimeConfig.PARAMETERIZE_DML.setBoolean( parameterizeDml );
        try {
            assertArrayEquals( new String[]{ "1" }, execute( "deleteMany(" + originalFilter + ")" ).getData() );
            assertIds( "{}", "reordered" );

            assertArrayEquals( new String[]{ "0" }, execute( "deleteMany({\"array\":{\"$eq\":[" + original + "]}})" ).getData() );
            assertArrayEquals( new String[]{ "1" }, execute( "deleteMany({\"array\":{\"$eq\":[" + reordered + "]}})" ).getData() );
            assertIds( "{}" );
        } finally {
            RuntimeConfig.PARAMETERIZE_DML.setBoolean( previousParameterizeDml );
        }
    }


    private void assertIds( String filter, String... expected ) {
        List<String> ids = Arrays.stream( execute( "find(" + filter + ")" ).getData() )
                .map( row -> JsonParser.parseString( row ).getAsJsonObject().get( "_id" ).getAsString() )
                .sorted()
                .toList();
        assertEquals( List.of( expected ), ids, filter );
    }


    private DocResult execute( String operation ) {
        return MqlTestTemplate.execute( "db." + collection + "." + operation, NAMESPACE );
    }

}
