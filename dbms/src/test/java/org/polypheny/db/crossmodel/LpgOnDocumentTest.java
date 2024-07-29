/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.crossmodel;

import static java.lang.String.format;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.CypherTestTemplate.Row;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.util.Pair;

public class LpgOnDocumentTest extends CrossModelTestTemplate {

    private static final String DATABASE_NAME = "crossdocumentschema";

    private static final String COLLECTION_NAME = "crosscollection";

    public static final Map<String, Object> TEST_MAP = Map.of(
            "test", 3,
            "key", "value",
            "_id", "630103687f2e95058018fd9b" );
    public static final String TEST_DATA = TEST_MAP.entrySet().stream().map( e -> {
        String value = "" + e.getValue();
        if ( e.getValue() instanceof String ) {
            value = "\"" + e.getValue() + "\"";
        }
        return format( "\"%s\": %s", e.getKey(), value );
    } ).collect( Collectors.joining( ",", "{", "}" ) );


    @BeforeAll
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( DATABASE_NAME );
        MqlTestTemplate.createCollection( COLLECTION_NAME, DATABASE_NAME );
        MqlTestTemplate.insert( TEST_DATA, COLLECTION_NAME, DATABASE_NAME );
    }


    @AfterAll
    public static void tearDown() {
        MqlTestTemplate.dropDatabase( DATABASE_NAME );
    }


    public static Row dataAsRow() {
        return Row.of( TestNode.from( List.of( COLLECTION_NAME ), TEST_MAP.entrySet().stream().filter( e -> !e.getKey().equals( "_id" ) ).map( e -> Pair.of( e.getKey(), e.getValue() ) ).toList().toArray( new Pair[0] ) ) );
    }


    @Test
    public void simpleMatchTest() {
        CypherTestTemplate.containsRows(
                CypherTestTemplate.execute( format( "MATCH (n:%s) RETURN n", COLLECTION_NAME ), DATABASE_NAME ),
                true,
                false,
                dataAsRow() );
    }


    @Test
    public void emptyMatchTest() {
        CypherTestTemplate.containsRows(
                CypherTestTemplate.execute( format( "MATCH (n:%s) RETURN n", "random" ), DATABASE_NAME ),
                true,
                false );
    }

}
