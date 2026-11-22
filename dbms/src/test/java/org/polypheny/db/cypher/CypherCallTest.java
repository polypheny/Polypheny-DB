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

package org.polypheny.db.cypher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherCallTest extends CypherTestTemplate {

    @BeforeEach
    void resetGraph() {
        tearDown();
        createGraph();
    }


    @Test
    void runsTheDocumentedReturningSubquery() {
        execute( """
                CREATE (:Person {name:'John Doe', age:30}),
                       (:Person {name:'Jane Doe', age:42}),
                       (:Person {name:'Alice', age:20})
                """ );
        GraphResult result = execute( """
                CALL { MATCH (p:Person {name:'John Doe'}) RETURN p.age AS age }
                MATCH (p:Person) WHERE p.age > age
                RETURN p
                """ );
        assertEquals( 1, result.getData().length );
        assertEquals( PolyString.of( "Jane Doe" ), PolyValue.fromTypedJson( result.getData()[0][0], PolyValue.class )
                .asNode().getProperties().get( PolyString.of( "name" ) ) );
    }


    @Test
    void listsAndCountsDistinctNodeLabelsAndFiltersAnAlias() {
        execute( "CREATE (:Person:Employee)-[:KNOWS]->(:Person:Friend), (:Person), ()" );
        GraphResult count = execute( "CALL db.labels() YIELD label RETURN count(*) AS numberOfLabels" );
        assertEquals( "numberOfLabels", count.getHeader()[0].name );
        assertEquals( 3, number( count, 0, 0 ) );

        for ( String query : List.of( "CALL db.labels()", "CALL db.labels() YIELD *" ) ) {
            GraphResult result = execute( query );
            assertEquals( "label", result.getHeader()[0].name );
            assertEquals( List.of( "Employee", "Friend", "Person" ), strings( result ).stream().sorted().toList() );
        }
        assertEquals( List.of( "Person" ), strings( execute( """
                CALL db.labels() YIELD label AS value WHERE value STARTS WITH 'P'
                RETURN value
                """ ) ) );
    }


    @Test
    void listsNoLabelsInAnEmptyGraph() {
        GraphResult labels = execute( "CALL db.labels() YIELD label" );
        assertEquals( "label", labels.getHeader()[0].name );
        assertEquals( 0, labels.getData().length );
        assertEquals( 0, scalar( "CALL db.labels() YIELD label RETURN count(*)" ) );
    }


    @Test
    void runsTheSubqueryForEachOuterRowAndDiscardsUnmatchedRows() {
        GraphResult result = execute( """
                UNWIND [5, 6] AS position
                CALL { UNWIND [1, 2] AS x RETURN x AS item }
                RETURN position, item ORDER BY position, item
                """ );
        assertEquals( 4, result.getData().length );
        assertEquals( 5, number( result, 0, 0 ) );
        assertEquals( 1, number( result, 0, 1 ) );
        assertEquals( 5, number( result, 1, 0 ) );
        assertEquals( 2, number( result, 1, 1 ) );
        assertEquals( 6, number( result, 2, 0 ) );
        assertEquals( 1, number( result, 2, 1 ) );
        assertEquals( 6, number( result, 3, 0 ) );
        assertEquals( 2, number( result, 3, 1 ) );
        assertEquals( 0, scalar( "UNWIND [] AS v CALL { RETURN 5 AS item } RETURN count(*)" ) );
        assertEquals( 0, scalar( "UNWIND [1, 2] AS v CALL { MATCH (:Missing) RETURN 5 AS item } RETURN count(*)" ) );

        execute( "CREATE (:Seed), (:Seed)" );
        assertEquals( 2, scalar( "MATCH (:Seed) CALL { RETURN 5 AS item } RETURN count(*)" ) );
        assertEquals( 2, scalar( "MATCH (:Seed) CALL db.labels() YIELD label WHERE label = 'Seed' RETURN count(*)" ) );
    }


    @Test
    void importsNamedRowsExplicitlyAndKeepsSubqueryLocalsPrivate() {
        GraphResult result = execute( """
                UNWIND [1, 2] AS x
                CALL { WITH x WITH x * 2 AS hidden RETURN hidden * 5 AS y }
                RETURN x, y ORDER BY x
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( 1, number( result, 0, 0 ) );
        assertEquals( 10, number( result, 0, 1 ) );
        assertEquals( 2, number( result, 1, 0 ) );
        assertEquals( 20, number( result, 1, 1 ) );
        assertNotNull( CypherConnection.executeGetResponse( "WITH 3 AS x CALL { RETURN x AS y } RETURN y" ).getError() );
        assertNotNull( CypherConnection.executeGetResponse( "CALL { WITH 3 AS hidden RETURN hidden AS y } RETURN hidden" ).getError() );
        assertError( "WITH 3 AS x CALL { WITH x RETURN x } RETURN x", "already bound variable" );
    }


    @Test
    void validatesProcedureNameArgumentsAndYieldColumns() {
        assertError( "CALL db.unknown()", "Unknown Cypher procedure" );
        assertError( "CALL db.labels(1)", "takes no arguments" );
        assertError( "CALL db.labels() YIELD missing", "Unknown output" );
        assertError( "CALL db.labels() RETURN 1", "requires an explicit YIELD" );
        assertError( "WITH 1 AS label CALL db.labels() YIELD label RETURN label", "already bound variable" );
        assertError( "CALL db.labels() YIELD label AS duplicate, label AS duplicate", "already bound variable" );
    }


    @Test
    void readsUpdatesFromAPrecedingClauseInTheSameStatement() {
        assertEquals( List.of( "Fresh" ), strings( execute( """
                FOREACH (value IN [1] | CREATE (:Fresh {value:value}))
                CALL db.labels() YIELD label WHERE label = 'Fresh'
                RETURN label
                """ ) ) );
        assertError( "CALL { CREATE (:Unexpected) RETURN 1 AS value } RETURN value", "single read-only subquery" );
        assertEquals( 0, scalar( "MATCH (:Unexpected) RETURN count(*)" ) );
    }


    private static long scalar( String query ) {
        return number( execute( query ), 0, 0 );
    }


    private static long number( GraphResult result, int row, int column ) {
        return PolyValue.fromTypedJson( result.getData()[row][column], PolyValue.class ).asNumber().longValue();
    }


    private static List<String> strings( GraphResult result ) {
        return Arrays.stream( result.getData() ).map( row -> PolyValue.fromTypedJson( row[0], PolyValue.class ).asString().value ).toList();
    }


    private static void assertError( String query, String fragment ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error );
        assertTrue( error.contains( fragment ), error );
    }
}
