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
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;


class CypherPathWriteSemanticsTest extends CypherTestTemplate {

    @BeforeEach
    void resetGraph() {
        tearDown();
        createGraph();
    }


    @Test
    void unwindAfterEmptyMatchDoesNotCreateRowsOrNodes() {
        for ( String pattern : List.of( "(:Missing)", "(:Missing)-[:R*]->(:Missing)" ) ) {
            GraphResult result = execute( """
                    MATCH %s
                    UNWIND [1, 2] AS i
                    CREATE (:Unexpected {i:i})
                    RETURN i
                    """.formatted( pattern ) );

            assertEquals( 0, result.getData().length, pattern );
            assertEquals( 0, count( "MATCH (n:Unexpected) RETURN count(n)" ), pattern );
        }
    }


    @Test
    void literalUnwindExpandsEachIncomingRowSeparately() {
        execute( "CREATE (:A)-[:R]->(:B), (:A)-[:R]->(:B)" );

        assertEquals( 4, count( "MATCH (:A)-[:R*]->(:B) UNWIND [1, 2] AS i RETURN count(*)" ) );
        assertEquals( 4, count( "MATCH (:A) UNWIND [1, 2] AS i RETURN count(*)" ) );
    }


    @Test
    void unwindingRelationshipListKeepsEndpointBindings() {
        execute( "CREATE (:A {name:'Alice'})-[:R {rank:1}]->()-[:R {rank:2}]->(:B {name:'Bob'})" );

        GraphResult result = execute( """
                MATCH (a:A)-[rs:R*]->(b:B)
                UNWIND rs AS e
                RETURN a.name AS origin, b.name AS target, e.rank AS rank ORDER BY rank
                """ );

        assertEquals( 2, result.getData().length );
        for ( int i = 0; i < 2; i++ ) {
            assertEquals( "Alice", value( result, i, 0 ).asString().value );
            assertEquals( "Bob", value( result, i, 1 ).asString().value );
            assertEquals( i + 1, value( result, i, 2 ).asNumber().intValue() );
        }
    }


    @Test
    void unwindPreservesTheExpandedListInScope() {
        GraphResult result = execute( "WITH [1,2,3] AS list UNWIND list AS x RETURN *" );

        assertEquals( List.of( "list", "x" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        assertEquals( 3, result.getData().length );
        for ( int i = 0; i < 3; i++ ) {
            assertEquals( 3, value( result, i, 0 ).asList().size() );
            assertEquals( i + 1, value( result, i, 1 ).asNumber().intValue() );
        }
    }


    @Test
    void unwindSkipsNullAndEmptyListsButKeepsNullElements() {
        GraphResult result = execute( """
                UNWIND [[], null, [null, 2]] AS xs
                UNWIND xs AS x
                RETURN count(*) AS total, count(x) AS known
                """ );

        assertEquals( 2, value( result, 0, 0 ).asNumber().intValue() );
        assertEquals( 1, value( result, 0, 1 ).asNumber().intValue() );
    }


    @Test
    void detachDeleteRemovesAllNodesAndRelationshipsOfANamedPath() {
        execute( "CREATE (:X)-[:R]->()-[:R]->()-[:R]->()" );
        execute( "MATCH p=(:X)-->()-->()-->() DETACH DELETE p" );

        assertEquals( 0, count( "MATCH (n) RETURN count(n)" ) );
        assertEquals( 0, count( "MATCH ()-[r]->() RETURN count(r)" ) );
    }


    @Test
    void detachDeletePreservesMatchCardinalityWhenPathsOverlap() {
        execute( "CREATE (:X)-[:R]->()-[:R]->()-[:R]->()" );

        assertEquals( 3, count( "MATCH p=(:X)-[:R*]->() DETACH DELETE p RETURN count(*)" ) );
        assertEquals( 0, count( "MATCH (n) RETURN count(n)" ) );
        assertEquals( 0, count( "MATCH ()-[r]->() RETURN count(r)" ) );
    }


    @Test
    void deletingAnOptionalNullPathIsANoOp() {
        assertEquals( 1, count( "OPTIONAL MATCH p=()-[*]->() DETACH DELETE p RETURN count(*)" ) );
        assertEquals( 0, count( "MATCH (n) RETURN count(n)" ) );
    }


    @Test
    void plainPathDeleteChecksForIncidentRelationshipsBeforeWriting() {
        execute( "CREATE (:A)-[:R]->(:B)-[:OTHER]->(:C)" );
        String error = CypherConnection.executeGetResponse( "MATCH p=(:A)-[:R*]->(:B) DELETE p" ).getError();

        assertNotNull( error );
        assertTrue( error.contains( "Cannot delete a node with relationships" ) );
        assertEquals( 3, count( "MATCH (n) RETURN count(n)" ) );
        assertEquals( 2, count( "MATCH ()-[r]->() RETURN count(r)" ) );
    }


    private static long count( String query ) {
        return value( execute( query ), 0, 0 ).asNumber().longValue();
    }


    private static PolyValue value( GraphResult result, int row, int column ) {
        return PolyValue.fromTypedJson( result.getData()[row][column], PolyValue.class );
    }

}
