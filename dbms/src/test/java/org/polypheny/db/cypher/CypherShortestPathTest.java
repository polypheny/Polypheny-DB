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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherShortestPathTest extends CypherTestTemplate {

    @BeforeEach
    void createPaths() {
        tearDown();
        createGraph();
        execute( """
                CREATE (a:Origin {key:'a'})-[:ROAD {cost:0}]->(b:Destination {key:'b'}),
                       (a)-[:ROAD {cost:1}]->(x:Relay {key:'x'})-[:ROAD {cost:1}]->(b),
                       (a)-[:ROAD {cost:1}]->(:Relay {key:'y'})-[:ROAD {cost:1}]->(b),
                       (x)-[:ROAD {cost:1}]->(:Destination {key:'c'}),
                       (:Disconnected {key:'d'}),
                       (:LoopStart)-[:LINK]->(:LoopEnd)
                """ );
    }


    @Test
    void selectsOneShortestPathPerOrderedEndpointPair() {
        GraphResult result = execute( """
                MATCH (a:Origin {key:'a'}), (b:Destination),
                      p = shortestPath((a)-[steps:ROAD*]->(b))
                RETURN b.key AS destination, length(p) AS hops, size(steps) AS steps, p AS path
                ORDER BY destination
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( "b", cell( result, 0, 0 ).asString().value );
        assertEquals( 1, number( result, 0, 1 ) );
        assertEquals( 1, number( result, 0, 2 ) );
        assertEquals( "c", cell( result, 1, 0 ).asString().value );
        assertEquals( 2, number( result, 1, 1 ) );
        PolyPath path = cell( result, 1, 3 ).asPath();
        assertEquals( 3, path.getNodes().size() );
        assertEquals( 2, path.getEdges().size() );
        assertEquals( 2, count( """
                MATCH (a:Origin), (b:Destination), p=shortestPath((a)-[:ROAD*]->(b))
                WHERE a.key = 'a' AND b.key = 'c'
                RETURN length(p)
                """ ) );
    }


    @Test
    void honorsDirectionTypeBoundsAndRelationshipProperties() {
        assertEquals( 1, count( "MATCH p=shortestPath((:Origin)-[:ROAD*..1]->(:Destination)) RETURN count(p)" ) );
        assertEquals( 0, count( "MATCH p=shortestPath((:Destination)-[:ROAD*]->(:Origin)) RETURN count(p)" ) );
        assertEquals( 1, count( "MATCH p=shortestPath((:Destination {key:'b'})<-[:ROAD*]-(:Origin)) RETURN length(p)" ) );
        assertEquals( 2, count( "MATCH p=shortestPath((:Origin)-[:ROAD* {cost:1}]->(:Destination {key:'b'})) RETURN length(p)" ) );
        assertEquals( 2, count( "MATCH p=shortestPath((:Origin)-[:ROAD*2..2]->(:Destination {key:'b'})) RETURN length(p)" ) );
        assertEquals( 0, count( "MATCH p=shortestPath((:Origin)-[:LINK*]->(:Destination)) RETURN count(p)" ) );
    }


    @Test
    void returnsAllShortestTiesAndSupportsTheRelationshipListAlias() {
        GraphResult result = execute( """
                MATCH p=allShortestPaths((:Origin)-[:ROAD* {cost:1}]->(:Destination))
                RETURN last(nodes(p)).key AS destination, nodes(p)[1].key AS via,
                       size(rels(p)) AS hops, rels(p) = relationships(p) AS aliasMatches
                ORDER BY destination, via
                """ );
        assertEquals( 3, result.getData().length );
        assertEquals( "b", cell( result, 0, 0 ).asString().value );
        assertEquals( "x", cell( result, 0, 1 ).asString().value );
        assertEquals( "b", cell( result, 1, 0 ).asString().value );
        assertEquals( "y", cell( result, 1, 1 ).asString().value );
        assertEquals( "c", cell( result, 2, 0 ).asString().value );
        for ( int i = 0; i < result.getData().length; i++ ) {
            assertEquals( 2, number( result, i, 2 ) );
            assertTrue( cell( result, i, 3 ).asBoolean().value );
        }
        assertEquals( 0, count( "MATCH p=allShortestPaths((:Destination)-[:ROAD*]->(:Origin)) RETURN count(p)" ) );
        assertEquals( 2, count( """
                MATCH p=allShortestPaths((:Destination {key:'b'})<-[:ROAD*2..2]-(:Origin))
                RETURN count(p)
                """ ) );
    }


    @Test
    void appliesPathPredicatesBeforeKeepingTiesForEachOptionalInputRow() {
        GraphResult result = execute( """
                MATCH (a:Origin), (b:Destination {key:'b'})
                UNWIND [0,1,1,2] AS minimum
                OPTIONAL MATCH p=allShortestPaths((a)-[:ROAD*]->(b))
                WHERE length(p) > minimum
                RETURN minimum, length(p) AS hops ORDER BY minimum
                """ );
        assertEquals( 6, result.getData().length );
        assertEquals( 1, number( result, 0, 1 ) );
        for ( int i = 1; i < 5; i++ ) {
            assertEquals( 2, number( result, i, 1 ) );
        }
        assertNull( cell( result, 5, 1 ) );
    }


    @Test
    void returnsAListOfShortestPathsForBoundEndpointsInAnExpression() {
        assertEquals( 2, count( """
                MATCH (a:Origin), (b:Destination {key:'b'})
                RETURN size(allShortestPaths((a)-[:ROAD* {cost:1}]->(b)))
                """ ) );
        assertEquals( 0, count( """
                MATCH (a:Origin), (b:Disconnected)
                RETURN size(allShortestPaths((a)-[:ROAD*]->(b)))
                """ ) );
        GraphResult missing = execute( """
                MATCH (a:Origin)
                OPTIONAL MATCH (b:Missing)
                RETURN allShortestPaths((a)-[:ROAD*]->(b))
                """ );
        assertEquals( 1, missing.getData().length );
        assertNull( cell( missing, 0, 0 ) );
    }


    @Test
    void filtersTiesAndLongerAlternativesBeforeSelectingAShortestMatch() {
        assertEquals( 2, count( """
                MATCH (a:Origin), (b:Destination {key:'b'}), p=shortestPath((a)-[:ROAD*]->(b))
                WHERE length(p) > 1
                RETURN length(p)
                """ ) );
        assertEquals( 0, count( """
                MATCH p=shortestPath((:Origin)-[:ROAD*]->(:Destination {key:'b'}))
                WITH p WHERE length(p) > 1
                RETURN count(p)
                """ ) );
        assertEquals( 2, count( """
                MATCH (:Relay), p=shortestPath((:Origin)-[:ROAD*]->(:Destination {key:'b'}))
                WHERE length(p) > 1
                RETURN count(p)
                """ ) );
        assertEquals( 2, count( """
                WITH -1 AS cost
                MATCH p=shortestPath((:Origin)-[:ROAD* {cost:abs(cost)}]->(:Destination {key:'b'}))
                RETURN length(p)
                """ ) );
    }


    @Test
    void preservesDuplicateOuterRowsAndOptionalNoMatch() {
        GraphResult result = execute( """
                MATCH (a:Origin), (b:Destination {key:'b'})
                UNWIND [0,0,1,2] AS minimum
                OPTIONAL MATCH p=shortestPath((a)-[:ROAD*]->(b))
                WHERE length(p) > minimum
                RETURN minimum, length(p) AS hops ORDER BY minimum
                """ );
        assertEquals( 4, result.getData().length );
        assertEquals( 1, number( result, 0, 1 ) );
        assertEquals( 1, number( result, 1, 1 ) );
        assertEquals( 2, number( result, 2, 1 ) );
        assertNull( cell( result, 3, 1 ) );

        GraphResult disconnected = execute( """
                OPTIONAL MATCH p=shortestPath((:Origin)-[:ROAD*]->(:Disconnected))
                RETURN p
                """ );
        assertEquals( 1, disconnected.getData().length );
        assertNull( cell( disconnected, 0, 0 ) );
    }


    @Test
    void respectsExistingBindingsAndRelationshipUniqueness() {
        assertEquals( 2, count( """
                MATCH (a:Origin)-[:ROAD {cost:0}]->(b:Destination {key:'b'}),
                      p=shortestPath((a)-[:ROAD*]->(b))
                RETURN length(p)
                """ ) );
        GraphResult result = execute( """
                MATCH path=(a:Origin)-[steps:ROAD*2]->(b:Destination {key:'b'})
                WITH a, b, steps
                MATCH p=shortestPath((a)-[steps:ROAD*]->(b))
                RETURN length(p)
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( 2, number( result, 0, 0 ) );
        assertEquals( 2, number( result, 1, 0 ) );
    }


    @Test
    void distinguishesZeroHopsFromANonemptyClosedTrail() {
        assertEquals( 0, count( "MATCH p=shortestPath((a:LoopStart)-[:LINK*0..]-(a)) RETURN length(p)" ) );
        assertEquals( 0, count( "MATCH p=shortestPath((a:LoopStart)-[:LINK*]-(a)) RETURN count(p)" ) );
        execute( "MATCH (a:LoopStart),(b:LoopEnd) CREATE (a)-[:LINK]->(b)" );
        assertEquals( 2, count( "MATCH p=shortestPath((a:LoopStart)-[:LINK*]-(a)) RETURN length(p)" ) );
    }


    @Test
    void rejectsAPathShapeWithMultipleRelationshipPatterns() {
        String message = CypherConnection.executeGetResponse( "MATCH p=shortestPath((a)-[]->(x)-[]->(b)) RETURN p" ).getError();
        assertNotNull( message );
        assertTrue( message.contains( "requires exactly one relationship pattern" ), message );
    }


    private static long count( String query ) {
        GraphResult result = execute( query );
        assertEquals( 1, result.getData().length );
        return number( result, 0, 0 );
    }


    private static long number( GraphResult result, int row, int column ) {
        return cell( result, row, column ).asNumber().longValue();
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        String value = result.getData()[row][column];
        return value == null ? null : PolyValue.fromTypedJson( value, PolyValue.class );
    }
}
