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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;

@Tag("adapter")
class CypherPatternPredicateContextTest {

    private static final String NAMESPACE = "cypherpatternpredicatecontext";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
            connection.execute( NAMESPACE, "cypher", "CREATE (:Seed {id:1})-[:R]->(:Target), (:Seed {id:2})", 1 );
        }
    }


    @AfterAll
    static void dropGraph() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
        }
    }


    @Test
    void evaluatesBarePatternsInBooleanFilterPositions() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertEquals( 1, count( connection, """
                    MATCH (a:Seed)
                    WITH a AS source WHERE (source)-[:R]->()
                    RETURN count(*) AS matches
                    """ ) );
            assertEquals( 1, count( connection, """
                    MATCH (a:Seed)
                    WITH a.id AS id WHERE (a)-[:R]->()
                    RETURN count(*) AS matches
                    """ ) );
            assertEquals( 1, count( connection, """
                    MATCH (a:Seed)
                    WITH collect(a) AS candidates
                    WHERE any(item IN candidates WHERE (item)-[:R]->())
                    RETURN count(*) AS matches
                    """ ) );
            assertEquals( 2, count( connection, """
                    MATCH (a:Seed)
                    CALL db.labels() YIELD label WHERE (a)-[:R]->()
                    RETURN count(*) AS matches
                    """ ) );
            assertEquals( 0, count( connection, """
                    MATCH (a:Seed)
                    WITH a WHERE null
                    RETURN count(*) AS matches
                    """ ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsBarePatternsInValuePositionsInsidePredicates() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String predicate : List.of(
                    "[(a)-[:R]->()] = [true]",
                    "{flag:(a)-[:R]->()}.flag",
                    "toString((a)-[:R]->()) = 'true'",
                    "((a)-[:R]->()) IN [true]",
                    "NOT ([(a)-[:R]->()] = [false])",
                    "any(item IN [(a)-[:R]->()] WHERE item)",
                    "CASE true WHEN true THEN (a)-[:R]->() ELSE false END" ) ) {
                assertPatternError( connection, "MATCH (a:Seed) WHERE " + predicate + " RETURN a.id AS id", "Use exists(pattern)" );
            }
            assertPatternError( connection, "MATCH (a:Seed) WITH a WHERE [(a)-[:R]->()] = [true] RETURN a.id AS id", "Use exists(pattern)" );
            assertPatternError( connection, "MATCH (a:Seed) RETURN (a)-[:R]->()", "Use exists(pattern)" );
            assertEquals( 1, count( connection, """
                    MATCH (a:Seed)
                    WHERE [exists((a)-[:R]->())] = [true]
                       AND [NOT ((a)-[:R]->())] = [false]
                    RETURN count(*) AS matches
                    """ ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void validatesDynamicallyTypedPatternBindings() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertPatternError( connection, """
                    WITH {value:42} AS m WITH m.value AS a
                    RETURN NOT ((a)-[:R]->()) AS present
                    """, "MATCH requires a NODE binding" );
            assertPatternError( connection, """
                    WITH {value:42} AS m WITH m.value AS a
                    RETURN exists(()-[:R]->(a)-[:R]->()) AS present
                    """, "MATCH requires a NODE binding" );
            assertPatternError( connection, """
                    MATCH (a:Seed)
                    RETURN any(x IN [42,a] WHERE (x)-[:R]->()) AS present
                    """, "MATCH requires a NODE binding" );
            assertPatternError( connection, """
                    WITH {node:null, rel:42} AS m WITH m.node AS a, m.rel AS r
                    RETURN exists((a)-[r*]->()) AS present
                    """, "MATCH requires a relationship list binding" );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void matchesRelationshipBindingsAndPreservesNulls() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.execute( NAMESPACE, "cypher", """
                    MATCH (a:Seed)-[r:R]->(b:Target)
                    WITH a, b, {edge:r, trail:[r], mixed:[r,42], missing:[null],
                               empty:[], num:42, text:'R', nil:null} AS m
                    WITH a, b, m.edge AS edge, m.trail AS trail, m.mixed AS mixed,
                         m.missing AS missing, m.empty AS empty, m.num AS num, m.text AS txt, m.nil AS nil
                    RETURN exists((a)-[edge]->(b)) AS edgeMatches,
                           exists((a)-[trail*]->(b)) AS trailMatches,
                           exists((a)-[empty*0]->(a)) AS emptyMatches,
                           exists((a)-[num]->(b)) AS numericEdge,
                           exists((a)-[txt*]->(b)) AS stringTrail,
                           exists((a)-[mixed*]->(b)) AS mixedTrail,
                           exists((a)-[missing*]->(b)) AS nullItemTrail,
                           exists((nil)-[:R]->(b)) AS nullNode,
                           exists((a)-[nil]->(b)) AS nullEdge,
                           exists((a)-[nil*]->(b)) AS nullTrail
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( frame, true, List.of( "edgeMatches", "trailMatches", "emptyMatches", "numericEdge", "stringTrail",
                    "mixedTrail", "nullItemTrail", "nullNode", "nullEdge", "nullTrail" ) ) );
            for ( int i = 0; i < row.getValuesCount(); i++ ) {
                assertTrue( row.getValues( i ).hasBoolean() );
                assertEquals( i < 3, row.getValues( i ).getBoolean().getBoolean() );
            }
            connection.checkTransactions( 0 );
        }
    }


    private static void assertPatternError( PrismConnection connection, String query, String expectedMessage ) throws IOException {
        Request.Builder request = Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( NAMESPACE )
                .setLanguageName( "cypher" )
                .setStatement( query )
                .setFetchSize( -1 ) );
        Response response = connection.send( request );
        assertEquals( Response.TypeCase.ERROR_RESPONSE, response.getTypeCase(), query );
        String message = response.getErrorResponse().getMessage();
        assertTrue( message.contains( expectedMessage ), () -> query + " failed with: " + message );
    }


    private static long count( PrismConnection connection, String query ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return integerValue( singleRow( relationalFrame( frame, true, List.of( "matches" ) ) ).getValues( 0 ) );
    }

}
