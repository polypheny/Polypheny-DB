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
                assertPatternSyntaxError( connection, "MATCH (a:Seed) WHERE " + predicate + " RETURN a.id AS id" );
            }
            assertPatternSyntaxError( connection, "MATCH (a:Seed) WITH a WHERE [(a)-[:R]->()] = [true] RETURN a.id AS id" );
            assertPatternSyntaxError( connection, "MATCH (a:Seed) RETURN (a)-[:R]->()" );
            assertEquals( 1, count( connection, """
                    MATCH (a:Seed)
                    WHERE [exists((a)-[:R]->())] = [true]
                       AND [NOT ((a)-[:R]->())] = [false]
                    RETURN count(*) AS matches
                    """ ) );
            connection.checkTransactions( 0 );
        }
    }


    private static void assertPatternSyntaxError( PrismConnection connection, String query ) throws IOException {
        Request.Builder request = Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( NAMESPACE )
                .setLanguageName( "cypher" )
                .setStatement( query )
                .setFetchSize( -1 ) );
        Response response = connection.send( request );
        assertEquals( Response.TypeCase.ERROR_RESPONSE, response.getTypeCase(), query );
        String message = response.getErrorResponse().getMessage();
        assertTrue( message.contains( "Use exists(pattern)" ), () -> query + " failed with: " + message );
    }


    private static long count( PrismConnection connection, String query ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return integerValue( singleRow( relationalFrame( frame, true, List.of( "matches" ) ) ).getValues( 0 ) );
    }

}
