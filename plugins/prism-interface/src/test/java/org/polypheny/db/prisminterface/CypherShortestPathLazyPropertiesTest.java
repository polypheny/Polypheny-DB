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
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;

@Tag("adapter")
class CypherShortestPathLazyPropertiesTest {

    private static final String NAMESPACE = "cyphershortestpathlazyproperties";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
            connection.execute( NAMESPACE, "cypher", """
                    CREATE (a:Start {desired:2, divisor:0})-[:R {cost:1}]->(b:End {ready:true, divisor:0}),
                           (a)-[:R {cost:2}]->(:Relay)-[:R {cost:2}]->(b),
                           (a)-[:R {cost:1}]->(a), (:Island)
                    """, 1 );
        }
    }


    @AfterAll
    static void dropGraph() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
        }
    }


    @Test
    void skipsRelationshipPropertiesWhenTheShortestPathHasZeroHops() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertEquals( 0, integerValue( hops( connection, """
                    MATCH (a:Start)
                    RETURN length(shortestPath((a {desired:2})-[:R*0.. {cost:1 / a.divisor}]->(a {desired:a.desired}))) AS hops
                    """ ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void skipsPropertyExpressionsWhenNoStructuralCandidateExists() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertTrue( hops( connection, """
                    MATCH (a:Start), (b:Island)
                    RETURN length(shortestPath((a {desired:1 / a.divisor})-[:R* {cost:1 / a.divisor}]->(b))) AS hops
                    """ ).hasNull() );
            assertTrue( hops( connection, """
                    MATCH (a:Start), (b:End)
                    RETURN length(shortestPath((b)-[:R*]->(a {desired:1 / a.divisor}))) AS hops
                    """ ).hasNull() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void skipsPropertyExpressionsWhenAnEndpointIsNull() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertTrue( hops( connection, """
                    MATCH (b:End)
                    OPTIONAL MATCH (a:Missing)
                    RETURN length(shortestPath((a)-[:R* {cost:1 / b.divisor}]->(b {ready:1 / b.divisor}))) AS hops
                    """ ).hasNull() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesApplicablePropertiesAndFindsTheShortestFilteredPath() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertEquals( 2, integerValue( hops( connection, """
                    MATCH (a:Start), (b:End)
                    RETURN length(shortestPath((a {desired:a.desired})-[:R* {cost:a.desired}]->(b {ready:b.ready}))) AS hops
                    """ ) ) );
            assertEquals( 1, integerValue( hops( connection, """
                    MATCH (a:Start), (b:End)
                    RETURN length(shortestPath((a)-[:R* {cost:1}]->(b))) AS hops
                    """ ) ) );
            assertTrue( hops( connection, """
                    MATCH (a:Start), (b:End)
                    RETURN length(shortestPath((a)-[:R* {cost:3}]->(b))) AS hops
                    """ ).hasNull() );
            assertTrue( hops( connection, """
                    MATCH (a:Start), (b:End)
                    RETURN length(shortestPath((a)-[:R*]->(b {ready:false}))) AS hops
                    """ ).hasNull() );
            assertPropertyError( connection, """
                    MATCH (a:Start), (b:End)
                    RETURN length(shortestPath((a)-[:R* {cost:1 / a.divisor}]->(b))) AS hops
                    """ );
            assertPropertyError( connection, """
                    MATCH (a:Start)
                    RETURN length(shortestPath((a {desired:1 / a.divisor})-[:R*0..]->(a))) AS hops
                    """ );
            connection.checkTransactions( 0 );
        }
    }


    private static ProtoValue hops( PrismConnection connection, String query ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return singleRow( relationalFrame( frame, true, List.of( "hops" ) ) ).getValues( 0 );
    }


    private static void assertPropertyError( PrismConnection connection, String query ) throws IOException {
        Response response = connection.send( Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( NAMESPACE )
                .setLanguageName( "cypher" )
                .setStatement( query )
                .setFetchSize( -1 ) ) );
        assertEquals( Response.TypeCase.ERROR_RESPONSE, response.getTypeCase(), query );
        String message = response.getErrorResponse().getMessage();
        assertTrue( message.contains( "/ by zero" ), () -> query + " failed with: " + message );
    }

}
