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


package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.prism.CloseResultRequest;
import org.polypheny.prism.CloseStatementRequest;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ConnectionProperties;
import org.polypheny.prism.ConnectionRequest;
import org.polypheny.prism.DisconnectRequest;
import org.polypheny.prism.EntitiesRequest;
import org.polypheny.prism.EntitiesResponse;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.FetchRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CypherTest {

    private static final String NAMESPACE = "cyphertest";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
            StatementResult created = connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 ).getResult();
            assertFalse( created.hasFrame() );
            assertEquals( 1, created.getScalar() );
            StatementResult inserted = connection.cypher(
                    "CREATE (:Person {id: 1, name: 'Alice'})-[:KNOWS {since: 2020}]->"
                            + "(:Person {id: 2, name: 'Bob'})-[:KNOWS {since: 2021}]->"
                            + "(:Person {id: 3, name: 'Charlie'})", 1 ).getResult();
            assertFalse( inserted.hasFrame() );
            connection.checkTransactions( 0 );
        }
    }


    @AfterAll
    static void dropGraph() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
        }
    }


    @Test
    void pagesNodeResultsAndClosesTheAutocommitTransaction() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            StatementResponse statement = connection.cypher( "MATCH (n:Person) RETURN n ORDER BY n.id", 1 );
            Frame first = statement.getResult().getFrame();
            assertNode( graphElements( first, false ).get( 0 ), "Alice", 1 );
            assertEquals( 1, first.getGraphFrame().getElementCount() );
            connection.checkTransactions( 1 );

            Frame second = connection.fetch( statement.getStatementId(), 1 );
            assertNode( graphElements( second, false ).get( 0 ), "Bob", 2 );
            assertEquals( 1, second.getGraphFrame().getElementCount() );
            connection.checkTransactions( 1 );

            Frame last = connection.fetch( statement.getStatementId(), 1 );
            assertNode( graphElements( last, true ).get( 0 ), "Charlie", 3 );
            assertEquals( 1, last.getGraphFrame().getElementCount() );
            connection.checkTransactions( 0 );

            Response exhausted = connection.fetchResponse( statement.getStatementId(), 1 );
            expect( exhausted, Response.TypeCase.ERROR_RESPONSE );
            assertTrue( exhausted.getErrorResponse().getMessage().contains( "No open Cypher result" ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void closingAPartiallyFetchedResultEndsItsAutocommitTransaction() throws IOException {
        assertCloseEndsAutocommitTransaction( Response.TypeCase.CLOSE_RESULT_RESPONSE );
    }


    @Test
    void closingAStatementWithAPartiallyFetchedResultEndsItsAutocommitTransaction() throws IOException {
        assertCloseEndsAutocommitTransaction( Response.TypeCase.CLOSE_STATEMENT_RESPONSE );
    }


    private void assertCloseEndsAutocommitTransaction( Response.TypeCase closeResponse ) throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            StatementResponse statement = connection.cypher( "MATCH (n:Person) RETURN n ORDER BY n.id", 1 );
            assertEquals( 1, graphElements( statement.getResult().getFrame(), false ).size() );
            connection.checkTransactions( 1 );

            int statementId = statement.getStatementId();
            Request.Builder close = Request.newBuilder();
            if ( closeResponse == Response.TypeCase.CLOSE_RESULT_RESPONSE ) {
                close.setCloseResultRequest( CloseResultRequest.newBuilder().setStatementId( statementId ) );
            } else {
                close.setCloseStatementRequest( CloseStatementRequest.newBuilder().setStatementId( statementId ) );
            }
            expect( connection.send( close ), closeResponse );
            connection.checkTransactions( 0 );

            Frame next = connection.cypher( "MATCH (n:Person) RETURN n ORDER BY n.id", 3 ).getResult().getFrame();
            assertEquals( 3, graphElements( next, true ).size() );
        }
    }


    @Test
    void preservesManualTransactionsAfterTheFinalPage() throws IOException {
        try ( PrismConnection connection = new PrismConnection( false ) ) {
            StatementResponse statement = connection.cypher( "MATCH (n:Person) RETURN n ORDER BY n.id", 2 );
            assertEquals( 2, graphElements( statement.getResult().getFrame(), false ).size() );
            assertNode( graphElements( connection.fetch( statement.getStatementId(), 2 ), true ).get( 0 ), "Charlie", 3 );
            connection.checkTransactions( 1 );

            expect( connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) ), Response.TypeCase.COMMIT_RESPONSE );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesAllGraphColumnsAndEdgeEndpoints() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            List<GraphElement> elements = graphElements( connection.cypher(
                    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) RETURN a, r, b", 1 ).getResult().getFrame(), true );
            assertEquals( 3, elements.size() );
            ProtoNode alice = assertNode( elements.get( 0 ), "Alice", 1 );
            ProtoNode bob = assertNode( elements.get( 2 ), "Bob", 2 );
            assertEdge( elements.get( 1 ), alice, bob, 2020 );
        }
    }


    @Test
    void expandsPathsInTraversalOrder() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            List<GraphElement> path = graphElements( connection.cypher(
                    "MATCH p=(:Person {name: 'Alice'})-[:KNOWS*1..3]->(:Person {name: 'Charlie'}) RETURN p", 1 ).getResult().getFrame(), true );
            assertEquals( 5, path.size() );
            ProtoNode alice = assertNode( path.get( 0 ), "Alice", 1 );
            ProtoNode bob = assertNode( path.get( 2 ), "Bob", 2 );
            ProtoNode charlie = assertNode( path.get( 4 ), "Charlie", 3 );
            assertEdge( path.get( 1 ), alice, bob, 2020 );
            assertEdge( path.get( 3 ), bob, charlie, 2021 );
        }
    }


    @Test
    void returnsAllNodesAndEdgesForAFullGraphScan() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            List<GraphElement> elements = graphElements( connection.cypher(
                    "MATCH (*) RETURN *", 1 ).getResult().getFrame(), true );
            assertEquals( 5, elements.size() );
            var nodeIds = elements.stream().filter( GraphElement::hasNode ).map( GraphElement::getNode )
                    .collect( Collectors.toMap( n -> n.getPropertiesOrThrow( "name" ).getString().getString(), ProtoNode::getId ) );
            assertEquals( Set.of( "Alice", "Bob", "Charlie" ), nodeIds.keySet() );
            Set<List<String>> endpoints = elements.stream().filter( GraphElement::hasEdge ).map( GraphElement::getEdge )
                    .map( e -> List.of( e.getLeft(), e.getRight() ) ).collect( Collectors.toSet() );
            assertEquals( Set.of(
                    List.of( nodeIds.get( "Alice" ), nodeIds.get( "Bob" ) ),
                    List.of( nodeIds.get( "Bob" ), nodeIds.get( "Charlie" ) ) ), endpoints );
        }
    }


    @Test
    void returnsAnEmptyGraphFrameForNoMatches() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            Frame frame = connection.cypher( "MATCH (n:Person {name: 'Unknown'}) RETURN n", 1 ).getResult().getFrame();
            assertTrue( graphElements( frame, true ).isEmpty() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void returnsScalarProjectionsInRelationalFrames() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            StatementResponse statement = connection.cypher( "MATCH (n:Person) RETURN n.name AS name ORDER BY name", 2 );
            Frame first = statement.getResult().getFrame();
            assertFalse( first.getIsLast() );
            assertTrue( first.hasRelationalFrame() );
            RelationalFrame firstPage = first.getRelationalFrame();
            assertEquals( "name", firstPage.getColumnMeta( 0 ).getColumnLabel() );
            assertEquals( List.of( "Alice", "Bob" ), projectedNames( firstPage ) );

            Frame last = connection.fetch( statement.getStatementId(), 2 );
            assertTrue( last.getIsLast() );
            assertTrue( last.hasRelationalFrame() );
            assertEquals( firstPage.getColumnMetaList(), last.getRelationalFrame().getColumnMetaList() );
            assertEquals( List.of( "Charlie" ), projectedNames( last.getRelationalFrame() ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsMixedGraphAndScalarProjectionsInEitherOrder() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            for ( String projection : List.of( "n, n.name", "n.name, n" ) ) {
                Response response = connection.send( statementRequest( NAMESPACE, "cypher", "MATCH (n:Person) RETURN " + projection, 1 ) );
                expect( response, Response.TypeCase.ERROR_RESPONSE );
                String message = response.getErrorResponse().getMessage();
                assertTrue( message.contains( "graph" ) && message.contains( "scalar" ), message );
                connection.checkTransactions( 0 );
            }
            Frame next = connection.cypher( "MATCH (n:Person) RETURN n ORDER BY n.id", 3 ).getResult().getFrame();
            assertEquals( 3, graphElements( next, true ).size() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void discoversGraphEntitiesThroughPrismMetadata() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            EntitiesRequest.Builder request = EntitiesRequest.newBuilder().setNamespaceName( NAMESPACE );
            EntitiesResponse unfiltered = graphEntities( connection, request );
            assertEquals( 1, unfiltered.getEntitiesCount() );
            assertTrue( unfiltered.getEntities( 0 ).hasGraph() );

            assertEquals( unfiltered, graphEntities( connection, request.setEntityPattern( "cypher%" ) ) );
            assertEquals( 0, graphEntities( connection, request.setEntityPattern( "missing%" ) ).getEntitiesCount() );
            connection.checkTransactions( 0 );
        }
    }


    private static EntitiesResponse graphEntities( PrismConnection connection, EntitiesRequest.Builder request ) throws IOException {
        return expect( connection.send( Request.newBuilder().setEntitiesRequest( request ) ), Response.TypeCase.ENTITIES_RESPONSE ).getEntitiesResponse();
    }


    private static List<GraphElement> graphElements( Frame frame, boolean isLast ) {
        assertTrue( frame.hasGraphFrame(), () -> "Expected a graph frame but got " + frame.getResultCase() );
        assertEquals( isLast, frame.getIsLast() );
        return frame.getGraphFrame().getElementList();
    }


    private static ProtoNode assertNode( GraphElement element, String name, int id ) {
        assertTrue( element.hasNode() );
        ProtoNode node = element.getNode();
        assertFalse( node.getId().isBlank() );
        assertEquals( List.of( "Person" ), node.getLabelsList() );
        assertEquals( name, node.getPropertiesOrThrow( "name" ).getString().getString() );
        assertEquals( id, integerValue( node.getPropertiesOrThrow( "id" ) ) );
        return node;
    }


    private static void assertEdge( GraphElement element, ProtoNode from, ProtoNode to, int since ) {
        assertTrue( element.hasEdge() );
        ProtoEdge edge = element.getEdge();
        assertFalse( edge.getId().isBlank() );
        assertEquals( List.of( "KNOWS" ), edge.getLabelsList() );
        assertEquals( since, integerValue( edge.getPropertiesOrThrow( "since" ) ) );
        assertEquals( from.getId(), edge.getLeft() );
        assertEquals( to.getId(), edge.getRight() );
        assertEquals( ProtoEdge.Direction.LEFT_TO_RIGHT, edge.getDirection() );
    }


    private static long integerValue( ProtoValue value ) {
        return switch ( value.getValueCase() ) {
            case INTEGER -> value.getInteger().getInteger();
            case LONG -> value.getLong().getLong();
            // Cross-model graph storage can represent integral properties as strings.
            case STRING -> Long.parseLong( value.getString().getString() );
            default -> throw new AssertionError( "Expected a numeric graph property but got " + value.getValueCase() );
        };
    }


    private static List<String> projectedNames( RelationalFrame frame ) {
        return frame.getRowsList().stream().map( row -> row.getValues( 0 ).getString().getString() ).toList();
    }


    private static Request.Builder statementRequest( String namespace, String language, String query, int fetchSize ) {
        return Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( namespace )
                .setLanguageName( language )
                .setStatement( query )
                .setFetchSize( fetchSize ) );
    }


    private static Response expect( Response response, Response.TypeCase type ) {
        assertEquals( type, response.getTypeCase(), () -> response.getErrorResponse().getMessage() );
        return response;
    }


    /** Uses the JDBC plain transport with the server's unshaded Prism messages. */
    private static class PrismConnection implements AutoCloseable {

        private final PlainTransport transport;
        private long nextRequestId = 1;


        PrismConnection( boolean autocommit ) throws IOException {
            transport = new PlainTransport( "localhost", 20590 );
            try {
                Response response = send( Request.newBuilder().setConnectionRequest( ConnectionRequest.newBuilder()
                        .setMajorApiVersion( 2 )
                        .setMinorApiVersion( 2 )
                        .setUsername( "pa" )
                        .setPassword( "" )
                        .setConnectionProperties( ConnectionProperties.newBuilder().setIsAutoCommit( autocommit ) ) ) );
                assertTrue( expect( response, Response.TypeCase.CONNECTION_RESPONSE ).getConnectionResponse().getIsCompatible() );
            } catch ( IOException | AssertionError e ) {
                transport.close();
                throw e;
            }
        }


        Response send( Request.Builder request ) throws IOException {
            long id = nextRequestId++;
            transport.sendMessage( request.setId( id ).build().toByteArray() );
            Response response;
            do {
                response = Response.parseFrom( transport.receiveMessage() );
                assertEquals( id, response.getId() );
            } while ( !response.getLast() );
            return response;
        }


        StatementResponse execute( String namespace, String language, String query, int fetchSize ) throws IOException {
            Response response = send( statementRequest( namespace, language, query, fetchSize ) );
            StatementResponse statement = expect( response, Response.TypeCase.STATEMENT_RESPONSE ).getStatementResponse();
            assertTrue( statement.hasResult() );
            return statement;
        }


        StatementResponse cypher( String query, int fetchSize ) throws IOException {
            return execute( NAMESPACE, "cypher", query, fetchSize );
        }


        Frame fetch( int statementId, int fetchSize ) throws IOException {
            return expect( fetchResponse( statementId, fetchSize ), Response.TypeCase.FRAME ).getFrame();
        }


        Response fetchResponse( int statementId, int fetchSize ) throws IOException {
            return send( Request.newBuilder().setFetchRequest( FetchRequest.newBuilder()
                    .setStatementId( statementId )
                    .setFetchSize( fetchSize ) ) );
        }


        void checkTransactions( int expected ) {
            TestHelper.getInstance().checkTrxStatus( expected );
        }


        @Override
        public void close() throws IOException {
            try {
                expect( send( Request.newBuilder().setDisconnectRequest( DisconnectRequest.getDefaultInstance() ) ), Response.TypeCase.DISCONNECT_RESPONSE );
            } finally {
                transport.close();
            }
        }

    }

}
