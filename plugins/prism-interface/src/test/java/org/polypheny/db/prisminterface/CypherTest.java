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
import org.polypheny.prism.ConnectionPropertiesUpdateRequest;
import org.polypheny.prism.ConnectionRequest;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.DisconnectRequest;
import org.polypheny.prism.EntitiesRequest;
import org.polypheny.prism.EntitiesResponse;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.FetchRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.NamespacesRequest;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CypherTest {

    private static final String NAMESPACE = "cyphertest";
    private static final String SQL_PAGE_QUERY = "SELECT n FROM (VALUES ('a'), ('b'), ('c')) AS t(n) ORDER BY n";


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
    void acceptsSemicolonsInLiteralsAndCommentsWithATrailingSeparator() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            Frame frame = connection.cypher( """
                    MATCH (n:Person {name: 'Alice'}) /* ; inside a block comment */
                    RETURN 'literal;semicolon' AS message; // ; inside a line comment
                    """, 1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( frame, true, List.of( "message" ) ) );
            assertEquals( "literal;semicolon", row.getValues( 0 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsMultipleStatementsBeforeExecutingCypherDdl() throws IOException {
        String namespace = "cyphermultistatementtest";
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            try {
                Response rejected = connection.send( statementRequest( NAMESPACE, "cypher",
                        "CREATE DATABASE " + namespace + "; MATCH (n:Person) RETURN n", 1 ) );
                expect( rejected, Response.TypeCase.ERROR_RESPONSE );
                String message = rejected.getErrorResponse().getMessage();
                assertTrue( message.contains( "exactly one Cypher statement" ), message );

                Response namespaces = connection.send( Request.newBuilder().setNamespacesRequest(
                        NamespacesRequest.newBuilder().setNamespacePattern( namespace ) ) );
                assertEquals( 0, expect( namespaces, Response.TypeCase.NAMESPACES_RESPONSE ).getNamespacesResponse().getNamespacesCount() );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + namespace, 1 );
            }
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
    void rejectsOverlappingAutocommitQueriesAndKeepsTheFirstResultOpen() throws IOException {
        String query = "MATCH (n:Person) RETURN n ORDER BY n.id";
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            StatementResponse first = connection.cypher( query, 1 );
            assertNode( graphElements( first.getResult().getFrame(), false ).get( 0 ), "Alice", 1 );
            connection.checkTransactions( 1 );

            assertOpenResultError( connection.send( statementRequest( NAMESPACE, "cypher", query, 3 ) ) );
            assertOpenResultError( connection.send( statementRequest( Catalog.DEFAULT_NAMESPACE_NAME, "sql", SQL_PAGE_QUERY, 2 ) ) );
            assertOpenResultError( connection.send( Request.newBuilder().setPrepareIndexedStatementRequest(
                    PrepareStatementRequest.newBuilder()
                            .setNamespaceName( Catalog.DEFAULT_NAMESPACE_NAME )
                            .setLanguageName( "sql" )
                            .setStatement( SQL_PAGE_QUERY ) ) ) );

            ExecuteUnparameterizedStatementBatchRequest ddlBatch = ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                    .addStatements( ExecuteUnparameterizedStatementRequest.newBuilder()
                            .setNamespaceName( Catalog.DEFAULT_NAMESPACE_NAME )
                            .setLanguageName( "sql" )
                            .setStatement( "DROP NAMESPACE IF EXISTS cypherblockedbatchtest" ) )
                    .build();
            assertOpenResultError( connection.send( Request.newBuilder().setExecuteUnparameterizedStatementBatchRequest( ddlBatch ) ) );

            Response rejectedModeChange = connection.send( Request.newBuilder().setConnectionPropertiesUpdateRequest(
                    ConnectionPropertiesUpdateRequest.newBuilder().setConnectionProperties(
                            ConnectionProperties.newBuilder().setIsAutoCommit( false ) ) ) );
            assertEquals( "Close or finish the open Cypher result before changing autocommit mode",
                    expect( rejectedModeChange, Response.TypeCase.ERROR_RESPONSE ).getErrorResponse().getMessage() );
            connection.checkTransactions( 1 );

            Frame remaining = connection.fetch( first.getStatementId(), 2 );
            List<GraphElement> nodes = graphElements( remaining, true );
            assertEquals( 2, nodes.size() );
            assertNode( nodes.get( 0 ), "Bob", 2 );
            assertNode( nodes.get( 1 ), "Charlie", 3 );
            connection.checkTransactions( 0 );

            Frame retried = connection.cypher( query, 3 ).getResult().getFrame();
            assertEquals( 3, graphElements( retried, true ).size() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsAnAutocommitCypherQueryWhileAnSqlResultIsOpen() throws IOException {
        String query = "MATCH (n:Person) RETURN n ORDER BY n.id";
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            StatementResponse first = connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", SQL_PAGE_QUERY, 1 );
            Frame firstPage = first.getResult().getFrame();
            assertTrue( firstPage.hasRelationalFrame() );
            assertFalse( firstPage.getIsLast() );
            assertEquals( "a", singleRow( firstPage.getRelationalFrame() ).getValues( 0 ).getString().getString() );
            connection.checkTransactions( 1 );

            assertOpenResultError( connection.send( statementRequest( NAMESPACE, "cypher", query, 3 ) ) );
            connection.checkTransactions( 1 );

            Frame remaining = connection.fetch( first.getStatementId(), 2 );
            assertTrue( remaining.hasRelationalFrame() );
            assertTrue( remaining.getIsLast() );
            assertEquals( List.of( "b", "c" ), projectedNames( remaining.getRelationalFrame() ) );

            Frame retried = connection.cypher( query, 3 ).getResult().getFrame();
            assertEquals( 3, graphElements( retried, true ).size() );
            connection.checkTransactions( 0 );
        }
    }


    private static void assertOpenResultError( Response response ) {
        String message = expect( response, Response.TypeCase.ERROR_RESPONSE ).getErrorResponse().getMessage();
        assertTrue( message.contains( "Close or finish the open result" ), message );
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
    void returnsAnAggregateAsRelationalData() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            Frame result = connection.cypher( "MATCH (n:Person) RETURN count(*) AS total", 1 ).getResult().getFrame();
            assertEquals( 3, integerValue( singleRow( relationalFrame( result, true, List.of( "total" ) ) ).getValues( 0 ) ) );
        }
    }


    @Test
    void pagesMixedNodeAndScalarProjectionsInEitherOrder() throws IOException {
        List<String> names = List.of( "Alice", "Bob", "Charlie" );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( boolean nodeFirst : List.of( true, false ) ) {
                String projection = nodeFirst ? "n, n.name AS name" : "n.name AS name, n";
                int nodeColumn = nodeFirst ? 0 : 1;
                int nameColumn = 1 - nodeColumn;
                List<String> labels = nodeFirst ? List.of( "n", "name" ) : List.of( "name", "n" );
                StatementResponse statement = connection.cypher( "MATCH (n:Person) RETURN " + projection + " ORDER BY name", 1 );
                Frame frame = statement.getResult().getFrame();

                for ( int i = 0; i < names.size(); i++ ) {
                    boolean last = i == names.size() - 1;
                    RelationalFrame page = relationalFrame( frame, last, labels );
                    assertEquals( ProtoPolyType.NODE, page.getColumnMeta( nodeColumn ).getTypeMeta().getProtoValueType() );
                    Row row = singleRow( page );
                    assertEquals( ProtoValue.ValueCase.NODE, row.getValues( nodeColumn ).getValueCase() );
                    assertNode( row.getValues( nodeColumn ).getNode(), names.get( i ), i + 1 );
                    assertEquals( ProtoValue.ValueCase.STRING, row.getValues( nameColumn ).getValueCase() );
                    assertEquals( names.get( i ), row.getValues( nameColumn ).getString().getString() );
                    connection.checkTransactions( last ? 0 : 1 );
                    if ( !last ) {
                        frame = connection.fetch( statement.getStatementId(), 1 );
                    }
                }
            }
        }
    }


    @Test
    void preservesEdgeAndEndpointCellsAlongsideScalarValues() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher(
                    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) RETURN a, r, b, b.name AS name", 1 )
                    .getResult().getFrame();
            RelationalFrame page = relationalFrame( frame, true, List.of( "a", "r", "b", "name" ) );
            assertEquals( ProtoPolyType.NODE, page.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertEquals( ProtoPolyType.EDGE, page.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
            assertEquals( ProtoPolyType.NODE, page.getColumnMeta( 2 ).getTypeMeta().getProtoValueType() );
            Row row = singleRow( page );
            assertEquals( ProtoValue.ValueCase.NODE, row.getValues( 0 ).getValueCase() );
            assertEquals( ProtoValue.ValueCase.EDGE, row.getValues( 1 ).getValueCase() );
            assertEquals( ProtoValue.ValueCase.NODE, row.getValues( 2 ).getValueCase() );
            ProtoNode alice = assertNode( row.getValues( 0 ).getNode(), "Alice", 1 );
            ProtoNode bob = assertNode( row.getValues( 2 ).getNode(), "Bob", 2 );
            assertEdge( row.getValues( 1 ).getEdge(), alice, bob, 2020 );
            assertEquals( "Bob", row.getValues( 3 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesThePathAsASingleOrderedCell() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher(
                    "MATCH p=(:Person {name: 'Alice'})-[:KNOWS*1..3]->(:Person {name: 'Charlie'}) RETURN p, 'route' AS title", 1 )
                    .getResult().getFrame();
            RelationalFrame page = relationalFrame( frame, true, List.of( "p", "title" ) );
            assertEquals( ProtoPolyType.PATH, page.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            Row row = singleRow( page );
            assertEquals( ProtoValue.ValueCase.PATH, row.getValues( 0 ).getValueCase() );
            assertEquals( "route", row.getValues( 1 ).getString().getString() );
            ProtoPath path = row.getValues( 0 ).getPath();
            assertEquals( 3, path.getNodesCount() );
            assertEquals( 2, path.getEdgesCount() );
            ProtoNode alice = assertNode( path.getNodes( 0 ), "Alice", 1 );
            ProtoNode bob = assertNode( path.getNodes( 1 ), "Bob", 2 );
            ProtoNode charlie = assertNode( path.getNodes( 2 ), "Charlie", 3 );
            assertEdge( path.getEdges( 0 ), alice, bob, 2020 );
            assertEdge( path.getEdges( 1 ), bob, charlie, 2021 );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void emptyMixedResultsStillExposeTheGraphColumn() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( "MATCH (n:Person {name: 'Unknown'}) RETURN n, n.name AS name", 1 ).getResult().getFrame();
            RelationalFrame page = relationalFrame( frame, true, List.of( "n", "name" ) );
            assertEquals( ProtoPolyType.NODE, page.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertEquals( 0, page.getRowsCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void olderClientsReceiveAnUpgradeHintForMixedProjections() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            for ( String projection : List.of( "n, n.name", "n.name, n" ) ) {
                Response response = connection.send( statementRequest( NAMESPACE, "cypher", "MATCH (n:Person) RETURN " + projection, 1 ) );
                expect( response, Response.TypeCase.ERROR_RESPONSE );
                String message = response.getErrorResponse().getMessage();
                assertTrue( message.contains( "2.3" ), message );
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


    static RelationalFrame relationalFrame( Frame frame, boolean isLast, List<String> labels ) {
        assertTrue( frame.hasRelationalFrame(), () -> "Expected a relational frame but got " + frame.getResultCase() );
        assertEquals( isLast, frame.getIsLast() );
        RelationalFrame page = frame.getRelationalFrame();
        assertEquals( labels.size(), page.getColumnMetaCount() );
        for ( int i = 0; i < labels.size(); i++ ) {
            assertEquals( labels.get( i ), page.getColumnMeta( i ).getColumnLabel() );
            assertEquals( i, page.getColumnMeta( i ).getColumnIndex() );
        }
        return page;
    }


    static Row singleRow( RelationalFrame frame ) {
        assertEquals( 1, frame.getRowsCount() );
        Row row = frame.getRows( 0 );
        assertEquals( frame.getColumnMetaCount(), row.getValuesCount() );
        return row;
    }


    private static ProtoNode assertNode( GraphElement element, String name, int id ) {
        assertTrue( element.hasNode() );
        return assertNode( element.getNode(), name, id );
    }


    private static ProtoNode assertNode( ProtoNode node, String name, int id ) {
        assertFalse( node.getId().isBlank() );
        assertEquals( List.of( "Person" ), node.getLabelsList() );
        assertEquals( name, node.getPropertiesOrThrow( "name" ).getString().getString() );
        assertEquals( id, integerValue( node.getPropertiesOrThrow( "id" ) ) );
        return node;
    }


    private static void assertEdge( GraphElement element, ProtoNode from, ProtoNode to, int since ) {
        assertTrue( element.hasEdge() );
        assertEdge( element.getEdge(), from, to, since );
    }


    private static void assertEdge( ProtoEdge edge, ProtoNode from, ProtoNode to, int since ) {
        assertFalse( edge.getId().isBlank() );
        assertEquals( List.of( "KNOWS" ), edge.getLabelsList() );
        assertEquals( since, integerValue( edge.getPropertiesOrThrow( "since" ) ) );
        assertEquals( from.getId(), edge.getLeft() );
        assertEquals( to.getId(), edge.getRight() );
        assertEquals( ProtoEdge.Direction.LEFT_TO_RIGHT, edge.getDirection() );
    }


    static long integerValue( ProtoValue value ) {
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
    static class PrismConnection implements AutoCloseable {

        private final PlainTransport transport;
        private long nextRequestId = 1;


        PrismConnection( boolean autocommit ) throws IOException {
            this( autocommit, 2 );
        }


        PrismConnection( boolean autocommit, int minorVersion ) throws IOException {
            transport = new PlainTransport( "localhost", 20590 );
            try {
                Response response = send( Request.newBuilder().setConnectionRequest( ConnectionRequest.newBuilder()
                        .setMajorApiVersion( 2 )
                        .setMinorApiVersion( minorVersion )
                        .setUsername( "pa" )
                        .setPassword( "" )
                        .setConnectionProperties( ConnectionProperties.newBuilder().setIsAutoCommit( autocommit ) ) ) );
                ConnectionResponse handshake = expect( response, Response.TypeCase.CONNECTION_RESPONSE ).getConnectionResponse();
                assertTrue( handshake.getIsCompatible() );
                if ( minorVersion == 3 ) {
                    assertTrue( handshake.getMinorApiVersion() >= 3 );
                }
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
