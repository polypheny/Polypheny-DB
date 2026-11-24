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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.polypheny.db.prisminterface.utils.PolyValueSerializer;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ClientInfoPropertiesRequest;
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
import org.polypheny.prism.ExecuteNamedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.FetchRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.NamedParameters;
import org.polypheny.prism.NamespacesRequest;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoList;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.ProtoString;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CypherTest {

    private static final String NAMESPACE = "cyphertest";
    private static final String WRITE_RETURN_NAMESPACE = "cypherwritereturntest";
    private static final String SQL_PAGE_QUERY = "SELECT n FROM (VALUES ('a'), ('b'), ('c')) AS t(n) ORDER BY n";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + WRITE_RETURN_NAMESPACE, 1 );
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + WRITE_RETURN_NAMESPACE, 1 );
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
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + WRITE_RETURN_NAMESPACE, 1 );
        }
    }


    @Test
    void bindsNamedCypherParametersWithoutRewritingQueryText() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person) WHERE n.id = $id // $ignored
                    RETURN n.name AS person, $term AS term, $term AS repeated, '$literal' AS literal
                    """ );
            assertEquals( List.of( "id", "term" ), signature.getParameterMetasList().stream().map( ParameterMeta::getName ).toList() );
            assertEquals( List.of( "id", "term" ), signature.getParameterMetasList().stream().map( ParameterMeta::getParameterName ).toList() );
            assertTrue( signature.getParameterMetasList().stream().allMatch( meta -> meta.getTypeName().equals( "ANY" ) ) );
            connection.checkTransactions( 0 );

            for ( int id : List.of( 1, 2 ) ) {
                String term = id + "\\u0027\\'; CREATE (:Injected) // \"$unexpected\"\nline\r\tπ";
                Response response = executeNamedCypher( connection, signature.getStatementId(), Map.of(
                        "id", ProtoValue.newBuilder().setInteger( ProtoInteger.newBuilder().setInteger( id ) ).build(),
                        "term", stringParameter( term ) ) );
                Frame frame = expect( response, Response.TypeCase.STATEMENT_RESULT ).getStatementResult().getFrame();
                Row row = singleRow( relationalFrame( frame, true, List.of( "person", "term", "repeated", "literal" ) ) );
                assertEquals( List.of( id == 1 ? "Alice" : "Bob", term, term, "$literal" ),
                        row.getValuesList().stream().map( value -> value.getString().getString() ).toList() );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void validatesNamedCypherArgumentsAndCanRetryTheStatement() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, "RETURN $value AS supplied" );
            int statementId = signature.getStatementId();
            assertNamedCypherError( executeNamedCypher( connection, statementId, Map.of() ), "Missing Cypher parameter: $value" );
            assertNamedCypherError( executeNamedCypher( connection, statementId, Map.of( "value", stringParameter( "valid" ), "other", stringParameter( "extra" ) ) ),
                    "Unexpected Cypher parameter: $other" );
            ProtoValue invalid = PolyValueSerializer.serialize( PolyDouble.of( Double.NaN ) );
            assertNamedCypherError( executeNamedCypher( connection, statementId, Map.of( "value", invalid ) ), "Unsupported Cypher parameter type" );
            connection.checkTransactions( 0 );

            Response response = expect( executeNamedCypher( connection, statementId, Map.of( "value", stringParameter( "valid" ) ) ), Response.TypeCase.STATEMENT_RESULT );
            Row row = singleRow( relationalFrame( response.getStatementResult().getFrame(), true, List.of( "supplied" ) ) );
            assertEquals( "valid", row.getValues( 0 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void bindsTopLevelCypherListsWithNoInferredParameterType() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int statementId = prepareNamedCypher( connection, "RETURN $list[$index] AS selected, $list AS supplied" ).getStatementId();
            ProtoValue list = ProtoValue.newBuilder().setList( ProtoList.newBuilder()
                    .addValues( stringParameter( "first" ) )
                    .addValues( PolyValueSerializer.serialize( PolyNull.NULL ) ) ).build();
            ProtoValue index = PolyValueSerializer.serialize( PolyInteger.of( 0 ) );

            StatementResult result = expect( executeNamedCypher( connection, statementId, Map.of( "list", list, "index", index ) ),
                    Response.TypeCase.STATEMENT_RESULT ).getStatementResult();
            Row row = singleRow( relationalFrame( result.getFrame(), true, List.of( "selected", "supplied" ) ) );
            assertEquals( stringParameter( "first" ), row.getValues( 0 ) );
            assertEquals( list, row.getValues( 1 ) );

            ProtoValue empty = ProtoValue.newBuilder().setList( ProtoList.getDefaultInstance() ).build();
            result = expect( executeNamedCypher( connection, statementId, Map.of( "list", empty, "index", index ) ),
                    Response.TypeCase.STATEMENT_RESULT ).getStatementResult();
            row = singleRow( relationalFrame( result.getFrame(), true, List.of( "selected", "supplied" ) ) );
            assertTrue( row.getValues( 0 ).hasNull() );
            assertEquals( empty, row.getValues( 1 ) );
        }
    }


    @Test
    void unwindsNamedCypherListOfMaps() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int statementId = prepareNamedCypher( connection, """
                    UNWIND $people AS person
                    RETURN person.name AS name ORDER BY name
                    """ ).getStatementId();
            ProtoValue list = ProtoValue.newBuilder().setList( ProtoList.newBuilder()
                    .addValues( ProtoValue.newBuilder().setDocument( ProtoDocument.newBuilder().putEntries( "name", stringParameter( "Lin" ) ) ) )
                    .addValues( ProtoValue.newBuilder().setDocument( ProtoDocument.newBuilder().putEntries( "name", stringParameter( "Ada" ) ) ) ) ).build();
            StatementResult result = expect( executeNamedCypher( connection, statementId, Map.of( "people", list ) ),
                    Response.TypeCase.STATEMENT_RESULT ).getStatementResult();
            assertEquals( List.of( stringParameter( "Ada" ), stringParameter( "Lin" ) ),
                    relationalFrame( result.getFrame(), true, List.of( "name" ) ).getRowsList().stream()
                            .map( row -> row.getValues( 0 ) ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void pagesCypherUnionWithGraphAndScalarColumns() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            StatementResponse result = connection.cypher( """
                    MATCH (a:Person {id: 1}) RETURN a AS person, 'first' AS branch
                    UNION ALL
                    MATCH (b:Person {id: 2}) RETURN 'second' AS branch, b AS person
                    """, 1 );
            Frame frame = result.getResult().getFrame();
            Row first = singleRow( relationalFrame( frame, false, List.of( "person", "branch" ) ) );
            assertNode( first.getValues( 0 ).getNode(), "Alice", 1 );
            assertEquals( "first", first.getValues( 1 ).getString().getString() );
            Row second = singleRow( relationalFrame( connection.fetch( result.getStatementId(), 1 ), true, List.of( "person", "branch" ) ) );
            assertNode( second.getValues( 0 ).getNode(), "Bob", 2 );
            assertEquals( "second", second.getValues( 1 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void foreachWritesForEachItemAndPreservesOuterRows() throws IOException {
        String namespace = "cypherforeachprism";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", "CREATE (:Person {name:'John Doe'})", -1 );
                Frame result = connection.execute( namespace, "cypher", """
                        MATCH (p:Person {name:'John Doe'})
                        UNWIND [[], null, ['Jane','Bob','Alice','Bob']] AS names
                        FOREACH (name IN names |
                            CREATE (p)-[:KNOWS]->(n:Person {name:name})
                            SET n.active = true
                        )
                        RETURN count(*) AS inputCount
                        """, -1 ).getResult().getFrame();
                Row count = singleRow( relationalFrame( result, true, List.of( "inputCount" ) ) );
                assertEquals( 3, integerValue( count.getValues( 0 ) ) );
                connection.checkTransactions( 0 );

                Frame stored = connection.execute( namespace, "cypher", """
                        MATCH (:Person {name:'John Doe'})-[:KNOWS]->(friend:Person)
                        RETURN friend.name AS name, friend.active AS active ORDER BY name
                        """, -1 ).getResult().getFrame();
                List<Row> friends = relationalFrame( stored, true, List.of( "name", "active" ) ).getRowsList();
                assertEquals( List.of( "Alice", "Bob", "Bob", "Jane" ), friends.stream()
                        .map( row -> row.getValues( 0 ).getString().getString() ).toList() );
                assertTrue( friends.stream().allMatch( row -> row.getValues( 1 ).getBoolean().getBoolean() ) );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void foreachUpdateBatchRollsBackWhenALaterIterationFails() throws IOException {
        String namespace = "cypherforeachbatch";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                String insert = "FOREACH (v IN %s | CREATE (:BatchLoop {value:v}))";
                Response error = expect( cypherBatch( connection, namespace, insert.formatted( "[1,2]" ),
                        insert.formatted( "[3,{invalid:true}]" ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().toLowerCase( Locale.ROOT ).contains( "property" ) );
                connection.checkTransactions( 0 );

                Frame stored = connection.execute( namespace, "cypher", "MATCH (n:BatchLoop) RETURN count(n) AS count", -1 ).getResult().getFrame();
                assertEquals( 0, integerValue( singleRow( relationalFrame( stored, true, List.of( "count" ) ) ).getValues( 0 ) ) );

                Response batch = expect( cypherBatch( connection, namespace, insert.formatted( "[1,2]" ), insert.formatted( "[3]" ) ),
                        Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                assertEquals( 2, batch.getStatementBatchResponse().getScalarsCount() );
                connection.checkTransactions( 0 );

                stored = connection.execute( namespace, "cypher", "MATCH (n:BatchLoop) RETURN n.value AS value ORDER BY value", -1 ).getResult().getFrame();
                assertEquals( List.of( 1L, 2L, 3L ), relationalFrame( stored, true, List.of( "value" ) ).getRowsList().stream()
                        .map( row -> integerValue( row.getValues( 0 ) ) ).toList() );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void callsDocumentedCypherSubqueryAndLabelsProcedure() throws IOException {
        String namespace = "cyphercallprism";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", """
                        CREATE (:Person {name:'John Doe', age:30}), (:Person:Employee {name:'Jane Doe', age:32}),
                            (:Person:Employee {name:'Lin', age:40}), (:Animal)
                        """, -1 );
                StatementResponse query = connection.execute( namespace, "cypher", """
                        CALL {
                            MATCH (p:Person {name:'John Doe'})
                            RETURN p.age AS age
                        }
                        MATCH (p:Person) WHERE p.age > age
                        RETURN p.name AS name ORDER BY name
                        """, 1 );
                Row first = singleRow( relationalFrame( query.getResult().getFrame(), false, List.of( "name" ) ) );
                assertEquals( "Jane Doe", first.getValues( 0 ).getString().getString() );
                Row second = singleRow( relationalFrame( connection.fetch( query.getStatementId(), 1 ), true, List.of( "name" ) ) );
                assertEquals( "Lin", second.getValues( 0 ).getString().getString() );
                connection.checkTransactions( 0 );

                Frame counted = connection.execute( namespace, "cypher", """
                        CALL db.labels() YIELD label
                        RETURN count(*) AS numberOfLabels
                        """, -1 ).getResult().getFrame();
                Row count = singleRow( relationalFrame( counted, true, List.of( "numberOfLabels" ) ) );
                assertEquals( 3, integerValue( count.getValues( 0 ) ) );

                Frame filtered = connection.execute( namespace, "cypher", """
                        CALL db.labels() YIELD label AS tag WHERE tag STARTS WITH 'E'
                        RETURN tag
                        """, -1 ).getResult().getFrame();
                Row category = singleRow( relationalFrame( filtered, true, List.of( "tag" ) ) );
                assertEquals( "Employee", category.getValues( 0 ).getString().getString() );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void evaluatesCypherStringFunctionsWithUnicodeAndNamedLists() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int statementId = prepareNamedCypher( connection, """
                    RETURN left($text, 1) AS first, substring($text, 1) AS rest,
                           toUpper($text) AS upper, reverse($items) AS reversed
                    """ ).getStatementId();
            ProtoValue missing = PolyValueSerializer.serialize( PolyNull.NULL );
            ProtoValue firstValue = PolyValueSerializer.serialize( PolyInteger.of( 1 ) );
            ProtoValue secondValue = PolyValueSerializer.serialize( PolyInteger.of( 2 ) );
            ProtoValue items = ProtoValue.newBuilder().setList( ProtoList.newBuilder().addValues( firstValue ).addValues( missing )
                    .addValues( secondValue ) ).build();

            for ( String suffix : List.of( "Straße", "aLICE" ) ) {
                String text = "🍀" + suffix;
                Frame frame = expect( executeNamedCypher( connection, statementId, Map.of( "text", stringParameter( text ), "items", items ) ),
                        Response.TypeCase.STATEMENT_RESULT ).getStatementResult().getFrame();
                Row row = singleRow( relationalFrame( frame, true, List.of( "first", "rest", "upper", "reversed" ) ) );
                assertEquals( stringParameter( "🍀" ), row.getValues( 0 ) );
                assertEquals( stringParameter( suffix ), row.getValues( 1 ) );
                assertEquals( stringParameter( text.toUpperCase( Locale.ROOT ) ), row.getValues( 2 ) );
                assertEquals( List.of( secondValue, missing, firstValue ), row.getValues( 3 ).getList().getValuesList() );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void evaluatesNumericFunctionsInPreparedBoundsAndRandomCallsPerRow() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int statementId = prepareNamedCypher( connection, """
                    UNWIND [3, 2, 1] AS x
                    RETURN x ORDER BY x SKIP toInteger($offset) LIMIT toInteger(ceil(1.2))
                    """ ).getStatementId();
            Frame page = expect( executeNamedCypher( connection, statementId, Map.of( "offset", stringParameter( "1" ) ) ),
                    Response.TypeCase.STATEMENT_RESULT ).getStatementResult().getFrame();
            assertEquals( List.of( 2L, 3L ), relationalFrame( page, true, List.of( "x" ) ).getRowsList().stream()
                    .map( row -> integerValue( row.getValues( 0 ) ) ).toList() );
            connection.checkTransactions( 0 );

            Frame samples = connection.cypher( """
                    UNWIND range(1, 12) AS row
                    CALL { RETURN rand() AS sample }
                    RETURN row, sample, rand() AS second ORDER BY row
                    """, -1 ).getResult().getFrame();
            List<Double> values = relationalFrame( samples, true, List.of( "row", "sample", "second" ) ).getRowsList().stream()
                    .flatMap( row -> List.of( row.getValues( 1 ).getDouble().getDouble(), row.getValues( 2 ).getDouble().getDouble() ).stream() )
                    .toList();
            assertEquals( 24, values.size() );
            assertTrue( values.stream().allMatch( value -> value >= 0.0 && value < 1.0 ) );
            assertEquals( 24, values.stream().distinct().count() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void mergesInUpdateBatchesAndReturnsGraphValuesDirectly() throws IOException {
        String namespace = "cyphermergeprism";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                Response batch = expect( cypherBatch( connection, namespace,
                        "MERGE (:MergeTarget {id: 1})", "MERGE (:MergeTarget {id: 1})" ), Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                assertEquals( List.of( 1L, 0L ), batch.getStatementBatchResponse().getScalarsList() );
                connection.checkTransactions( 0 );

                String query = """
                        MERGE (n:MergeTarget {id: 1})
                        ON MATCH SET n.seen = true
                        RETURN n AS node, n.seen AS seen
                        """;
                Frame frame = connection.execute( namespace, "cypher", query, -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( frame, true, List.of( "node", "seen" ) ) );
                ProtoNode node = row.getValues( 0 ).getNode();
                assertEquals( "n", node.getName() );
                assertEquals( List.of( "MergeTarget" ), node.getLabelsList() );
                assertEquals( 1L, integerValue( node.getPropertiesOrThrow( "id" ) ) );
                assertTrue( row.getValues( 1 ).getBoolean().getBoolean() );
                connection.checkTransactions( 0 );

                String relationship = """
                        MERGE (a:MergeTarget {id: 1})
                        MERGE (b:MergeTarget {id: 2})
                        MERGE (a)-[r:LINK]->(b)
                        ON CREATE SET r.weight = 1
                        ON MATCH SET r.weight = r.weight + 1
                        RETURN r AS relationship, type(r) AS kind
                        """;
                String relationshipId = null;
                for ( long weight : List.of( 1L, 2L ) ) {
                    Frame edges = connection.execute( namespace, "cypher", relationship, -1 ).getResult().getFrame();
                    Row edgeRow = singleRow( relationalFrame( edges, true, List.of( "relationship", "kind" ) ) );
                    ProtoEdge edge = edgeRow.getValues( 0 ).getEdge();
                    assertEquals( "r", edge.getName() );
                    assertEquals( "LINK", edgeRow.getValues( 1 ).getString().getString() );
                    assertEquals( weight, integerValue( edge.getPropertiesOrThrow( "weight" ) ) );
                    if ( relationshipId == null ) {
                        relationshipId = edge.getId();
                    } else {
                        assertEquals( relationshipId, edge.getId() );
                    }
                    connection.checkTransactions( 0 );
                }

                Frame union = connection.execute( namespace, "cypher", """
                        MATCH (n:MergeUnion) RETURN n.name AS value
                        UNION ALL MERGE (n:MergeUnion {name: 'created'}) RETURN n.name AS value
                        UNION ALL MATCH (n:MergeUnion) RETURN n.name AS value
                        """, -1 ).getResult().getFrame();
                assertEquals( List.of( stringParameter( "created" ), stringParameter( "created" ) ),
                        relationalFrame( union, true, List.of( "value" ) ).getRowsList().stream().map( match -> match.getValues( 0 ) ).toList() );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void roundTripsNamedCypherMapParametersAcrossStores() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person)
                    WITH n, $payload AS p WHERE n.name = p.profile.name
                    RETURN $payload.profile.name AS person, p.items[0].score AS score, p.items[1] AS missing, p AS payload
                    """ );
            for ( int id : List.of( 1, 2, 1 ) ) {
                String person = id == 1 ? "Alice" : "Bob";
                int score = id * 7;
                Frame source = connection.cypher( """
                        RETURN {profile:{name:'%s'}, items:[{score:%d},null], tags:[], empty:{}} AS payload
                        """.formatted( person, score ), -1 ).getResult().getFrame();
                RelationalFrame sourceFrame = relationalFrame( source, true, List.of( "payload" ) );
                assertEquals( ProtoPolyType.MAP, sourceFrame.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
                ProtoValue map = singleRow( sourceFrame ).getValues( 0 );
                assertTrue( map.hasDocument() );

                Response result = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "payload", map ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame roundTrip = relationalFrame( result.getStatementResult().getFrame(), true, List.of( "person", "score", "missing", "payload" ) );
                assertEquals( ProtoPolyType.MAP, roundTrip.getColumnMeta( 3 ).getTypeMeta().getProtoValueType() );
                Row row = singleRow( roundTrip );
                assertEquals( stringParameter( person ), row.getValues( 0 ) );
                assertEquals( score, integerValue( row.getValues( 1 ) ) );
                assertTrue( row.getValues( 2 ).hasNull() );
                assertEquals( map, row.getValues( 3 ) );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void validatesNestedNamedCypherMapValuesBeforeWritingAcrossStores() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            int statementId = prepareNamedCypher( writer, WRITE_RETURN_NAMESPACE, """
                    CREATE (n:NamedCypherMapWrite {old:'previous'})
                    SET n += $payload
                    RETURN n.name AS name, n.scores[1] AS score, n.old AS old
                    """ ).getStatementId();
            ProtoValue invalid = ProtoValue.newBuilder().setDocument( ProtoDocument.newBuilder().putEntries( "scores",
                    ProtoValue.newBuilder().setList( ProtoList.newBuilder()
                            .addValues( PolyValueSerializer.serialize( PolyDouble.of( Double.NaN ) ) ) ).build() ) ).build();
            assertNamedCypherError( executeNamedCypher( writer, statementId, Map.of( "payload", invalid ) ),
                    "Unsupported Cypher parameter type for $payload: DOUBLE" );
            writer.checkTransactions( 0 );
            Frame beforeRetry = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH (n:NamedCypherMapWrite) RETURN count(n) AS total", -1 )
                    .getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( beforeRetry, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame source = writer.cypher( "RETURN {name:'saved', scores:[3,7], old:null} AS payload", -1 ).getResult().getFrame();
            ProtoValue map = singleRow( relationalFrame( source, true, List.of( "payload" ) ) ).getValues( 0 );
            Response result = expect( executeNamedCypher( writer, statementId, Map.of( "payload", map ) ), Response.TypeCase.STATEMENT_RESULT );
            Row written = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "name", "score", "old" ) ) );
            assertEquals( stringParameter( "saved" ), written.getValues( 0 ) );
            assertEquals( 7, integerValue( written.getValues( 1 ) ) );
            assertTrue( written.getValues( 2 ).hasNull() );
            Frame persisted = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:NamedCypherMapWrite) RETURN n.name AS name, n.scores[1] AS score, n.old AS old
                    """, -1 ).getResult().getFrame();
            assertEquals( written, singleRow( relationalFrame( persisted, true, List.of( "name", "score", "old" ) ) ) );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void createsPropertiesFromNamedMapParametersAcrossStores() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            int statementId = prepareNamedCypher( writer, WRITE_RETURN_NAMESPACE, """
                    CREATE (n:CreateFromMap {score:$payload.metrics.score, missing:$payload.missing})
                        -[r:CREATE_FROM_MAP {active:$payload.active}]->(:CreateFromMapTarget)
                    RETURN n.score AS score, n.missing AS missing, r.active AS active
                    """ ).getStatementId();
            Frame source = writer.cypher( "RETURN {metrics:{score:7}, active:true} AS payload", -1 ).getResult().getFrame();
            ProtoValue map = singleRow( relationalFrame( source, true, List.of( "payload" ) ) ).getValues( 0 );
            Response result = expect( executeNamedCypher( writer, statementId, Map.of( "payload", map ) ), Response.TypeCase.STATEMENT_RESULT );
            List<String> columns = List.of( "score", "missing", "active" );
            Row created = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, columns ) );
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:CreateFromMap)-[r:CREATE_FROM_MAP]->(:CreateFromMapTarget)
                    RETURN n.score AS score, n.missing AS missing, r.active AS active
                    """, -1 ).getResult().getFrame();
            Row persisted = singleRow( relationalFrame( stored, true, columns ) );
            for ( Row row : List.of( created, persisted ) ) {
                assertEquals( 7, integerValue( row.getValues( 0 ) ) );
                assertTrue( row.getValues( 1 ).hasNull() );
                assertTrue( row.getValues( 2 ).getBoolean().getBoolean() );
            }
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void preservesNamedCypherScalarValueTypesWhenReusingTheStatement() throws IOException {
        List<PolyValue> scalars = List.of(
                PolyNull.NULL, new PolyBoolean( true ), new PolyInteger( -13 ), new PolyLong( 2147483648L ),
                new PolyBigDecimal( new BigDecimal( "-123.456" ) ), new PolyFloat( 0.125f ), new PolyDouble( -2.75 ),
                PolyString.of( "π" ), PolyBinary.of( new byte[]{ 0x01, 0x02 } ), PolyDate.ofDays( 20000 ), new PolyTime( 12345 ),
                new PolyTimestamp( 1700000000123L ) );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, "RETURN $value AS supplied" );
            for ( PolyValue scalar : scalars ) {
                ProtoValue parameter = PolyValueSerializer.serialize( scalar );
                Response response = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", parameter ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame frame = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "supplied" ) );
                assertEquals( ProtoPolyType.valueOf( scalar.getType().getName() ), frame.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
                Row row = singleRow( frame );
                assertEquals( parameter, row.getValues( 0 ), () -> "Roundtrip for " + scalar.getType() );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void preservesLongAndBinaryParametersInGraphProjections() throws IOException {
        assertPreservesGraphProjectionValues( List.of( PolyLong.of( 2147483648L ), PolyBinary.of( new byte[]{ 0x01, 0x02 } ) ) );
    }


    @Test
    void preservesExactDecimalParametersInGraphProjections() throws IOException {
        assertPreservesGraphProjectionValues( List.of( PolyBigDecimal.of( "0.123456789012345678901234567891" ) ) );
    }


    private static void assertPreservesGraphProjectionValues( List<PolyValue> values ) throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person {name: 'Alice'})
                    RETURN n.name AS person, $value AS supplied
                    """ );
            for ( PolyValue value : values ) {
                ProtoValue encoded = PolyValueSerializer.serialize( value );
                Response result = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", encoded ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame frame = relationalFrame( result.getStatementResult().getFrame(), true, List.of( "person", "supplied" ) );
                assertEquals( ProtoPolyType.valueOf( value.getType().getName() ), frame.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
                Row row = singleRow( frame );
                assertEquals( "Alice", row.getValues( 0 ).getString().getString() );
                assertEquals( encoded, row.getValues( 1 ) );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void comparesNamedParametersToTypedLiterals() throws IOException {
        String match = "MATCH (n:Person {name: 'Alice'})";
        ProtoValue trueValue = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue falseValue = PolyValueSerializer.serialize( PolyBoolean.FALSE );
        ProtoValue nullValue = PolyValueSerializer.serialize( PolyNull.NULL );
        Map<ProtoValue, ProtoValue> equalities = Map.of(
                PolyValueSerializer.serialize( PolyInteger.of( 42 ) ), trueValue,
                PolyValueSerializer.serialize( PolyLong.of( 42L ) ), trueValue,
                PolyValueSerializer.serialize( PolyBigDecimal.of( "42.00" ) ), trueValue,
                PolyValueSerializer.serialize( PolyInteger.of( 43 ) ), falseValue,
                stringParameter( "42" ), falseValue,
                trueValue, falseValue,
                nullValue, nullValue );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String operator : List.of( "=", "<>" ) ) {
                for ( String comparison : List.of( "$value " + operator + " 42", "42 " + operator + " $value" ) ) {
                    PreparedStatementSignature projection = prepareNamedCypher( connection, match + " RETURN " + comparison + " AS matched" );
                    PreparedStatementSignature filter = prepareNamedCypher( connection, match + " WHERE " + comparison + " RETURN n.name AS name" );
                    for ( Map.Entry<ProtoValue, ProtoValue> equality : equalities.entrySet() ) {
                        ProtoValue expected = equality.getValue();
                        if ( operator.equals( "<>" ) && !expected.hasNull() ) {
                            expected = expected.equals( trueValue ) ? falseValue : trueValue;
                        }
                        Map<String, ProtoValue> binding = Map.of( "value", equality.getKey() );
                        Response projected = expect( executeNamedCypher( connection, projection.getStatementId(), binding ), Response.TypeCase.STATEMENT_RESULT );
                        Row row = singleRow( relationalFrame( projected.getStatementResult().getFrame(), true, List.of( "matched" ) ) );
                        assertEquals( expected, row.getValues( 0 ), () -> comparison + " with " + equality.getKey() );

                        Response filtered = expect( executeNamedCypher( connection, filter.getStatementId(), binding ), Response.TypeCase.STATEMENT_RESULT );
                        RelationalFrame rows = relationalFrame( filtered.getStatementResult().getFrame(), true, List.of( "name" ) );
                        assertEquals( expected.equals( trueValue ) ? List.of( "Alice" ) : List.of(), projectedNames( rows ) );
                    }
                }
            }

            String alias = match + " WITH n, $value AS x";
            PreparedStatementSignature aliasedProjection = prepareNamedCypher( connection, alias + " RETURN x = 42 AS equal, 42 <> x AS unequal" );
            PreparedStatementSignature aliasedFilter = prepareNamedCypher( connection, alias + " WHERE x = 42 RETURN n.name AS name" );
            ProtoValue integer = PolyValueSerializer.serialize( PolyInteger.of( 42 ) );
            for ( ProtoValue value : List.of( integer, stringParameter( "42" ), nullValue ) ) {
                Map<String, ProtoValue> binding = Map.of( "value", value );
                ProtoValue expected = equalities.get( value );
                boolean matches = expected.equals( trueValue );
                ProtoValue unequal = PolyValueSerializer.serialize( expected.hasNull() ? PolyNull.NULL : PolyBoolean.of( !matches ) );
                Response projected = expect( executeNamedCypher( connection, aliasedProjection.getStatementId(), binding ), Response.TypeCase.STATEMENT_RESULT );
                Row row = singleRow( relationalFrame( projected.getStatementResult().getFrame(), true, List.of( "equal", "unequal" ) ) );
                assertEquals( List.of( expected, unequal ), row.getValuesList() );

                Response filtered = expect( executeNamedCypher( connection, aliasedFilter.getStatementId(), binding ), Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame rows = relationalFrame( filtered.getStatementResult().getFrame(), true, List.of( "name" ) );
                assertEquals( matches ? List.of( "Alice" ) : List.of(), projectedNames( rows ) );
            }

            Map<String, List<String>> nullFilters = Map.of(
                    "($value = 42) OR n.name = 'Alice'", List.of( "Alice" ),
                    "($value = 42) AND n.name = 'Alice'", List.of(),
                    "NOT ($value = 42)", List.of() );
            for ( Map.Entry<String, List<String>> filter : nullFilters.entrySet() ) {
                PreparedStatementSignature query = prepareNamedCypher( connection, match + " WHERE " + filter.getKey() + " RETURN n.name AS name" );
                Response result = expect( executeNamedCypher( connection, query.getStatementId(), Map.of( "value", nullValue ) ), Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame rows = relationalFrame( result.getStatementResult().getFrame(), true, List.of( "name" ) );
                assertEquals( filter.getValue(), projectedNames( rows ), filter.getKey() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void comparesFloatingPointParametersAfterWithAndNormalizesSignedZero() throws IOException {
        String match = "MATCH (n:Person {name: 'Alice'})";
        ProtoValue truth = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue falsity = PolyValueSerializer.serialize( PolyBoolean.FALSE );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature floats = prepareNamedCypher( connection, match + """
                     WITH $value AS x
                     RETURN x = 42.0 AS equalsDouble, x <> 42.0 AS differsFromDouble
                    """ );
            for ( float scalar : List.of( 42.0f, 43.0f ) ) {
                ProtoValue value = PolyValueSerializer.serialize( new PolyFloat( scalar ) );
                Response result = expect( executeNamedCypher( connection, floats.getStatementId(), Map.of( "value", value ) ), Response.TypeCase.STATEMENT_RESULT );
                Row row = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "equalsDouble", "differsFromDouble" ) ) );
                assertEquals( scalar == 42.0f ? List.of( truth, falsity ) : List.of( falsity, truth ), row.getValuesList() );
            }

            for ( String query : List.of(
                    match + " RETURN $value = 0.0 AS equalsDouble, $value = 0 AS equalsInteger, $value <> 0.0 AS differsFromDouble",
                    match + " WITH $value AS x RETURN x = 0.0 AS equalsDouble, x = 0 AS equalsInteger, x <> 0.0 AS differsFromDouble" ) ) {
                PreparedStatementSignature zero = prepareNamedCypher( connection, query );
                for ( PolyValue signedZero : List.of( new PolyFloat( -0.0f ), new PolyDouble( -0.0d ), new PolyDouble( 0.0d ) ) ) {
                    ProtoValue value = PolyValueSerializer.serialize( signedZero );
                    Response result = expect( executeNamedCypher( connection, zero.getStatementId(), Map.of( "value", value ) ), Response.TypeCase.STATEMENT_RESULT );
                    Row row = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "equalsDouble", "equalsInteger", "differsFromDouble" ) ) );
                    assertEquals( List.of( truth, truth, falsity ), row.getValuesList(), () -> query + " with " + signedZero );
                }
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void ordersNamedParametersWithoutRoundingOrTreatingNullAsFalse() throws IOException {
        String match = "MATCH (n:Person {name: 'Alice'})";
        ProtoValue truth = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue falsity = PolyValueSerializer.serialize( PolyBoolean.FALSE );
        ProtoValue unknown = PolyValueSerializer.serialize( PolyNull.NULL );
        List<String> columns = List.of( "lt", "le", "gt", "ge" );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String comparisons = " RETURN %1$s < 42.0 AS lt, %1$s <= 42.0 AS le, %1$s > 42.0 AS gt, %1$s >= 42.0 AS ge";
            for ( String query : List.of( match + comparisons.formatted( "$value" ), match + " WITH $value AS x" + comparisons.formatted( "x" ) ) ) {
                PreparedStatementSignature signature = prepareNamedCypher( connection, query );
                Map<PolyValue, List<ProtoValue>> expected = Map.of(
                        PolyFloat.of( 41f ), List.of( truth, truth, falsity, falsity ),
                        PolyFloat.of( 42f ), List.of( falsity, truth, falsity, truth ),
                        PolyFloat.of( 43f ), List.of( falsity, falsity, truth, truth ),
                        PolyNull.NULL, List.of( unknown, unknown, unknown, unknown ) );
                for ( Map.Entry<PolyValue, List<ProtoValue>> comparison : expected.entrySet() ) {
                    Response result = expect( executeNamedCypher( connection, signature.getStatementId(),
                            Map.of( "value", PolyValueSerializer.serialize( comparison.getKey() ) ) ), Response.TypeCase.STATEMENT_RESULT );
                    assertEquals( comparison.getValue(), singleRow( relationalFrame( result.getStatementResult().getFrame(), true, columns ) ).getValuesList() );
                }
            }

            PreparedStatementSignature precise = prepareNamedCypher( connection, match + """
                     WITH $value AS x
                     RETURN x < 0.0 AS lt, x <= 0.0 AS le, x > 9007199254740992.0 AS gt, x >= 0.0 AS ge
                    """ );
            Map<PolyValue, List<ProtoValue>> preciseResults = Map.of(
                    PolyDouble.of( -0.0 ), List.of( falsity, truth, falsity, truth ),
                    PolyLong.of( 9_007_199_254_740_993L ), List.of( falsity, falsity, truth, truth ) );
            for ( Map.Entry<PolyValue, List<ProtoValue>> comparison : preciseResults.entrySet() ) {
                Response result = expect( executeNamedCypher( connection, precise.getStatementId(),
                        Map.of( "value", PolyValueSerializer.serialize( comparison.getKey() ) ) ), Response.TypeCase.STATEMENT_RESULT );
                assertEquals( comparison.getValue(), singleRow( relationalFrame( result.getStatementResult().getFrame(), true, columns ) ).getValuesList() );
            }

            PreparedStatementSignature filter = prepareNamedCypher( connection,
                    match + " WITH n, $value AS x WHERE x <= 42.0 RETURN n.name AS name" );
            Response matched = expect( executeNamedCypher( connection, filter.getStatementId(),
                    Map.of( "value", PolyValueSerializer.serialize( PolyFloat.of( 42f ) ) ) ), Response.TypeCase.STATEMENT_RESULT );
            assertEquals( List.of( "Alice" ), projectedNames( relationalFrame( matched.getStatementResult().getFrame(), true, List.of( "name" ) ) ) );
            Response filtered = expect( executeNamedCypher( connection, filter.getStatementId(), Map.of( "value", unknown ) ), Response.TypeCase.STATEMENT_RESULT );
            assertEquals( 0, relationalFrame( filtered.getStatementResult().getFrame(), true, List.of( "name" ) ).getRowsCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesBooleanLiteralsWithNamedArguments() throws IOException {
        ProtoValue truth = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue falsity = PolyValueSerializer.serialize( PolyBoolean.FALSE );
        ProtoValue unknown = PolyValueSerializer.serialize( PolyNull.NULL );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person {name: 'Alice'})
                    RETURN $value AND true AS withAnd, $value OR false AS withOr, NOT $value AS negated
                    """ );
            Map<ProtoValue, List<ProtoValue>> expected = Map.of(
                    truth, List.of( truth, truth, falsity ),
                    falsity, List.of( falsity, falsity, truth ),
                    unknown, List.of( unknown, unknown, unknown ) );
            for ( Map.Entry<ProtoValue, List<ProtoValue>> binding : expected.entrySet() ) {
                Response result = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", binding.getKey() ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame frame = relationalFrame( result.getStatementResult().getFrame(), true, List.of( "withAnd", "withOr", "negated" ) );
                assertEquals( binding.getValue(), singleRow( frame ).getValuesList() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesUnknownInProjectedNamedBooleanExpressions() throws IOException {
        String aliasedValue = "MATCH (n:Person {name: 'Alice'}) WITH n, $value AS x";
        ProtoValue truth = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue falsity = PolyValueSerializer.serialize( PolyBoolean.FALSE );
        ProtoValue unknown = PolyValueSerializer.serialize( PolyNull.NULL );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature projection = prepareNamedCypher( connection, aliasedValue + """
                     RETURN x = 42 AND n.name = 'Alice' AS andTrue,
                            x = 42 AND n.name = 'Bob' AS andFalse,
                            x = 42 OR n.name = 'Alice' AS orTrue,
                            x = 42 OR n.name = 'Bob' AS orFalse,
                            NOT (x = 42 OR n.name = 'Bob') AS negated
                    """ );
            Map<ProtoValue, List<ProtoValue>> expectations = Map.of(
                    unknown, List.of( unknown, falsity, truth, unknown, unknown ),
                    PolyValueSerializer.serialize( PolyInteger.of( 42 ) ), List.of( truth, falsity, truth, truth, falsity ),
                    PolyValueSerializer.serialize( PolyInteger.of( 43 ) ), List.of( falsity, falsity, truth, falsity, truth ) );
            for ( Map.Entry<ProtoValue, List<ProtoValue>> entry : expectations.entrySet() ) {
                Response result = expect( executeNamedCypher( connection, projection.getStatementId(), Map.of( "value", entry.getKey() ) ), Response.TypeCase.STATEMENT_RESULT );
                Row row = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "andTrue", "andFalse", "orTrue", "orFalse", "negated" ) ) );
                assertEquals( entry.getValue(), row.getValuesList() );
            }

            for ( String condition : List.of( "x = 42 AND n.name = 'Alice'", "x = 42 OR n.name = 'Bob'", "NOT (x = 42 OR n.name = 'Bob')" ) ) {
                PreparedStatementSignature filter = prepareNamedCypher( connection, aliasedValue + " WHERE " + condition + " RETURN n.name AS name" );
                Response result = expect( executeNamedCypher( connection, filter.getStatementId(), Map.of( "value", unknown ) ), Response.TypeCase.STATEMENT_RESULT );
                assertTrue( projectedNames( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "name" ) ) ).isEmpty(), condition );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesNamedStringPredicatesLiterallyAndPreservesNulls() throws IOException {
        ProtoValue trueValue = PolyValueSerializer.serialize( new PolyBoolean( true ) );
        ProtoValue falseValue = PolyValueSerializer.serialize( new PolyBoolean( false ) );
        ProtoValue nullValue = PolyValueSerializer.serialize( PolyNull.NULL );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( Map.Entry<String, String> operator : Map.of( "STARTS WITH", "Al", "ENDS WITH", "ice", "CONTAINS", "lic" ).entrySet() ) {
                PreparedStatementSignature signature = prepareNamedCypher( connection, """
                        MATCH (n:Person {name: 'Alice'})
                        RETURN n.name %s $needle AS matched
                        """.formatted( operator.getKey() ) );
                PreparedStatementSignature filter = prepareNamedCypher( connection, """
                        MATCH (n:Person {name: 'Alice'})
                        WHERE n.name %s $needle
                        RETURN n.name AS name
                        """.formatted( operator.getKey() ) );
                PreparedStatementSignature optionalFilter = prepareNamedCypher( connection, """
                        OPTIONAL MATCH (n:Person {name: 'Alice'})
                        WHERE n.name %s $needle
                        RETURN n.name AS name
                        """.formatted( operator.getKey() ) );
                Map<ProtoValue, ProtoValue> arguments = Map.of(
                        stringParameter( operator.getValue() ), trueValue,
                        stringParameter( "%" ), falseValue,
                        stringParameter( "_" ), falseValue,
                        stringParameter( "[" ), falseValue,
                        nullValue, nullValue,
                        PolyValueSerializer.serialize( PolyInteger.of( 1 ) ), nullValue );
                for ( Map.Entry<ProtoValue, ProtoValue> argument : arguments.entrySet() ) {
                    Response result = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "needle", argument.getKey() ) ),
                            Response.TypeCase.STATEMENT_RESULT );
                    Row row = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "matched" ) ) );
                    assertEquals( argument.getValue(), row.getValues( 0 ), () -> operator.getKey() + " with " + argument.getKey() );

                    boolean matches = trueValue.equals( argument.getValue() );
                    Map<String, ProtoValue> binding = Map.of( "needle", argument.getKey() );
                    Response filtered = expect( executeNamedCypher( connection, filter.getStatementId(), binding ), Response.TypeCase.STATEMENT_RESULT );
                    RelationalFrame filteredRows = relationalFrame( filtered.getStatementResult().getFrame(), true, List.of( "name" ) );
                    assertEquals( matches ? List.of( "Alice" ) : List.of(), projectedNames( filteredRows ) );

                    Response optional = expect( executeNamedCypher( connection, optionalFilter.getStatementId(), binding ), Response.TypeCase.STATEMENT_RESULT );
                    Row optionalRow = singleRow( relationalFrame( optional.getStatementResult().getFrame(), true, List.of( "name" ) ) );
                    assertEquals( matches ? stringParameter( "Alice" ) : nullValue, optionalRow.getValues( 0 ) );
                }
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void matchesAndPersistsTypedNamedGraphProperties() throws IOException {
        String namespace = "cyphertypednamedtest";
        ProtoValue integer = PolyValueSerializer.serialize( PolyInteger.of( 42 ) );
        ProtoValue truth = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue nullValue = PolyValueSerializer.serialize( PolyNull.NULL );
        ProtoValue positiveZero = PolyValueSerializer.serialize( new PolyDouble( 0.0d ) );
        Map<String, ProtoValue> values = Map.of( "integer", integer, "numberText", stringParameter( "42" ),
                "boolean", truth, "booleanText", stringParameter( "true" ), "absent", nullValue, "zero", positiveZero );
        try ( PrismConnection writer = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                PreparedStatementSignature insert = prepareNamedCypher( writer, namespace, "CREATE (:TypedNamedValue {name:$name, value:$value})" );
                for ( Map.Entry<String, ProtoValue> value : values.entrySet() ) {
                    expect( executeNamedCypher( writer, insert.getStatementId(), Map.of( "name", stringParameter( value.getKey() ), "value", value.getValue() ) ),
                            Response.TypeCase.STATEMENT_RESULT );
                }
                PreparedStatementSignature edge = prepareNamedCypher( writer, namespace, """
                        CREATE (:TypedNamedEndpoint)-[e:TYPED_VALUE {value:$value}]->(:TypedNamedEndpoint)
                        RETURN e.value AS value
                        """ );
                Response inserted = expect( executeNamedCypher( writer, edge.getStatementId(), Map.of( "value", integer ) ), Response.TypeCase.STATEMENT_RESULT );
                assertStoredInteger( singleRow( relationalFrame( inserted.getStatementResult().getFrame(), true, List.of( "value" ) ) ).getValues( 0 ) );

                // Read over a new connection so this verifies storage rather than the values in the write plan.
                try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
                    for ( String query : List.of( "MATCH (n:TypedNamedValue {value:$value}) RETURN n.name AS name",
                            "MATCH (n:TypedNamedValue) WHERE n.value = $value RETURN n.name AS name" ) ) {
                        PreparedStatementSignature lookup = prepareNamedCypher( reader, namespace, query );
                        for ( Map.Entry<String, ProtoValue> value : values.entrySet() ) {
                            Response result = expect( executeNamedCypher( reader, lookup.getStatementId(), Map.of( "value", value.getValue() ) ),
                                    Response.TypeCase.STATEMENT_RESULT );
                            RelationalFrame rows = relationalFrame( result.getStatementResult().getFrame(), true, List.of( "name" ) );
                            assertEquals( value.getValue().equals( nullValue ) ? List.of() : List.of( value.getKey() ), projectedNames( rows ) );
                        }
                        ProtoValue negativeZero = PolyValueSerializer.serialize( new PolyDouble( -0.0d ) );
                        Response result = expect( executeNamedCypher( reader, lookup.getStatementId(), Map.of( "value", negativeZero ) ), Response.TypeCase.STATEMENT_RESULT );
                        RelationalFrame rows = relationalFrame( result.getStatementResult().getFrame(), true, List.of( "name" ) );
                        assertEquals( List.of( "zero" ), projectedNames( rows ) );
                    }

                    Frame readNodes = reader.execute( namespace, "cypher", "MATCH (n:TypedNamedValue) RETURN n.name AS name, n.value AS value", -1 ).getResult().getFrame();
                    Map<String, ProtoValue> stored = relationalFrame( readNodes, true, List.of( "name", "value" ) ).getRowsList().stream()
                            .collect( Collectors.toMap( row -> row.getValues( 0 ).getString().getString(), row -> row.getValues( 1 ) ) );
                    assertStoredInteger( stored.get( "integer" ) );
                    assertEquals( truth, stored.get( "boolean" ) );
                    assertEquals( stringParameter( "42" ), stored.get( "numberText" ) );
                    assertEquals( stringParameter( "true" ), stored.get( "booleanText" ) );
                    assertEquals( nullValue, stored.get( "absent" ) );

                    Frame readEdge = reader.execute( namespace, "cypher", "MATCH ()-[e:TYPED_VALUE]->() RETURN e.value AS value", -1 ).getResult().getFrame();
                    assertStoredInteger( singleRow( relationalFrame( readEdge, true, List.of( "value" ) ) ).getValues( 0 ) );
                }
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void preservesStoredExactDecimalsAcrossGraphWritePaths() throws IOException {
        String namespace = "cypherexactdecimalproperties";
        PolyBigDecimal number = PolyBigDecimal.of( "9007199254740993" );
        ProtoValue decimal = PolyValueSerializer.serialize( number );
        ProtoValue taggedText = stringParameter( org.polypheny.db.functions.CrossModelFunctions.graphPropertyStorageValue( number ) );
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                writer.execute( namespace, "cypher", "CREATE (:ExactDecimal {variant:'updated', v:0})", -1 );
                for ( String query : List.of(
                        "CREATE (:ExactDecimal {variant:'direct', v:$value})",
                        "CREATE (n:ExactDecimal {variant:'returning', v:$value}) RETURN n.v AS v",
                        "CREATE (n:ExactDecimal {variant:'chained'}) SET n.v=$value RETURN n.v AS v",
                        "MATCH (n:ExactDecimal {variant:'updated'}) SET n.v=$value",
                        "CREATE (:ExactEndpoint)-[r:EXACT_DECIMAL {v:$value}]->(:ExactEndpoint) RETURN r.v AS v" ) ) {
                    PreparedStatementSignature write = prepareNamedCypher( writer, namespace, query );
                    var response = expect( executeNamedCypher( writer, write.getStatementId(), Map.of( "value", decimal ) ), Response.TypeCase.STATEMENT_RESULT );
                    if ( response.getStatementResult().hasFrame() ) {
                        RelationalFrame frame = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "v" ) );
                        assertEquals( decimal, singleRow( frame ).getValues( 0 ), query );
                    }
                }

                PreparedStatementSignature text = prepareNamedCypher( writer, namespace, "CREATE (:ExactDecimal {variant:'text', v:$value})" );
                expect( executeNamedCypher( writer, text.getStatementId(), Map.of( "value", taggedText ) ), Response.TypeCase.STATEMENT_RESULT );

                Frame result = reader.execute( namespace, "cypher", """
                        MATCH (n:ExactDecimal) RETURN n AS node, n.variant AS variant, n.v AS v ORDER BY variant
                        """, -1 ).getResult().getFrame();
                RelationalFrame stored = relationalFrame( result, true, List.of( "node", "variant", "v" ) );
                assertEquals( 5, stored.getRowsCount() );
                for ( Row row : stored.getRowsList() ) {
                    String variant = row.getValues( 1 ).getString().getString();
                    ProtoValue expected = variant.equals( "text" ) ? taggedText : decimal;
                    assertEquals( expected, row.getValues( 2 ), variant );
                    assertEquals( expected, row.getValues( 0 ).getNode().getPropertiesOrThrow( "v" ), variant );
                }

                Frame relationship = reader.execute( namespace, "cypher", """
                        MATCH ()-[r:EXACT_DECIMAL]->() RETURN r AS edge, r.v AS v
                        """, -1 ).getResult().getFrame();
                Row edge = singleRow( relationalFrame( relationship, true, List.of( "edge", "v" ) ) );
                assertEquals( decimal, edge.getValues( 0 ).getEdge().getPropertiesOrThrow( "v" ) );
                assertEquals( decimal, edge.getValues( 1 ) );

                for ( String query : List.of(
                        "MATCH (n:ExactDecimal {v:$value}) RETURN n.variant AS variant ORDER BY variant",
                        "MATCH (n:ExactDecimal) WHERE n.v = $value RETURN n.variant AS variant ORDER BY variant" ) ) {
                    PreparedStatementSignature lookup = prepareNamedCypher( reader, namespace, query );
                    ProtoValue equivalent = PolyValueSerializer.serialize( PolyLong.of( 9_007_199_254_740_993L ) );
                    var matches = expect( executeNamedCypher( reader, lookup.getStatementId(), Map.of( "value", equivalent ) ), Response.TypeCase.STATEMENT_RESULT );
                    assertEquals( List.of( "chained", "direct", "returning", "updated" ), projectedNames(
                            relationalFrame( matches.getStatementResult().getFrame(), true, List.of( "variant" ) ) ) );
                    ProtoValue adjacent = PolyValueSerializer.serialize( PolyLong.of( 9_007_199_254_740_992L ) );
                    var misses = expect( executeNamedCypher( reader, lookup.getStatementId(), Map.of( "value", adjacent ) ), Response.TypeCase.STATEMENT_RESULT );
                    assertTrue( relationalFrame( misses.getStatementResult().getFrame(), true, List.of( "variant" ) ).getRowsList().isEmpty() );
                }
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void persistsTemporalPropertiesWithTheirCypherTypesAndPrismText() throws IOException {
        String namespace = "cyphertemporalproperties";
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                writer.execute( namespace, "cypher", """
                        CREATE (:TemporalEvent {
                            starts:datetime('2024-03-31T00:00:00.123456789[Europe/Stockholm]'),
                            literal:'2024-03-31T00:00:00.123456789+01:00[Europe/Stockholm]',
                            checkpoints:[time('12:34:56.000000001+02:00'), time('23:59:00.123456789Z')]
                        })-[:ELAPSED {span:duration('P1DT24H')}]->(:TemporalDestination)
                        """, -1 );
                Frame result = reader.execute( namespace, "cypher", """
                        MATCH (event:TemporalEvent)-[elapsed:ELAPSED]->(:TemporalDestination)
                        RETURN event, elapsed, event.starts AS starts, event.starts.nanosecond AS nanosecond,
                               event.starts.timezone AS zone, event.starts = event.literal AS sameType,
                               elapsed.span.days AS days, elapsed.span.hours AS hours
                        """, -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( result, true,
                        List.of( "event", "elapsed", "starts", "nanosecond", "zone", "sameType", "days", "hours" ) ) );
                ProtoValue starts = stringParameter( "2024-03-31T00:00:00.123456789+01:00[Europe/Stockholm]" );
                ProtoNode event = row.getValues( 0 ).getNode();
                assertEquals( starts, event.getPropertiesOrThrow( "starts" ) );
                assertEquals( starts, row.getValues( 2 ) );
                assertEquals( List.of( stringParameter( "12:34:56.000000001+02:00" ), stringParameter( "23:59:00.123456789Z" ) ),
                        event.getPropertiesOrThrow( "checkpoints" ).getList().getValuesList() );
                assertEquals( stringParameter( "P1DT24H" ), row.getValues( 1 ).getEdge().getPropertiesOrThrow( "span" ) );
                assertEquals( 123456789L, integerValue( row.getValues( 3 ) ) );
                assertEquals( stringParameter( "Europe/Stockholm" ), row.getValues( 4 ) );
                assertFalse( row.getValues( 5 ).getBoolean().getBoolean() );
                assertEquals( 1L, integerValue( row.getValues( 6 ) ) );
                assertEquals( 24L, integerValue( row.getValues( 7 ) ) );

                writer.execute( namespace, "cypher", """
                        MATCH (event:TemporalEvent) SET event.starts = event.starts + duration('P1D')
                        """, -1 );
                Frame update = reader.execute( namespace, "cypher", """
                        MATCH (event:TemporalEvent) RETURN event.starts AS starts
                        """, -1 ).getResult().getFrame();
                assertEquals( stringParameter( "2024-04-01T00:00:00.123456789+02:00[Europe/Stockholm]" ),
                        singleRow( relationalFrame( update, true, List.of( "starts" ) ) ).getValues( 0 ) );
                writer.checkTransactions( 0 );
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void evaluatesTemporalClocksForEachPrismStatementAndTransaction() throws IOException {
        try ( PrismConnection connection = new PrismConnection( false, 3 ) ) {
            String query = """
                    RETURN datetime.transaction('Z') AS tx, datetime.statement('Z') AS stmt,
                           datetime.realtime('Z') AS realtime, timestamp() AS millis,
                           datetime.statement('Z').epochMillis AS statementMillis
                    """;
            List<String> columns = List.of( "tx", "stmt", "realtime", "millis", "statementMillis" );
            Row first = singleRow( relationalFrame( connection.cypher( query, -1 ).getResult().getFrame(), true, columns ) );
            Row second = singleRow( relationalFrame( connection.cypher( query, -1 ).getResult().getFrame(), true, columns ) );
            Instant transaction = Instant.parse( first.getValues( 0 ).getString().getString() );
            Instant firstStatement = Instant.parse( first.getValues( 1 ).getString().getString() );
            Instant secondStatement = Instant.parse( second.getValues( 1 ).getString().getString() );
            assertEquals( first.getValues( 0 ), second.getValues( 0 ) );
            assertFalse( firstStatement.isBefore( transaction ) );
            assertTrue( secondStatement.isAfter( firstStatement ) );
            assertFalse( Instant.parse( second.getValues( 2 ).getString().getString() ).isBefore( secondStatement ) );
            assertEquals( firstStatement.toEpochMilli(), integerValue( first.getValues( 3 ) ) );
            assertEquals( secondStatement.toEpochMilli(), integerValue( second.getValues( 4 ) ) );
            assertEquals( second.getValues( 3 ), second.getValues( 4 ) );
            expect( connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) ), Response.TypeCase.COMMIT_RESPONSE );
            Row next = singleRow( relationalFrame( connection.cypher( query, -1 ).getResult().getFrame(), true, columns ) );
            assertTrue( Instant.parse( next.getValues( 0 ).getString().getString() ).isAfter( transaction ) );
        }
    }


    @Test
    void convertsNativePrismDateParametersWithoutChangingTheirWireType() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    RETURN $value AS input, date($value) AS converted, date($value) = $value AS equal,
                           $value.ordinalDay AS ordinalDay
                    """ );
            ProtoValue nativeDate = PolyValueSerializer.serialize( PolyDate.ofDays( 1 ) );
            Response response = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", nativeDate ) ),
                    Response.TypeCase.STATEMENT_RESULT );
            Row row = singleRow( relationalFrame( response.getStatementResult().getFrame(), true,
                    List.of( "input", "converted", "equal", "ordinalDay" ) ) );
            assertEquals( nativeDate, row.getValues( 0 ) );
            assertEquals( stringParameter( "1970-01-02" ), row.getValues( 1 ) );
            assertTrue( row.getValues( 2 ).getBoolean().getBoolean() );
            assertEquals( 2L, integerValue( row.getValues( 3 ) ) );

            String namespace = "cyphernativeprismtemporal";
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                PreparedStatementSignature write = prepareNamedCypher( connection, namespace, "CREATE (:NativePrismTemporal {value:$value})" );
                expect( executeNamedCypher( connection, write.getStatementId(), Map.of( "value", nativeDate ) ), Response.TypeCase.STATEMENT_RESULT );
                Frame stored = connection.execute( namespace, "cypher", """
                        MATCH (event:NativePrismTemporal)
                        RETURN event.value AS storedValue, event.value = date('1970-01-02') AS equal,
                               event.value.ordinalDay AS ordinalDay
                        """, -1 ).getResult().getFrame();
                Row storedRow = singleRow( relationalFrame( stored, true, List.of( "storedValue", "equal", "ordinalDay" ) ) );
                assertEquals( nativeDate, storedRow.getValues( 0 ) );
                assertTrue( storedRow.getValues( 1 ).getBoolean().getBoolean() );
                assertEquals( 2L, integerValue( storedRow.getValues( 2 ) ) );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    private static void assertStoredInteger( ProtoValue value ) {
        // Neo4j stores all integers as longs.
        assertTrue( value.hasInteger() || value.hasLong(), () -> "Expected an integer property, got " + value );
        assertEquals( 42, integerValue( value ) );
    }


    @Test
    void acceptsLongNamedSkipAndLimitArguments() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person)
                    RETURN n.name AS name ORDER BY n.id SKIP $offset LIMIT $count
                    """ );
            ProtoValue one = PolyValueSerializer.serialize( PolyLong.of( 1L ) );
            Response result = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "offset", one, "count", one ) ),
                    Response.TypeCase.STATEMENT_RESULT );
            Row row = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "name" ) ) );
            assertEquals( "Bob", row.getValues( 0 ).getString().getString() );

            ProtoValue negative = PolyValueSerializer.serialize( PolyLong.of( -1L ) );
            ProtoValue tooLarge = PolyValueSerializer.serialize( PolyLong.of( (long) Integer.MAX_VALUE + 1 ) );
            assertNamedCypherError( executeNamedCypher( connection, signature.getStatementId(), Map.of( "offset", negative, "count", one ) ),
                    "SKIP requires an integer between 0 and " + Integer.MAX_VALUE );
            assertNamedCypherError( executeNamedCypher( connection, signature.getStatementId(), Map.of( "offset", one, "count", tooLarge ) ),
                    "LIMIT requires an integer between 0 and " + Integer.MAX_VALUE );
            assertNamedCypherError( executeNamedCypher( connection, signature.getStatementId(), Map.of( "offset", one, "count", stringParameter( "1" ) ) ),
                    "LIMIT requires a non-negative integer" );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void countsNonNullNamedArgumentsWhenReusingTheStatement() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, "MATCH (n:Person) RETURN count($value) AS counted" );

            for ( ProtoValue value : List.of( PolyValueSerializer.serialize( PolyNull.NULL ), PolyValueSerializer.serialize( PolyInteger.of( 1 ) ) ) ) {
                Response response = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", value ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame frame = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "counted" ) );
                ProtoValue counted = singleRow( frame ).getValues( 0 );
                assertTrue( counted.hasLong() );
                assertEquals( value.hasNull() ? 0 : 3, counted.getLong().getLong() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void createsGraphPropertiesFromBoundScalars() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            try {
                PreparedStatementSignature signature = prepareNamedCypher( connection, "CREATE (:PreparedPerson {name: $name})" );
                String name = "D'Arcy\\'\\\" \tπ";
                Response response = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "name", stringParameter( name ) ) ), Response.TypeCase.STATEMENT_RESULT );
                assertFalse( response.getStatementResult().hasFrame() );
                connection.checkTransactions( 0 );
                Frame stored = connection.cypher( "MATCH (n:PreparedPerson) RETURN n.name AS name", -1 ).getResult().getFrame();
                assertEquals( name, singleRow( relationalFrame( stored, true, List.of( "name" ) ) ).getValues( 0 ).getString().getString() );

                PreparedStatementSignature lookup = prepareNamedCypher( connection, """
                        MATCH (n:PreparedPerson)
                        WHERE n.name = $name
                        RETURN n.name AS name
                        """ );
                Response selected = expect( executeNamedCypher( connection, lookup.getStatementId(), Map.of( "name", stringParameter( name ) ) ), Response.TypeCase.STATEMENT_RESULT );
                assertEquals( name, singleRow( relationalFrame( selected.getStatementResult().getFrame(), true, List.of( "name" ) ) ).getValues( 0 ).getString().getString() );

                PreparedStatementSignature prefix = prepareNamedCypher( connection, """
                        MATCH (n:PreparedPerson)
                        WHERE n.name STARTS WITH $prefix
                        RETURN n.name AS name
                        """ );
                Response selectedByPrefix = expect( executeNamedCypher( connection, prefix.getStatementId(), Map.of( "prefix", stringParameter( "D'" ) ) ), Response.TypeCase.STATEMENT_RESULT );
                assertEquals( name, singleRow( relationalFrame( selectedByPrefix.getStatementResult().getFrame(), true, List.of( "name" ) ) ).getValues( 0 ).getString().getString() );
            } finally {
                connection.cypher( "MATCH (n:PreparedPerson) DELETE n", -1 );
            }
        }
    }


    @Test
    void canPrepareNamedCypherWhileAnotherResultIsOpen() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            StatementResponse open = connection.cypher( "MATCH (n:Person) RETURN n ORDER BY n.id", 1 );
            assertNode( graphElements( open.getResult().getFrame(), false ).get( 0 ), "Alice", 1 );
            connection.checkTransactions( 1 );

            PreparedStatementSignature signature = prepareNamedCypher( connection, "RETURN $value AS supplied" );
            Map<String, ProtoValue> values = Map.of( "value", stringParameter( "after fetch" ) );
            assertOpenResultError( executeNamedCypher( connection, signature.getStatementId(), values ) );
            connection.checkTransactions( 1 );
            List<GraphElement> rest = graphElements( connection.fetch( open.getStatementId(), 2 ), true );
            assertNode( rest.get( 0 ), "Bob", 2 );
            assertNode( rest.get( 1 ), "Charlie", 3 );
            connection.checkTransactions( 0 );

            Response response = expect( executeNamedCypher( connection, signature.getStatementId(), values ), Response.TypeCase.STATEMENT_RESULT );
            Row row = singleRow( relationalFrame( response.getStatementResult().getFrame(), true, List.of( "supplied" ) ) );
            assertEquals( "after fetch", row.getValues( 0 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    private static PreparedStatementSignature prepareNamedCypher( PrismConnection connection, String query ) throws IOException {
        return prepareNamedCypher( connection, NAMESPACE, query );
    }


    static PreparedStatementSignature prepareNamedCypher( PrismConnection connection, String namespace, String query ) throws IOException {
        return expect( connection.send( Request.newBuilder().setPrepareNamedStatementRequest( PrepareStatementRequest.newBuilder()
                .setNamespaceName( namespace )
                .setLanguageName( "cypher" )
                .setStatement( query ) ) ), Response.TypeCase.PREPARED_STATEMENT_SIGNATURE ).getPreparedStatementSignature();
    }


    static Response executeNamedCypher( PrismConnection connection, int statementId, Map<String, ProtoValue> values ) throws IOException {
        return connection.send( Request.newBuilder().setExecuteNamedStatementRequest( ExecuteNamedStatementRequest.newBuilder()
                .setStatementId( statementId )
                .setParameters( NamedParameters.newBuilder().putAllParameters( values ) )
                .setFetchSize( -1 ) ) );
    }


    private static void assertNamedCypherError( Response response, String message ) {
        String actual = expect( response, Response.TypeCase.ERROR_RESPONSE ).getErrorResponse().getMessage();
        assertTrue( actual.contains( message ), () -> actual );
    }


    private static ProtoValue stringParameter( String value ) {
        return ProtoValue.newBuilder().setString( ProtoString.newBuilder().setString( value ) ).build();
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
            assertTrue( firstPage.getColumnMeta( 0 ).getIsNullable() );
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
    void reportsMissingPropertiesAsNullableAcrossMixedResultPages() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            StatementResponse statement = connection.cypher( "MATCH (n:Person) RETURN n, n.missing AS missing", 2 );
            List<String> labels = List.of( "n", "missing" );
            RelationalFrame first = relationalFrame( statement.getResult().getFrame(), false, labels );
            RelationalFrame last = relationalFrame( connection.fetch( statement.getStatementId(), 2 ), true, labels );
            assertEquals( 2, first.getRowsCount() );
            assertEquals( 1, last.getRowsCount() );
            assertEquals( first.getColumnMetaList(), last.getColumnMetaList() );
            assertTrue( first.getColumnMeta( 1 ).getIsNullable() );
            for ( RelationalFrame page : List.of( first, last ) ) {
                for ( Row row : page.getRowsList() ) {
                    assertTrue( row.getValues( 0 ).hasNode() );
                    assertTrue( row.getValues( 1 ).hasNull() );
                }
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void comparesMissingPropertiesAsNullableBooleans() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher(
                    "MATCH (n:Person) RETURN n, n.missing = n.missing AS comparison", -1 ).getResult().getFrame();
            RelationalFrame result = relationalFrame( frame, true, List.of( "n", "comparison" ) );
            assertEquals( ProtoPolyType.BOOLEAN, result.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
            assertTrue( result.getColumnMeta( 1 ).getIsNullable() );
            assertEquals( 3, result.getRowsCount() );
            for ( Row row : result.getRowsList() ) {
                assertTrue( row.getValues( 0 ).hasNode() );
                assertTrue( row.getValues( 1 ).hasNull() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void reprojectsMissingPropertiesUnderAnExistingAlias() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( """
                    MATCH (n:Person)
                    WITH n.name AS picked, n
                    RETURN n.missing AS picked, n
                    """, -1 ).getResult().getFrame();
            RelationalFrame result = relationalFrame( frame, true, List.of( "picked", "n" ) );
            assertTrue( result.getColumnMeta( 0 ).getIsNullable() );
            assertEquals( 3, result.getRowsCount() );
            for ( Row row : result.getRowsList() ) {
                assertTrue( row.getValues( 0 ).hasNull() );
                assertTrue( row.getValues( 1 ).hasNode() );
            }
            connection.checkTransactions( 0 );
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
    void pagesUnboundedPathsInRelationalFrames() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            StatementResponse statement = connection.cypher( "MATCH p=(:Person {name:'Alice'})-[*]->(:Person) RETURN p AS path", 1 );
            RelationalFrame first = relationalFrame( statement.getResult().getFrame(), false, List.of( "path" ) );
            connection.checkTransactions( 1 );
            RelationalFrame second = relationalFrame( connection.fetch( statement.getStatementId(), 1 ), true, List.of( "path" ) );

            List<ProtoPath> paths = List.of( first, second ).stream().map( page -> {
                assertEquals( ProtoPolyType.PATH, page.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
                ProtoValue value = singleRow( page ).getValues( 0 );
                assertTrue( value.hasPath() );
                return value.getPath();
            } ).sorted( Comparator.comparingInt( ProtoPath::getEdgesCount ) ).toList();
            assertEquals( 2, paths.get( 0 ).getNodesCount() );
            assertEquals( 1, paths.get( 0 ).getEdgesCount() );
            assertEquals( 3, paths.get( 1 ).getNodesCount() );
            assertEquals( 2, paths.get( 1 ).getEdgesCount() );
            ProtoNode alice = assertNode( paths.get( 1 ).getNodes( 0 ), "Alice", 1 );
            ProtoNode bob = assertNode( paths.get( 1 ).getNodes( 1 ), "Bob", 2 );
            ProtoNode charlie = assertNode( paths.get( 1 ).getNodes( 2 ), "Charlie", 3 );
            assertEquals( List.of( alice, bob ), paths.get( 0 ).getNodesList() );
            assertEdge( paths.get( 0 ).getEdges( 0 ), alice, bob, 2020 );
            assertEdge( paths.get( 1 ).getEdges( 0 ), alice, bob, 2020 );
            assertEdge( paths.get( 1 ).getEdges( 1 ), bob, charlie, 2021 );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void emptyRelationshipIntervalsHaveNoMatches() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            for ( String range : List.of( "2..1", "1..0", "..0" ) ) {
                String pattern = "(:Person {name:'Alice'})-[:KNOWS*%s]->(:Person)".formatted( range );
                Frame frame = connection.cypher( "MATCH p=" + pattern + " RETURN p", -1 ).getResult().getFrame();
                assertEquals( 0, relationalFrame( frame, true, List.of( "p" ) ).getRowsCount(), range );

                Frame optional = connection.cypher( "MATCH (a:Person {name:'Alice'}) "
                        + "OPTIONAL MATCH p=(a)-[r:KNOWS*%s]->(b:Person) ".formatted( range )
                        + "RETURN a.name AS origin, p, r, b", -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( optional, true, List.of( "origin", "p", "r", "b" ) ) );
                assertEquals( "Alice", row.getValues( 0 ).getString().getString() );
                assertTrue( row.getValuesList().subList( 1, 4 ).stream().allMatch( ProtoValue::hasNull ), range );
            }

            Frame zero = connection.cypher( "MATCH p=(:Person {name:'Alice'})-[r:ABSENT*0..0]->(b:Person) "
                    + "RETURN p, r, b.name AS target", -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( zero, true, List.of( "p", "r", "target" ) ) );
            assertEquals( 1, row.getValues( 0 ).getPath().getNodesCount() );
            assertEquals( 0, row.getValues( 0 ).getPath().getEdgesCount() );
            assertTrue( row.getValues( 1 ).hasList() );
            assertEquals( 0, row.getValues( 1 ).getList().getValuesCount() );
            assertEquals( "Alice", row.getValues( 2 ).getString().getString() );
        }
    }


    @Test
    void enforcesRelationshipUniquenessAcrossPatternsInTheFirstMatch() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String path = "(:Person {name:'Alice'})-[:KNOWS*]->(:Person {name:'Bob'})";
            for ( var entry : Map.of(
                    "MATCH p=" + path + ", q=" + path, 0,
                    "MATCH " + path + ", " + path, 0,
                    "MATCH p=" + path + " MATCH q=" + path, 1,
                    "MATCH ()-[:KNOWS]->(), ()-[:KNOWS]->()", 2,
                    "MATCH ()-[:KNOWS*]->(), ()-[:KNOWS*]->(), ()-[:KNOWS*]->()", 0,
                    "MATCH ()-[*0]->(), ()-[*0]->()", 9,
                    "MATCH ()-[:KNOWS]->(), ()", 6 ).entrySet() ) {
                Frame frame = connection.cypher( entry.getKey() + " RETURN count(*) AS total", -1 ).getResult().getFrame();
                assertEquals( entry.getValue().longValue(), integerValue( singleRow( relationalFrame( frame, true, List.of( "total" ) ) ).getValues( 0 ) ), entry.getKey() );
            }
        }
    }


    @Test
    void joinsSharedNodesAndFiltersTheFirstMatch() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String start = "MATCH (a)-[:KNOWS*]->(middle:Person {name:'Bob'}), ";
            String projection = " RETURN a.name AS origin, middle.name AS midpoint, c.name AS target";
            List<String> columns = List.of( "origin", "midpoint", "target" );
            Frame matched = connection.cypher( start + "(middle)-[:KNOWS*]->(c) WHERE a.name = 'Alice' AND c.name = 'Charlie'" + projection, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( matched, true, columns ) );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), row.getValuesList().stream().map( value -> value.getString().getString() ).toList() );

            Frame absent = connection.cypher( start + "(middle:Person {name:'Charlie'})<-[:KNOWS*]-(c) WHERE c.name = 'Bob'" + projection, -1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( absent, true, columns ).getRowsCount() );
        }
    }


    @Test
    void writesOnlyRowsWithValidJoinedMatchBindings() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (:JoinedWriteSource {name:'left'})"
                    + "-[:JOINED_WRITE_LINK]->(:JoinedWriteMiddle)-[:JOINED_WRITE_LINK]->(:JoinedWriteTarget {name:'right'})", -1 );
            String query = """
                    MATCH (a:JoinedWriteSource)-[:JOINED_WRITE_LINK*]->(middle:JoinedWriteMiddle),
                          (middle)-[:JOINED_WRITE_LINK*]->(b:JoinedWriteTarget)
                    CREATE (a)-[:JOINED_WRITE_CREATED]->(b)
                    RETURN a.name AS origin, b.name AS target
                    """;
            Frame frame = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( frame, true, List.of( "origin", "target" ) ) );
            assertEquals( List.of( "left", "right" ), row.getValuesList().stream().map( value -> value.getString().getString() ).toList() );
            Frame persisted = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH ()-[:JOINED_WRITE_CREATED]->() RETURN count(*) AS total", -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( persisted, true, List.of( "total" ) ) ).getValues( 0 ) ) );
        }
    }


    @Test
    void matchesFixedTypeAlternativesBesideAnUnboundedSegment() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (start:TypeAlternativeStart)-[:TYPE_OPTION_A]->()-[:TYPE_CONTINUATION]->(:TypeAlternativeEnd),
                           (start)-[:TYPE_OPTION_B]->()-[:TYPE_CONTINUATION]->(:TypeAlternativeEnd),
                           (start)-[:TYPE_OPTION_C]->()-[:TYPE_CONTINUATION]->(:TypeAlternativeEnd)
                    """, -1 );
            Frame frame = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(:TypeAlternativeStart)-[:TYPE_OPTION_A|TYPE_OPTION_B]->()-[:TYPE_CONTINUATION*]->(:TypeAlternativeEnd)
                    RETURN p
                    """, -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( frame, true, List.of( "p" ) );
            assertEquals( 2, rows.getRowsCount() );
            List<String> types = rows.getRowsList().stream().map( row -> {
                ProtoPath path = row.getValues( 0 ).getPath();
                assertEquals( 2, path.getEdgesCount() );
                assertEquals( List.of( "TYPE_CONTINUATION" ), path.getEdges( 1 ).getLabelsList() );
                return path.getEdges( 0 ).getLabels( 0 );
            } ).sorted().toList();
            assertEquals( List.of( "TYPE_OPTION_A", "TYPE_OPTION_B" ), types );
        }
    }


    @Test
    void preservesImplicitBoundedPathEndpointsInEnumerableProjections() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String bounds : List.of( "..2", "1..2" ) ) {
                Frame frame = connection.cypher( "MATCH (a:Person {name:'Alice'})-[:KNOWS*%s]->(b:Person) ".formatted( bounds )
                        + "RETURN b.name AS endpoint, NULL AS missing", -1 ).getResult().getFrame();
                RelationalFrame rows = relationalFrame( frame, true, List.of( "endpoint", "missing" ) );
                assertEquals( 2, rows.getRowsCount() );
                assertEquals( List.of( "Bob", "Charlie" ), rows.getRowsList().stream()
                        .map( row -> row.getValues( 0 ).getString().getString() ).sorted().toList() );
                assertTrue( rows.getRowsList().stream().allMatch( row -> row.getValues( 1 ).hasNull() ) );
            }
        }
    }


    @Test
    void returnsNodesAsListElements() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( "MATCH (n:Person) RETURN [n] AS people", -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( frame, true, List.of( "people" ) );
            var type = rows.getColumnMeta( 0 ).getTypeMeta();
            assertEquals( ProtoPolyType.ARRAY, type.getProtoValueType() );
            assertEquals( ProtoPolyType.NODE, type.getArrayMeta().getElementType().getProtoValueType() );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), rows.getRowsList().stream().map( row -> {
                ProtoValue value = row.getValues( 0 );
                assertTrue( value.hasList() );
                ProtoList people = value.getList();
                assertEquals( 1, people.getValuesCount() );
                assertTrue( people.getValues( 0 ).hasNode() );
                return people.getValues( 0 ).getNode().getPropertiesOrThrow( "name" ).getString().getString();
            } ).sorted().toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void returnsPathsAsListElements() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( var expected : Map.of( "*", List.of( 1, 2 ), "*0..", List.of( 0, 1, 2 ) ).entrySet() ) {
                Frame frame = connection.cypher( "MATCH p=(:Person {name:'Alice'})-[:KNOWS%s]->(:Person) RETURN [p] AS paths"
                        .formatted( expected.getKey() ), -1 ).getResult().getFrame();
                RelationalFrame rows = relationalFrame( frame, true, List.of( "paths" ) );
                assertEquals( ProtoPolyType.PATH, rows.getColumnMeta( 0 ).getTypeMeta().getArrayMeta().getElementType().getProtoValueType() );
                assertEquals( expected.getValue(), rows.getRowsList().stream().map( row -> {
                    ProtoList paths = row.getValues( 0 ).getList();
                    assertEquals( 1, paths.getValuesCount() );
                    assertTrue( paths.getValues( 0 ).hasPath() );
                    ProtoPath path = paths.getValues( 0 ).getPath();
                    assertEquals( path.getEdgesCount() + 1, path.getNodesCount() );
                    assertEquals( "Alice", path.getNodes( 0 ).getPropertiesOrThrow( "name" ).getString().getString() );
                    return path.getEdgesCount();
                } ).sorted().toList() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesEscapedPathNamesAndAliases() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame paths = connection.cypher( """
                    MATCH `route name`=(:Person {name:'Alice'})-[:KNOWS*]->(:Person)
                    WITH `route name` AS `path.result`
                    RETURN [`path.result`] AS `path list`
                    """, -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( paths, true, List.of( "path list" ) );
            assertEquals( List.of( 1, 2 ), rows.getRowsList().stream().map( row ->
                    row.getValues( 0 ).getList().getValues( 0 ).getPath().getEdgesCount() ).sorted().toList() );

            Frame fixed = connection.cypher( """
                    MATCH p=(`start node`:Person {name:'Alice'})-[`route edge`:KNOWS]->(`end.node`:Person)
                    RETURN p AS `path.result`, `end.node`.name AS `end name`, `route edge` AS `chosen edge`, NULL AS missing
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( fixed, true, List.of( "path.result", "end name", "chosen edge", "missing" ) ) );
            assertEquals( "Bob", row.getValues( 1 ).getString().getString() );
            assertEquals( 1, row.getValues( 0 ).getPath().getEdgesCount() );
            assertEquals( row.getValues( 0 ).getPath().getEdges( 0 ).getId(), row.getValues( 2 ).getEdge().getId() );
            assertTrue( row.getValues( 3 ).hasNull() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesEscapedNodeAndPropertyAliasesInNativeProjections() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( """
                    MATCH (`n dot.name`:Person)
                    RETURN `n dot.name`.name AS `person.result` ORDER BY `person.result`
                    """, -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( frame, true, List.of( "person.result" ) );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), rows.getRowsList().stream()
                    .map( row -> row.getValues( 0 ).getString().getString() ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesEscapedLabelsTypesAndPropertyKeys() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:`Quoted Start` {`given name`:'A'})-[:`Quoted Link`]->
                           (:`Quoted End` {`given name`:'B'})
                    """, -1 );
            Frame frame = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(`start node`:`Quoted Start` {`given name`:'A'})-[:`Quoted Link`]->(`end node`:`Quoted End`)
                    RETURN p, `end node`.`given name` AS `end name`
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( frame, true, List.of( "p", "end name" ) ) );
            assertEquals( "B", row.getValues( 1 ).getString().getString() );
            ProtoPath path = row.getValues( 0 ).getPath();
            assertEquals( List.of( "Quoted Start" ), path.getNodes( 0 ).getLabelsList() );
            assertEquals( List.of( "Quoted Link" ), path.getEdges( 0 ).getLabelsList() );
            assertEquals( List.of( "Quoted End" ), path.getNodes( 1 ).getLabelsList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesMapLiteralsBeforeReducingThemToScalarResults() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame counted = connection.cypher( "MATCH (n:Person) RETURN count({a:1}) AS total", -1 ).getResult().getFrame();
            assertEquals( 3, integerValue( singleRow( relationalFrame( counted, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame filtered = connection.cypher( "MATCH (n:Person) WITH {a:1} AS payload RETURN payload IS NOT NULL AS present", -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( filtered, true, List.of( "present" ) );
            assertEquals( 3, rows.getRowsCount() );
            assertTrue( rows.getRowsList().stream().allMatch( row -> row.getValues( 0 ).getBoolean().getBoolean() ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void countsNamedAndAnonymousLabelledNodes() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String query : List.of( "MATCH (:Person) RETURN count(*) AS total", "MATCH (n:Person) RETURN count(n) AS total" ) ) {
                Frame frame = connection.cypher( query, -1 ).getResult().getFrame();
                assertEquals( 3, integerValue( singleRow( relationalFrame( frame, true, List.of( "total" ) ) ).getValues( 0 ) ), query );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesImplicitFixedPathBindingsInEnumerableProjections() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( "MATCH ()-[r:KNOWS]->() RETURN r, NULL AS missing", -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( frame, true, List.of( "r", "missing" ) );
            assertEquals( 2, rows.getRowsCount() );
            assertEquals( List.of( 2020L, 2021L ), rows.getRowsList().stream().map( row -> {
                assertTrue( row.getValues( 0 ).hasEdge() );
                assertTrue( row.getValues( 1 ).hasNull() );
                return integerValue( row.getValues( 0 ).getEdge().getPropertiesOrThrow( "since" ) );
            } ).sorted().toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesFixedRelationshipBindingsAfterAnEnumerableFilter() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame reused = connection.cypher( """
                    MATCH ()-[r:KNOWS]->() WHERE r.since >= 2021
                    MATCH ()-[r:KNOWS]->()
                    RETURN count(*) AS total
                    """, -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( reused, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame constructed = connection.cypher( """
                    MATCH ()-[r1:KNOWS]->()-[r2:KNOWS]->() WHERE r1.since >= 2020
                    WITH [r1,r2] AS rs LIMIT 1
                    MATCH (a)-[rs*]->(b)
                    RETURN b.name AS target, rs
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( constructed, true, List.of( "target", "rs" ) ) );
            assertEquals( "Charlie", row.getValues( 0 ).getString().getString() );
            assertEquals( List.of( 2020L, 2021L ), row.getValues( 1 ).getList().getValuesList().stream()
                    .map( value -> integerValue( value.getEdge().getPropertiesOrThrow( "since" ) ) ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesUserLabelsInMatchedPathsAndPropertyWrites() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:LabelAuditSource)-[:LABEL_AUDIT]->(:LabelAuditTarget:PublicLabel__:__namespacePortrait)", -1 );
            Frame matched = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(a:LabelAuditSource)-[:LABEL_AUDIT*1..2]->(b:PublicLabel__) RETURN p
                    """, -1 ).getResult().getFrame();
            ProtoPath path = singleRow( relationalFrame( matched, true, List.of( "p" ) ) ).getValues( 0 ).getPath();
            Set<String> labels = Set.of( "LabelAuditTarget", "PublicLabel__", "__namespacePortrait" );
            assertEquals( labels, Set.copyOf( path.getNodes( 1 ).getLabelsList() ) );

            Frame written = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (:LabelAuditSource)-[:LABEL_AUDIT*]->(b:LabelAuditTarget)
                    SET b.flag = true RETURN b
                    """, -1 ).getResult().getFrame();
            ProtoNode affected = singleRow( relationalFrame( written, true, List.of( "b" ) ) ).getValues( 0 ).getNode();
            assertEquals( labels, Set.copyOf( affected.getLabelsList() ) );

            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:LabelAuditTarget:PublicLabel__:__namespacePortrait) RETURN n.flag AS flag
                    """, -1 ).getResult().getFrame();
            assertTrue( singleRow( relationalFrame( stored, true, List.of( "flag" ) ) ).getValues( 0 ).getBoolean().getBoolean() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void refreshesGraphValuesInsideListsAndMapsAfterWrites() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:NestedWriteStart)-[:NESTED_WRITE {state:'old'}]->(:NestedWriteEnd)", -1 );
            Frame updated = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(:NestedWriteStart)-[rs:NESTED_WRITE*1..1]->(:NestedWriteEnd)
                    WITH rs, [[p]] AS nested, {edge:last(rs)} AS projected
                    MATCH (:NestedWriteStart)-[r:NESTED_WRITE]->(:NestedWriteEnd)
                    SET r.state='new'
                    RETURN rs, nested, r, (projected.edge).state AS mapState
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( updated, true, List.of( "rs", "nested", "r", "mapState" ) ) );
            assertEquals( "new", row.getValues( 3 ).getString().getString() );
            ProtoEdge direct = row.getValues( 2 ).getEdge();
            ProtoEdge inRelationshipList = row.getValues( 0 ).getList().getValues( 0 ).getEdge();
            ProtoEdge inPathList = row.getValues( 1 ).getList().getValues( 0 ).getList().getValues( 0 ).getPath().getEdges( 0 );
            for ( ProtoEdge edge : List.of( direct, inRelationshipList, inPathList ) ) {
                assertEquals( direct.getId(), edge.getId() );
                assertEquals( "new", edge.getPropertiesOrThrow( "state" ).getString().getString() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesUserPropertiesWithTheNamesOfGraphStorageMetadata() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:StoragePropsStart {_id:'user-id', __polypheny_user_property___id:'literal'})
                           -[:STORAGE_PROPS {__sourceId__:'user-source', __targetId__:'user-target'}]->(:StoragePropsEnd)
                    """, -1 );
            Frame matched = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(:StoragePropsStart {_id:'user-id'})-[r:STORAGE_PROPS]->(:StoragePropsEnd)
                    WHERE r.__targetId__ = 'user-target'
                    RETURN p
                    """, -1 ).getResult().getFrame();
            ProtoPath path = singleRow( relationalFrame( matched, true, List.of( "p" ) ) ).getValues( 0 ).getPath();
            assertEquals( "user-id", path.getNodes( 0 ).getPropertiesOrThrow( "_id" ).getString().getString() );
            assertEquals( "literal", path.getNodes( 0 ).getPropertiesOrThrow( "__polypheny_user_property___id" ).getString().getString() );
            assertEquals( "user-source", path.getEdges( 0 ).getPropertiesOrThrow( "__sourceId__" ).getString().getString() );
            assertEquals( "user-target", path.getEdges( 0 ).getPropertiesOrThrow( "__targetId__" ).getString().getString() );
            assertFalse( path.getNodes( 1 ).getPropertiesMap().containsKey( "_id" ) );
            assertEquals( path.getNodes( 0 ).getId(), path.getEdges( 0 ).getLeft() );
            assertEquals( path.getNodes( 1 ).getId(), path.getEdges( 0 ).getRight() );

            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:StoragePropsStart)-[r:STORAGE_PROPS]->(b:StoragePropsEnd)
                    SET r.__sourceId__ = 'updated-source'
                    CREATE (a)-[:STORAGE_PROP_COPY]->(b)
                    """, -1 );
            Frame updated = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (:StoragePropsStart)-[r:STORAGE_PROPS]->(:StoragePropsEnd) RETURN r.__sourceId__ AS source
                    """, -1 ).getResult().getFrame();
            assertEquals( "updated-source", singleRow( relationalFrame( updated, true, List.of( "source" ) ) )
                    .getValues( 0 ).getString().getString() );

            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH ()-[r:STORAGE_PROPS]->() DELETE r", -1 );
            Frame remaining = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (:StoragePropsStart)-[r]->(:StoragePropsEnd) RETURN r", -1 ).getResult().getFrame();
            assertEquals( List.of( "STORAGE_PROP_COPY" ), singleRow( relationalFrame( remaining, true, List.of( "r" ) ) )
                    .getValues( 0 ).getEdge().getLabelsList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void returnsAProjectedPathAfterItsPatternBindingsLeaveScope() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( """
                    MATCH p=(a:Person {name:'Alice'})-[r:KNOWS]->(b:Person)
                    WITH p WHERE p IS NOT NULL
                    RETURN p, NULL AS missing
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( frame, true, List.of( "p", "missing" ) ) );
            assertEquals( 1, row.getValues( 0 ).getPath().getEdgesCount() );
            assertTrue( row.getValues( 1 ).hasNull() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void ordersMatchedGraphValuesByTheirPolyphenyIds() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( """
                    MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b:Person)
                    RETURN a, b, a < b AS smaller, b > a AS greater, p < p AS strict, p <= p AS inclusive
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( frame, true, List.of( "a", "b", "smaller", "greater", "strict", "inclusive" ) ) );
            boolean firstIsSmaller = row.getValues( 0 ).getNode().getId().compareTo( row.getValues( 1 ).getNode().getId() ) < 0;
            assertEquals( firstIsSmaller, row.getValues( 2 ).getBoolean().getBoolean() );
            assertEquals( firstIsSmaller, row.getValues( 3 ).getBoolean().getBoolean() );
            assertFalse( row.getValues( 4 ).getBoolean().getBoolean() );
            assertTrue( row.getValues( 5 ).getBoolean().getBoolean() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void matchesRequiredNodeLabelsIndependentOfAdditionalLabelsAndOrder() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:RangeArtist:RangeArtistA {name:'A'})-[:RANGE_COLLABORATION {year:1987}]->
                           (:RangeArtist:RangeArtistB {name:'B'})-[:RANGE_COLLABORATION {year:1988}]->
                           (:RangeArtist:RangeArtistC {name:'C'})
                    """, -1 );
            String projection = " RETURN a.name AS origin, b.name AS target";
            List<String> columns = List.of( "origin", "target" );
            for ( String pattern : List.of(
                    "(a:RangeArtist)-[:RANGE_COLLABORATION* {year:1988}]->(b:RangeArtist)",
                    "(a:RangeArtistB:RangeArtist)-[:RANGE_COLLABORATION* {year:1988}]->(b:RangeArtistC:RangeArtist)" ) ) {
                Frame frame = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH " + pattern + projection, -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( frame, true, columns ) );
                assertEquals( List.of( "B", "C" ), row.getValuesList().stream().map( value -> value.getString().getString() ).toList() );
            }

            Frame absent = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (a:RangeArtistB:RangeArtist)-[:RANGE_COLLABORATION*]->(b:RangeArtistC:RangeArtistA)" + projection, -1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( absent, true, columns ).getRowsCount() );

            Frame zero = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (a:RangeArtistB)-[:RANGE_NEVER_USED*0]->(b:RangeArtist)" + projection, -1 ).getResult().getFrame();
            Row sameNode = singleRow( relationalFrame( zero, true, columns ) );
            assertEquals( List.of( "B", "B" ), sameNode.getValuesList().stream().map( value -> value.getString().getString() ).toList() );

            Frame nodeOnly = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:RangeArtistB:RangeArtistB) RETURN n.name AS artist", -1 ).getResult().getFrame();
            assertEquals( "B", singleRow( relationalFrame( nodeOnly, true, List.of( "artist" ) ) ).getValues( 0 ).getString().getString() );
        }
    }


    @Test
    void matchesAConstructedRelationshipListInTraversalOrder() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String start = "MATCH ()-[r1:KNOWS]->()-[r2:KNOWS]->() WITH %s AS rs LIMIT 1 ";
            String projection = " RETURN first.name AS origin, rs, second.name AS target";
            List<String> columns = List.of( "origin", "rs", "target" );
            for ( boolean reversed : List.of( false, true ) ) {
                String list = reversed ? "[r2, r1]" : "[r1, r2]";
                String matching = reversed ? "MATCH (first)<-[rs*]-(second)" : "MATCH (first)-[rs*]->(second)";
                RelationalFrame matches = relationalFrame( connection.cypher( start.formatted( list ) + matching + projection, -1 )
                        .getResult().getFrame(), true, columns );
                Row row = singleRow( matches );
                assertEquals( reversed ? "Charlie" : "Alice", row.getValues( 0 ).getString().getString() );
                assertEquals( reversed ? "Alice" : "Charlie", row.getValues( 2 ).getString().getString() );
                assertEquals( ProtoPolyType.EDGE, matches.getColumnMeta( 1 ).getTypeMeta().getArrayMeta().getElementType().getProtoValueType() );
                List<Long> years = row.getValues( 1 ).getList().getValuesList().stream()
                        .map( value -> integerValue( value.getEdge().getPropertiesOrThrow( "since" ) ) ).toList();
                assertEquals( reversed ? List.of( 2021L, 2020L ) : List.of( 2020L, 2021L ), years );
            }

            Frame absent = connection.cypher( start.formatted( "[r2, r1]" ) + "MATCH (first)-[rs*]->(second)" + projection, -1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( absent, true, columns ).getRowsCount() );

            Frame scalarList = connection.cypher( "MATCH (n:Person {name:'Alice'}) RETURN [n.name, NULL] AS items", -1 ).getResult().getFrame();
            ProtoList items = singleRow( relationalFrame( scalarList, true, List.of( "items" ) ) ).getValues( 0 ).getList();
            assertEquals( 2, items.getValuesCount() );
            assertEquals( "Alice", items.getValues( 0 ).getString().getString() );
            assertTrue( items.getValues( 1 ).hasNull() );

            Frame mixedList = connection.cypher( "MATCH (n:Person {name:'Alice'}) RETURN [n, 1] AS items", -1 ).getResult().getFrame();
            ProtoList mixed = singleRow( relationalFrame( mixedList, true, List.of( "items" ) ) ).getValues( 0 ).getList();
            assertEquals( 2, mixed.getValuesCount() );
            assertNode( mixed.getValues( 0 ).getNode(), "Alice", 1 );
            assertEquals( 1, integerValue( mixed.getValues( 1 ) ) );
        }
    }


    @Test
    void matchesAnEmptyBoundListOnlyWhenZeroHopsAreAllowed() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String query = "WITH [] AS rs MATCH (:Person)-[rs*%s]->(:Person) RETURN count(*) AS total";
            for ( var expected : Map.of( "0..", 3L, "0", 3L, "", 0L ).entrySet() ) {
                Frame frame = connection.cypher( query.formatted( expected.getKey() ), -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( frame, true, List.of( "total" ) ) );
                assertEquals( expected.getValue().longValue(), integerValue( row.getValues( 0 ) ), expected.getKey() );
            }
        }
    }


    @Test
    void validatesDynamicRelationshipListBindingsBeforeMatching() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
                for ( String start : List.of( "Person", "NoSuchLabel" ) ) {
                    String query = "WITH last([null,1]) AS rs %s (:%s)-[rs:KNOWS*]->(n) RETURN count(*) AS total".formatted( match, start );
                    Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    String message = error.getErrorResponse().getMessage();
                    assertTrue( message.contains( "MATCH requires a relationship list binding" ), message );
                    connection.checkTransactions( 0 );
                }
            }

            String match = " AS rs MATCH (:Person)-[rs:KNOWS*]->() RETURN count(*) AS total";
            Frame nonRelationship = connection.cypher( "WITH last([null,[1]])" + match, -1 ).getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( nonRelationship, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame unmatched = connection.cypher( "WITH [null] AS rs OPTIONAL MATCH (:Person)-[rs:KNOWS*]->(n) RETURN rs,n", -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( unmatched, true, List.of( "rs", "n" ) ) );
            assertEquals( 1, row.getValues( 0 ).getList().getValuesCount() );
            assertTrue( row.getValues( 0 ).getList().getValues( 0 ).hasNull() );
            assertTrue( row.getValues( 1 ).hasNull() );
        }
    }


    @Test
    void rejectsDynamicallyTypedScalarsAsNodeBindingsWithoutMatchCandidates() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
                for ( String start : List.of( "Person", "NoSuchLabel" ) ) {
                    String query = "WITH last([null,1]) AS a %s (a:%s)-[:KNOWS*]->() RETURN count(*)".formatted( match, start );
                    Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    String message = error.getErrorResponse().getMessage();
                    assertTrue( message.contains( "MATCH requires a NODE binding" ), message );
                    connection.checkTransactions( 0 );
                }
            }
        }
    }


    @Test
    void doesNotValidateBindingsWhenTheMatchPredicateIsFalse() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String input = "WITH last([null,1]) AS n ";
            Frame match = connection.cypher( input + "MATCH (n:Person) WHERE false RETURN count(*) AS total", -1 )
                    .getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( match, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame optional = connection.cypher( input + "OPTIONAL MATCH (n:Person)-[:KNOWS*]->(b) WHERE false RETURN n,b", -1 )
                    .getResult().getFrame();
            Row row = singleRow( relationalFrame( optional, true, List.of( "n", "b" ) ) );
            assertEquals( 1, integerValue( row.getValues( 0 ) ) );
            assertTrue( row.getValues( 1 ).hasNull() );

            Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher",
                    input + "OPTIONAL MATCH (n:Person) WHERE null RETURN n", -1 ) ), Response.TypeCase.ERROR_RESPONSE );
            assertTrue( error.getErrorResponse().getMessage().contains( "MATCH requires a NODE binding" ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesInlineMatchPredicatesEvenWithANullWhere() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
                for ( String guard : List.of( "null", "false" ) ) {
                    String query = "%s (n:Person WHERE 1/0 = 1) WHERE %s RETURN n".formatted( match, guard );
                    Response response = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    String message = response.getErrorResponse().getMessage();
                    assertTrue( message.contains( "/ by zero" ), message );
                    connection.checkTransactions( 0 );
                }
            }
            for ( String query : List.of(
                    "OPTIONAL MATCH (a:Person WHERE 1/0 = 1)-[:KNOWS]->(b:Person WHERE null) RETURN count(a)",
                    "OPTIONAL MATCH (a:Person WHERE null)-[:KNOWS]->(b:Person WHERE 1/0 = 1) RETURN count(a)" ) ) {
                Response response = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                String message = response.getErrorResponse().getMessage();
                assertTrue( message.contains( "/ by zero" ), message );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void evaluatesWhereOnlyForMatchCandidatesAcrossStores() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String query = """
                    MATCH (a:Person {id:1})
                    WITH last([null,a]) AS n
                    %s (n:%s) WHERE 1/(n.id-1) = 1
                    RETURN count(n) AS total
                    """;
            for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
                int expected = match.equals( "OPTIONAL MATCH" ) ? 1 : 0;
                for ( String pattern : List.of( "MissingPerson", "Person {id:2}" ) ) {
                    Frame frame = connection.cypher( query.formatted( match, pattern ), -1 ).getResult().getFrame();
                    assertEquals( expected, integerValue( singleRow( relationalFrame( frame, true, List.of( "total" ) ) ).getValues( 0 ) ) );
                }

                String withCandidate = query.formatted( match, "Person {id:1}" );
                Response response = expect( connection.send( statementRequest( NAMESPACE, "cypher", withCandidate, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                String message = response.getErrorResponse().getMessage();
                assertTrue( message.contains( "/ by zero" ), message );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void handlesNullAndFalseMatchGuardsAcrossStores() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame first = connection.cypher( "MATCH (n:Person) WHERE null RETURN count(n) AS total", -1 ).getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( first, true, List.of( "total" ) ) ).getValues( 0 ) ) );
            String invalidIndex = "WITH last([null,'x']) AS i MATCH (n:Person) WHERE [null][i] RETURN n";
            Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", invalidIndex, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
            String message = error.getErrorResponse().getMessage();
            assertTrue( message.contains( "A list index or slice bound must be an integer" ), message );

            Frame optional = connection.cypher( """
                    MATCH (n:Person {id:1}) OPTIONAL MATCH (n)-[:KNOWS*]->(b) WHERE null OR false RETURN n.id AS id, b
                    """, -1 ).getResult().getFrame();
            Row unmatched = singleRow( relationalFrame( optional, true, List.of( "id", "b" ) ) );
            assertEquals( 1, integerValue( unmatched.getValues( 0 ) ) );
            assertTrue( unmatched.getValues( 1 ).hasNull() );

            for ( String guard : List.of( "1=0", "false OR false", "guard", "last([null,false])" ) ) {
                String query = """
                        WITH last([null,1]) AS n, last([null,false]) AS guard
                        OPTIONAL MATCH (n:Person)-[:KNOWS*]->(b) WHERE %s RETURN n,b
                        """.formatted( guard );
                Frame frame = connection.cypher( query, -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( frame, true, List.of( "n", "b" ) ) );
                assertEquals( 1, integerValue( row.getValues( 0 ) ) );
                assertTrue( row.getValues( 1 ).hasNull() );
            }
            for ( String guard : List.of( "false", "guard" ) ) {
                String query = """
                        WITH last([null,1]) AS n, last([null,false]) AS guard
                        OPTIONAL MATCH (n:Person WHERE true) WHERE %s RETURN n
                        """.formatted( guard );
                Frame frame = connection.cypher( query, -1 ).getResult().getFrame();
                assertEquals( 1, integerValue( singleRow( relationalFrame( frame, true, List.of( "n" ) ) ).getValues( 0 ) ) );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void writesDynamicallyTypedNodesAndRelationships() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:DynamicWriteSource)-[:DYNAMIC_WRITE {value:1}]->(:DynamicWriteTarget {value:2})
                    """, -1 );
            String match = "MATCH (:DynamicWriteSource)-[rs:DYNAMIC_WRITE*]->(b:DynamicWriteTarget) ";
            Frame updated = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", match + """
                    UNWIND [b,last(rs)] AS target
                    SET target.value = target.value + 10
                    RETURN count(*) AS total
                    """, -1 ).getResult().getFrame();
            assertEquals( 2, integerValue( singleRow( relationalFrame( updated, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame created = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", match + """
                    WITH last([null,b]) AS target
                    CREATE p=(target)-[:DYNAMIC_CREATED]->(:DynamicCreatedTarget)
                    RETURN length(p) AS hops
                    """, -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( created, true, List.of( "hops" ) ) ).getValues( 0 ) ) );

            String invalidCreate = match + "UNWIND [b,1] AS target CREATE (target)-[:DYNAMIC_INVALID]->(:DynamicInvalidTarget)";
            Response error = expect( writer.send( statementRequest( WRITE_RETURN_NAMESPACE, "cypher", invalidCreate, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
            assertTrue( error.getErrorResponse().getMessage().contains( "CREATE requires node bindings for the source and target" ),
                    error.getErrorResponse().getMessage() );
            writer.checkTransactions( 0 );

            for ( String query : List.of(
                    "MATCH ()-[r:DYNAMIC_WRITE]->() SET r:Changed",
                    "MATCH ()-[r:DYNAMIC_WRITE]->() WITH last([null,r]) AS target SET target:Changed",
                    match + "WITH last(rs) AS target SET target:First:Second" ) ) {
                Response invalidLabels = expect( writer.send( statementRequest( WRITE_RETURN_NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( invalidLabels.getErrorResponse().getMessage().contains( "SET label assignment requires a node binding" ),
                        invalidLabels.getErrorResponse().getMessage() );
                writer.checkTransactions( 0 );
            }

            Frame persisted = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (:DynamicWriteSource)-[r:DYNAMIC_WRITE]->(b:DynamicWriteTarget)-[:DYNAMIC_CREATED]->(:DynamicCreatedTarget)
                    RETURN r.value AS relationship, b.value AS node
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( persisted, true, List.of( "relationship", "node" ) ) );
            assertEquals( 11, integerValue( row.getValues( 0 ) ) );
            assertEquals( 12, integerValue( row.getValues( 1 ) ) );
            Frame rejected = reader.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:DynamicInvalidTarget) RETURN count(n) AS total", -1 ).getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( rejected, true, List.of( "total" ) ) ).getValues( 0 ) ) );
            Frame renamed = reader.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH ()-[r:Changed]->() RETURN count(r) AS total", -1 ).getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( renamed, true, List.of( "total" ) ) ).getValues( 0 ) ) );
        }
    }


    @Test
    void rejectsConflictingPatternBindingsBeforeScanningTheGraph() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            List<String> conflicting = List.of(
                    "MATCH (r)-[]-()-[r*]-() RETURN r",
                    "MATCH ()-[]-(r)-[r*]-() RETURN r",
                    "MATCH r=()-[r*]->() RETURN r",
                    "MATCH r=()-[]-(), ()-[r*]-() RETURN r",
                    "MATCH ()-[r]->()-[r*]->() RETURN r",
                    "MATCH (r:NoSuchLabel), ()-[r*]->() RETURN r",
                    "WITH [1] AS r MATCH ()-[r*]->() RETURN r",
                    "MATCH ()-[r]->() MATCH ()-[r*]->() RETURN r" );
            for ( String query : conflicting ) {
                Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "Variable 'r' has" ), query );
                connection.checkTransactions( 0 );
            }

            for ( String duplicate : List.of( "MATCH (a)-[r]->()-[r]->(a) RETURN r", "MATCH ()-[r]->(), ()-[r]->() RETURN r" ) ) {
                Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", duplicate, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "Relationship variable 'r' is repeated in the same MATCH" ), duplicate );
            }
            Frame valid = connection.cypher( "MATCH ()-[r:KNOWS]->() MATCH ()-[r:KNOWS]->() RETURN count(*) AS total", -1 ).getResult().getFrame();
            assertEquals( 2, integerValue( singleRow( relationalFrame( valid, true, List.of( "total" ) ) ).getValues( 0 ) ) );
        }
    }


    @Test
    void returnsUnboundedRelationshipBindingsAsOrderedLists() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            String query = "MATCH (:Person {name:'Alice'})-[hops:KNOWS*%s]->(b:Person) RETURN b.name AS target, hops";
            for ( var range : Map.of(
                    "", Map.of( "Bob", List.of( 2020L ), "Charlie", List.of( 2020L, 2021L ) ),
                    "0..", Map.of( "Alice", List.<Long>of(), "Bob", List.of( 2020L ), "Charlie", List.of( 2020L, 2021L ) ),
                    "2..", Map.of( "Charlie", List.of( 2020L, 2021L ) ) ).entrySet() ) {
                Frame frame = connection.cypher( query.formatted( range.getKey() ), -1 ).getResult().getFrame();
                RelationalFrame page = relationalFrame( frame, true, List.of( "target", "hops" ) );
                var type = page.getColumnMeta( 1 ).getTypeMeta();
                assertEquals( ProtoPolyType.ARRAY, type.getProtoValueType() );
                assertEquals( ProtoPolyType.EDGE, type.getArrayMeta().getElementType().getProtoValueType() );
                assertEquals( range.getValue().size(), page.getRowsCount() );
                Map<String, List<Long>> hops = new HashMap<>();
                for ( Row row : page.getRowsList() ) {
                    String target = row.getValues( 0 ).getString().getString();
                    ProtoValue value = row.getValues( 1 );
                    assertTrue( value.hasList() );
                    hops.put( target, value.getList().getValuesList().stream().map( element -> {
                        assertTrue( element.hasEdge() );
                        ProtoEdge edge = element.getEdge();
                        assertEquals( List.of( "KNOWS" ), edge.getLabelsList() );
                        return integerValue( edge.getPropertiesOrThrow( "since" ) );
                    } ).toList() );
                }
                assertEquals( range.getValue(), hops );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void reusesAnUnboundedRelationshipBindingInTraversalOrder() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String first = "MATCH (:Person {name:'Alice'})-[hops:KNOWS*]->(:Person {name:'Charlie'}) ";
            String projection = " RETURN a.name AS origin, b.name AS target";
            List<String> columns = List.of( "origin", "target" );

            Frame sameOrder = connection.cypher( first + "MATCH (a:Person)-[hops:KNOWS*]->(b:Person)" + projection, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( sameOrder, true, columns ) );
            assertEquals( List.of( "Alice", "Charlie" ), row.getValuesList().stream().map( value -> value.getString().getString() ).toList() );

            Frame reversed = connection.cypher( first + "MATCH (a:Person)<-[hops:KNOWS*]-(b:Person)" + projection, -1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( reversed, true, columns ).getRowsCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void reusesAnAliasedRelationshipListAcrossMatchesWithMultipleRoutes() throws IOException {
        String namespace = "cypherrangealias";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", """
                        CREATE (a:Stop {name:'A'})-[:LINK {hop:1}]->(:Stop {name:'B'})-[:LINK {hop:2}]->(c:Stop {name:'C'}),
                            (a)-[:LINK {hop:3}]->(c)
                        """, -1 );
                Frame frame = connection.execute( namespace, "cypher", """
                        MATCH (a:Stop {name:'A'})-[original:LINK*]->(b:Stop {name:'C'})
                        WITH a, b, original AS rels
                        MATCH (a)-[rels:LINK*]->(b)
                        RETURN b.name AS name, rels
                        """, -1 ).getResult().getFrame();
                RelationalFrame page = relationalFrame( frame, true, List.of( "name", "rels" ) );
                assertEquals( 2, page.getRowsCount() );
                Set<List<Long>> routes = page.getRowsList().stream().map( row -> {
                    assertEquals( "C", row.getValues( 0 ).getString().getString() );
                    ProtoValue relationships = row.getValues( 1 );
                    assertTrue( relationships.hasList() );
                    return relationships.getList().getValuesList().stream()
                            .map( relationship -> integerValue( relationship.getEdge().getPropertiesOrThrow( "hop" ) ) ).toList();
                } ).collect( Collectors.toSet() );
                assertEquals( Set.of( List.of( 1L, 2L ), List.of( 3L ) ), routes );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsVariableLengthRelationshipsInCreate() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String query : List.of(
                    "CREATE (:InvalidRangeCreate)-[:KNOWS*]->(:InvalidRangeCreate)",
                    "MATCH (n:Person {name:'Alice'}) CREATE (n)-[:KNOWS*]->(:InvalidRangeCreate)" ) ) {
                Response response = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( response.getErrorResponse().getMessage().toLowerCase( Locale.ROOT ).contains( "variable-length relationships" ) );
                connection.checkTransactions( 0 );
                Frame absent = connection.cypher( "MATCH (n:InvalidRangeCreate) RETURN n", -1 ).getResult().getFrame();
                assertTrue( graphElements( absent, true ).isEmpty() );
            }
        }
    }


    @Test
    void rejectsInvalidCreatePropertiesAndRollsBackAcrossStores() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            for ( String query : List.of(
                    "CREATE (:CreatePropertyCheck {value:[{num:1}]}) RETURN 1",
                    "CREATE (:CreatePropertyCheck)-[:CREATE_PROPERTY_CHECK {value:[1,null]}]->(:CreatePropertyCheck)",
                    """
                            UNWIND [{rank:0, value:[1]}, {rank:1, value:[{num:1}]}] AS row
                            WITH row.value AS value ORDER BY row.rank
                            CREATE (:CreatePropertyCheck {value:value})
                            """,
                    """
                            CREATE (n:CreatePropertyCheck)
                            WITH n, {num:1} AS value
                            CREATE (n)-[:CREATE_PROPERTY_CHECK {value:value}]->(:CreatePropertyCheck)
                            """ ) ) {
                Response error = expect( writer.send( statementRequest( WRITE_RETURN_NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "InvalidPropertyType" ), error.getErrorResponse().getMessage() );
                writer.checkTransactions( 0 );
                for ( String match : List.of( "MATCH (n:CreatePropertyCheck) RETURN n", "MATCH ()-[r:CREATE_PROPERTY_CHECK]->() RETURN r" ) ) {
                    Frame absent = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", match, -1 ).getResult().getFrame();
                    assertTrue( graphElements( absent, true ).isEmpty(), query );
                }
            }

            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:CreatePropertyCheck {values:[1,2], empty:[], omitted:null})
                    """, -1 );
            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:CreatePropertyCheck)
                    WITH n, [3,4] AS values, [] AS empty, null AS omitted
                    CREATE (n)-[:CREATE_PROPERTY_CHECK {values:values, empty:empty, omitted:omitted}]->(:CreatePropertyTarget)
                    """, -1 );
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:CreatePropertyCheck)-[r:CREATE_PROPERTY_CHECK]->()
                    RETURN n.values AS nodeValues, n.empty AS nodeEmpty, n.omitted AS nodeOmitted,
                        r.values AS edgeValues, r.empty AS edgeEmpty, r.omitted AS edgeOmitted
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( stored, true, List.of( "nodeValues", "nodeEmpty", "nodeOmitted", "edgeValues", "edgeEmpty", "edgeOmitted" ) ) );
            assertEquals( List.of( 1L, 2L ), row.getValues( 0 ).getList().getValuesList().stream().map( CypherTest::integerValue ).toList() );
            assertTrue( row.getValues( 1 ).hasList() );
            assertEquals( 0, row.getValues( 1 ).getList().getValuesCount() );
            assertTrue( row.getValues( 2 ).hasNull() );
            assertEquals( List.of( 3L, 4L ), row.getValues( 3 ).getList().getValuesList().stream().map( CypherTest::integerValue ).toList() );
            assertTrue( row.getValues( 4 ).hasList() );
            assertEquals( 0, row.getValues( 4 ).getList().getValuesCount() );
            assertTrue( row.getValues( 5 ).hasNull() );
            writer.checkTransactions( 0 );
            reader.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsCreateRebindingAndInvalidRelationshipsBeforeAnyWriteAcrossStores() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            Map<String, String> invalid = Map.of(
                    "CREATE (n:CreateBindingCheck)-[:CREATE_BINDING]->(), (n:Redecorated)-[:CREATE_BINDING]->()", "VariableAlreadyBound",
                    "CREATE (n:CreateBindingCheck) CREATE (n:Redecorated)-[:CREATE_BINDING]->()", "VariableAlreadyBound",
                    "CREATE (n:CreateBindingCheck) CREATE (n {bad:[{x:1}]})-[:CREATE_BINDING]->()", "VariableAlreadyBound",
                    "CREATE (n:CreateBindingCheck) CREATE (n {})-[:CREATE_BINDING]->()", "VariableAlreadyBound",
                    "CREATE (n:CreateBindingCheck) CREATE (n)", "VariableAlreadyBound",
                    "CREATE (:CreateBindingCheck)-[:CREATE_BINDING]-()", "RequiresDirectedRelationship",
                    "CREATE (:CreateBindingCheck)-[:CREATE_BINDING|OTHER_BINDING]->()", "NoSingleRelationshipType",
                    "CREATE (:CreateBindingCheck)-->()", "NoSingleRelationshipType" );
            for ( Map.Entry<String, String> scenario : invalid.entrySet() ) {
                Response error = expect( writer.send( statementRequest( WRITE_RETURN_NAMESPACE, "cypher", scenario.getKey(), -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( scenario.getValue() ), error.getErrorResponse().getMessage() );
                writer.checkTransactions( 0 );
            }
            Frame absent = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH (n:CreateBindingCheck) RETURN n", -1 ).getResult().getFrame();
            assertTrue( graphElements( absent, true ).isEmpty() );

            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (n:CreateBindingCheck {score:7}), (n)-[:CREATE_BINDING]->(n)
                    CREATE (n)-[:CREATE_BINDING]->()
                    """, -1 );
            Frame persisted = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:CreateBindingCheck)-[r:CREATE_BINDING]->()
                    RETURN n.score AS score, count(r) AS relationships
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( persisted, true, List.of( "score", "relationships" ) ) );
            assertEquals( 7, integerValue( row.getValues( 0 ) ) );
            assertEquals( 2, integerValue( row.getValues( 1 ) ) );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsInlinePredicatesInCreateBeforeAnyWriteAcrossStores() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            for ( String query : List.of(
                    "CREATE (n:PredicateCreateCheck WHERE false) RETURN n",
                    "CREATE (:PredicateCreateCheck)-[r:PREDICATE_CREATE_CHECK WHERE false]->(:PredicateCreateCheck) RETURN r",
                    "CREATE p=(n:PredicateCreateCheck WHERE false)-[:PREDICATE_CREATE_CHECK]->(:PredicateCreateCheck) RETURN p",
                    """
                            CREATE (:PredicateCreateCheck)
                            UNWIND [false,true] AS passes
                            CREATE (:PredicateCreateCheck)-[r:PREDICATE_CREATE_CHECK WHERE passes]->(:PredicateCreateCheck)
                            """ ) ) {
                Response error = expect( writer.send( statementRequest( WRITE_RETURN_NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "Inline WHERE predicates are not allowed in CREATE patterns" ), error.getErrorResponse().getMessage() );
                writer.checkTransactions( 0 );
                for ( String match : List.of( "MATCH (n:PredicateCreateCheck) RETURN n", "MATCH ()-[r:PREDICATE_CREATE_CHECK]->() RETURN r" ) ) {
                    Frame absent = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", match, -1 ).getResult().getFrame();
                    assertTrue( graphElements( absent, true ).isEmpty(), query );
                }
            }
            reader.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsInvalidSetPropertiesAndRollsBackAcrossStores() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.cypher( "CREATE (:SetRegressionOriginal {marker:1})", 1 );
            for ( String query : List.of(
                    "CREATE (n:SetRegressionUnexpected) SET n.value = [{num:1}]",
                    "CREATE (n:SetRegressionUnexpected) SET n += {value:[{num:1}]}",
                    """
                            MATCH (n:SetRegressionOriginal)
                            UNWIND [{rank:0, value:[1]}, {rank:1, value:[{num:1}]}] AS row
                            WITH n, row.value AS value ORDER BY row.rank
                            SET n.marker = 2, n.value = value
                            """,
                    """
                            MATCH (n:SetRegressionOriginal) SET n.marker = 2
                            WITH n, {value:{num:1}} AS props
                            SET n = props
                            """ ) ) {
                Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "InvalidPropertyType" ), query );
                connection.checkTransactions( 0 );

                Frame unexpected = connection.cypher( "MATCH (n:SetRegressionUnexpected) RETURN count(n) AS total", -1 ).getResult().getFrame();
                assertEquals( 0, integerValue( singleRow( relationalFrame( unexpected, true, List.of( "total" ) ) ).getValues( 0 ) ), query );

                Frame original = connection.cypher( "MATCH (n:SetRegressionOriginal) RETURN n.marker AS marker, n.value AS value", -1 ).getResult().getFrame();
                Row row = singleRow( relationalFrame( original, true, List.of( "marker", "value" ) ) );
                assertEquals( 1, integerValue( row.getValues( 0 ) ), query );
                assertTrue( row.getValues( 1 ).hasNull(), query );
            }

            Frame updated = connection.cypher( """
                    MATCH (n:SetRegressionOriginal)
                    WITH n, {marker:null, value:[1,2]} AS props
                    SET n += props
                    RETURN n.marker AS marker, n.value[1] AS value
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( updated, true, List.of( "marker", "value" ) ) );
            assertTrue( row.getValues( 0 ).hasNull() );
            assertEquals( 2, integerValue( row.getValues( 1 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void emptyMixedResultsStillExposeTheGraphColumn() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( "MATCH (n:Person {name: 'Unknown'}) RETURN n, n.name AS name", 1 ).getResult().getFrame();
            RelationalFrame page = relationalFrame( frame, true, List.of( "n", "name" ) );
            assertEquals( ProtoPolyType.NODE, page.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertTrue( page.getColumnMeta( 1 ).getIsNullable() );
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
    void pureGraphRowsPreserveAliasesAndPagingWhenRequested() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String query = "MATCH (n:Person) RETURN n AS first, n AS second ORDER BY n.name";
            assertEquals( 6, graphElements( connection.cypher( query, -1 ).getResult().getFrame(), true ).size() );
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            assertEquals( "relational", expect( connection.send( Request.newBuilder()
                            .setClientInfoPropertiesRequest( ClientInfoPropertiesRequest.getDefaultInstance() ) ), Response.TypeCase.CLIENT_INFO_PROPERTIES_RESPONSE )
                    .getClientInfoPropertiesResponse().getPropertiesOrThrow( PIClientInfoProperties.CYPHER_RESULT_FORMAT ) );

            StatementResponse statement = connection.cypher( query, 1 );
            Frame frame = statement.getResult().getFrame();
            List<String> names = List.of( "Alice", "Bob", "Charlie" );
            for ( int i = 0; i < names.size(); i++ ) {
                boolean last = i == names.size() - 1;
                RelationalFrame page = relationalFrame( frame, last, List.of( "first", "second" ) );
                assertEquals( List.of( ProtoPolyType.NODE, ProtoPolyType.NODE ), page.getColumnMetaList().stream()
                        .map( column -> column.getTypeMeta().getProtoValueType() ).toList() );
                Row row = singleRow( page );
                ProtoNode first = assertNode( row.getValues( 0 ).getNode(), names.get( i ), i + 1 );
                assertEquals( first, row.getValues( 1 ).getNode() );
                if ( i == 0 ) {
                    expect( setCypherResultFormat( connection, "auto" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
                }
                if ( !last ) {
                    frame = connection.fetch( statement.getStatementId(), 1 );
                }
            }
            connection.checkTransactions( 0 );
            assertEquals( 6, graphElements( connection.cypher( query, -1 ).getResult().getFrame(), true ).size() );
        }
    }


    @Test
    void relationalGraphFormatPreservesPathsAndEmptyColumnMetadata() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            String query = "MATCH p=(:Person {name: 'Alice'})-[:KNOWS*1..3]->(:Person {name: '%s'}) RETURN p AS path";
            RelationalFrame pathFrame = relationalFrame( connection.cypher( query.formatted( "Charlie" ), 1 ).getResult().getFrame(), true, List.of( "path" ) );
            assertEquals( ProtoPolyType.PATH, pathFrame.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            ProtoPath path = singleRow( pathFrame ).getValues( 0 ).getPath();
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), path.getNodesList().stream()
                    .map( node -> node.getPropertiesOrThrow( "name" ).getString().getString() ).toList() );
            assertEquals( 2, path.getEdgesCount() );

            RelationalFrame empty = relationalFrame( connection.cypher( query.formatted( "Unknown" ), 1 ).getResult().getFrame(), true, List.of( "path" ) );
            assertEquals( ProtoPolyType.PATH, empty.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertEquals( 0, empty.getRowsCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void relationalGraphFormatCanCarryAWholeGraph() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            Frame frame = connection.cypher( "MATCH (*) RETURN *", 1 ).getResult().getFrame();
            assertTrue( frame.hasRelationalFrame() );
            assertTrue( frame.getIsLast() );
            RelationalFrame result = frame.getRelationalFrame();
            assertEquals( 1, result.getColumnMetaCount() );
            assertEquals( ProtoPolyType.GRAPH, result.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            ProtoValue graph = singleRow( result ).getValues( 0 );
            assertEquals( ProtoValue.ValueCase.GRAPH, graph.getValueCase() );
            assertEquals( 3, graph.getGraph().getNodesCount() );
            assertEquals( 2, graph.getGraph().getEdgesCount() );
        }
    }


    @Test
    void comparesIndependentlyMatchedGraphValuesByIdentity() throws IOException {
        ProtoValue truth = PolyValueSerializer.serialize( PolyBoolean.TRUE );
        ProtoValue falsity = PolyValueSerializer.serialize( PolyBoolean.FALSE );
        List<String> columns = List.of( "sameNode", "sameEdge", "samePath", "differentNode", "differentEdge", "differentPath" );
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String first = "OPTIONAL MATCH p=(a:Person {name: 'Alice'})-[r:KNOWS]->(:Person {name: 'Bob'})";
            String projection = """
                     RETURN a=b AS sameNode, r=s AS sameEdge, p=q AS samePath,
                            a<>b AS differentNode, r<>s AS differentEdge, p<>q AS differentPath
                    """;
            Frame matched = connection.cypher( first + """
                     OPTIONAL MATCH q=(b:Person {name: 'Alice'})-[s:KNOWS]->(:Person {name: 'Bob'})
                     WHERE a=b AND r=s AND p=q
                    """ + projection, 1 ).getResult().getFrame();
            assertEquals( List.of( truth, truth, truth, falsity, falsity, falsity ),
                    singleRow( relationalFrame( matched, true, columns ) ).getValuesList() );

            Frame different = connection.cypher( first + """
                     OPTIONAL MATCH q=(b:Person {name: 'Bob'})-[s:KNOWS]->(:Person {name: 'Charlie'})
                    """ + projection, 1 ).getResult().getFrame();
            assertEquals( List.of( falsity, falsity, falsity, truth, truth, truth ),
                    singleRow( relationalFrame( different, true, columns ) ).getValuesList() );

            Frame reversed = connection.cypher( first + """
                     OPTIONAL MATCH q=(:Person {name: 'Bob'})<-[s:KNOWS]-(:Person {name: 'Alice'})
                     RETURN r=s AS sameEdge, p=q AS samePath
                    """, 1 ).getResult().getFrame();
            assertEquals( List.of( truth, falsity ), singleRow( relationalFrame( reversed, true, List.of( "sameEdge", "samePath" ) ) ).getValuesList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void comparesUnmatchedOptionalGraphValuesAsNull() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame projected = connection.cypher( """
                    OPTIONAL MATCH p=(n:Missing)-[r:MISSING]->()
                    RETURN n=n AS nodeEq, n<>n AS nodeNe, r=r AS edgeEq, r<>r AS edgeNe, p=p AS pathEq, p<>p AS pathNe
                    """, 1 ).getResult().getFrame();
            RelationalFrame projection = relationalFrame( projected, true, List.of( "nodeEq", "nodeNe", "edgeEq", "edgeNe", "pathEq", "pathNe" ) );
            assertTrue( projection.getColumnMetaList().stream().allMatch( column -> column.getIsNullable() ) );
            assertTrue( singleRow( projection ).getValuesList().stream().allMatch( ProtoValue::hasNull ) );

            Frame filtered = connection.cypher( "OPTIONAL MATCH (n:Missing) WITH n WHERE n=n RETURN n.name AS name", 1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( filtered, true, List.of( "name" ) ).getRowsCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void optionalMatchPagesKeepUnmatchedGraphColumnsNull() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            StatementResponse statement = connection.cypher( "MATCH (a:Person) OPTIONAL MATCH (a)-[r:KNOWS]->(b) "
                    + "RETURN a AS sender, r AS link, b AS recipient ORDER BY a.name", 2 );
            List<String> labels = List.of( "sender", "link", "recipient" );
            RelationalFrame first = relationalFrame( statement.getResult().getFrame(), false, labels );
            assertEquals( 2, first.getRowsCount() );
            assertTrue( first.getColumnMeta( 1 ).getIsNullable() );
            assertTrue( first.getColumnMeta( 2 ).getIsNullable() );
            for ( int i = 0; i < first.getRowsCount(); i++ ) {
                Row row = first.getRows( i );
                ProtoNode sender = row.getValues( 0 ).getNode();
                ProtoNode recipient = row.getValues( 2 ).getNode();
                assertEdge( row.getValues( 1 ).getEdge(), sender, recipient, 2020 + i );
            }
            RelationalFrame last = relationalFrame( connection.fetch( statement.getStatementId(), 2 ), true, labels );
            Row unmatched = singleRow( last );
            assertNode( unmatched.getValues( 0 ).getNode(), "Charlie", 3 );
            assertEquals( ProtoValue.ValueCase.NULL, unmatched.getValues( 1 ).getValueCase() );
            assertEquals( ProtoValue.ValueCase.NULL, unmatched.getValues( 2 ).getValueCase() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void groupsEqualNumbersAndReturnsDistinctCountsAsLongs() throws IOException {
        String namespace = "prismnumericaggregation";
        long firstLarge = 9_007_199_254_740_992L;
        long secondLarge = firstLarge + 1L;
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            try {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
                int insert = prepareNamedCypher( connection, namespace, "CREATE (:NumericGroup {value:$value})" ).getStatementId();
                for ( PolyValue value : List.of( PolyInteger.of( 42 ), PolyDouble.of( 42.0 ),
                        PolyLong.of( firstLarge ), PolyLong.of( secondLarge ), PolyString.of( "42" ), PolyNull.NULL ) ) {
                    expect( executeNamedCypher( connection, insert, Map.of( "value", PolyValueSerializer.serialize( value ) ) ),
                            Response.TypeCase.STATEMENT_RESULT );
                }

                String match = "MATCH (n:NumericGroup) OPTIONAL MATCH (n)-[:MISSING]->(m) ";
                Frame counts = connection.execute( namespace, "cypher", match
                        + "RETURN count(n.value) AS total, count(DISTINCT n.value) AS uniqueValues", -1 ).getResult().getFrame();
                RelationalFrame aggregate = relationalFrame( counts, true, List.of( "total", "uniqueValues" ) );
                assertEquals( ProtoPolyType.BIGINT, aggregate.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
                Row row = singleRow( aggregate );
                assertEquals( 5, row.getValues( 0 ).getLong().getLong() );
                assertTrue( row.getValues( 1 ).hasLong() );
                assertEquals( 4, row.getValues( 1 ).getLong().getLong() );

                for ( boolean composite : List.of( false, true ) ) {
                    String groupKeys = composite ? "n.value AS value, 1 AS bucket" : "n.value AS value";
                    List<String> labels = composite ? List.of( "value", "bucket", "rows" ) : List.of( "value", "rows" );
                    Frame result = connection.execute( namespace, "cypher", match + "RETURN " + groupKeys + ", count(*) AS rows", -1 )
                            .getResult().getFrame();
                    RelationalFrame grouped = relationalFrame( result, true, labels );
                    assertEquals( 5, grouped.getRowsCount() );

                    Map<String, Long> rowsByValue = new HashMap<>();
                    for ( Row group : grouped.getRowsList() ) {
                        ProtoValue key = group.getValues( 0 );
                        String name = switch ( key.getValueCase() ) {
                            case NULL -> "null";
                            case STRING -> "text " + key.getString().getString();
                            case DOUBLE -> {
                                assertEquals( 42.0, key.getDouble().getDouble() );
                                yield "number 42";
                            }
                            default -> "number " + integerValue( key );
                        };
                        assertNull( rowsByValue.put( name, group.getValues( labels.size() - 1 ).getLong().getLong() ) );
                    }
                    assertEquals( Map.of( "null", 1L, "text 42", 1L, "number 42", 2L,
                            "number " + firstLarge, 1L, "number " + secondLarge, 1L ), rowsByValue );
                }
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + namespace, 1 );
            }
        }
    }


    @Test
    void keepsDistinctCountsAsLongsWhenGroupingAgain() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String group = "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) WITH n.name AS person, count(DISTINCT m) AS neighbors ";
            ProtoValue zero = PolyValueSerializer.serialize( PolyLong.of( 0 ) );
            ProtoValue one = PolyValueSerializer.serialize( PolyLong.of( 1 ) );
            ProtoValue two = PolyValueSerializer.serialize( PolyLong.of( 2 ) );

            Frame single = connection.cypher( group + "RETURN neighbors, count(*) AS people ORDER BY neighbors", -1 ).getResult().getFrame();
            RelationalFrame singleKey = relationalFrame( single, true, List.of( "neighbors", "people" ) );
            assertEquals( ProtoPolyType.BIGINT, singleKey.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertEquals( List.of( List.of( zero, one ), List.of( one, two ) ),
                    singleKey.getRowsList().stream().map( Row::getValuesList ).toList() );

            Frame composite = connection.cypher( group + "RETURN person, neighbors, count(*) AS people ORDER BY person", -1 ).getResult().getFrame();
            RelationalFrame compositeKey = relationalFrame( composite, true, List.of( "person", "neighbors", "people" ) );
            assertEquals( ProtoPolyType.BIGINT, compositeKey.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
            assertEquals( List.of( List.of( stringParameter( "Alice" ), one, one ), List.of( stringParameter( "Bob" ), one, one ),
                    List.of( stringParameter( "Charlie" ), zero, one ) ), compositeKey.getRowsList().stream().map( Row::getValuesList ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesALargeNamedLongInACompositeGroupingKey() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m)
                    RETURN n.name AS person, $value AS supplied, count(m) AS neighbors ORDER BY person
                    """ );
            ProtoValue large = PolyValueSerializer.serialize( PolyLong.of( 9_007_199_254_740_993L ) );
            Response response = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", large ) ), Response.TypeCase.STATEMENT_RESULT );
            RelationalFrame grouped = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "person", "supplied", "neighbors" ) );
            assertEquals( ProtoPolyType.BIGINT, grouped.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), grouped.getRowsList().stream().map( row -> row.getValues( 0 ).getString().getString() ).toList() );
            assertEquals( List.of( large, large, large ), grouped.getRowsList().stream().map( row -> row.getValues( 1 ) ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void groupsByBoundNullAndKeepsNativeProjectionAliases() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            ProtoValue number = PolyValueSerializer.serialize( PolyLong.of( 9_007_199_254_740_993L ) );
            ProtoValue nullValue = PolyValueSerializer.serialize( PolyNull.NULL );
            ProtoValue one = PolyValueSerializer.serialize( PolyLong.of( 1 ) );
            ProtoValue three = PolyValueSerializer.serialize( PolyLong.of( 3 ) );
            PreparedStatementSignature single = prepareNamedCypher( connection, "MATCH (n:Person) RETURN $value AS supplied, count(n) AS hits" );
            PreparedStatementSignature composite = prepareNamedCypher( connection, """
                    MATCH (n:Person)
                    RETURN n.name AS name, $value AS supplied, count(n) AS hits ORDER BY name
                    """ );
            for ( ProtoValue value : List.of( number, nullValue, number ) ) {
                Map<String, ProtoValue> bindings = Map.of( "value", value );
                Response singleResponse = expect( executeNamedCypher( connection, single.getStatementId(), bindings ), Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame singleFrame = relationalFrame( singleResponse.getStatementResult().getFrame(), true, List.of( "supplied", "hits" ) );
                assertEquals( List.of( value, three ), singleRow( singleFrame ).getValuesList() );

                Response compositeResponse = expect( executeNamedCypher( connection, composite.getStatementId(), bindings ), Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame compositeFrame = relationalFrame( compositeResponse.getStatementResult().getFrame(), true, List.of( "name", "supplied", "hits" ) );
                assertEquals( List.of( List.of( stringParameter( "Alice" ), value, one ), List.of( stringParameter( "Bob" ), value, one ),
                        List.of( stringParameter( "Charlie" ), value, one ) ), compositeFrame.getRowsList().stream().map( Row::getValuesList ).toList() );
            }

            Frame aliasedInput = connection.cypher( """
                    MATCH (n:Person) WITH n.name AS name
                    RETURN name, count(name) AS hits ORDER BY name
                    """, -1 ).getResult().getFrame();
            assertEquals( List.of( List.of( stringParameter( "Alice" ), one ), List.of( stringParameter( "Bob" ), one ),
                    List.of( stringParameter( "Charlie" ), one ) ),
                    relationalFrame( aliasedInput, true, List.of( "name", "hits" ) ).getRowsList().stream().map( Row::getValuesList ).toList() );

            Frame empty = connection.cypher( "MATCH (n:MissingPerson) RETURN count(n) AS hits", -1 ).getResult().getFrame();
            assertEquals( 0L, singleRow( relationalFrame( empty, true, List.of( "hits" ) ) ).getValues( 0 ).getLong().getLong() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void keepsProjectionAndGroupingScopesAfterAnIntermediateSort() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String withSortedLimit = "MATCH (n:Person) WITH n.name AS name ORDER BY name LIMIT 2 ";
            Frame reordered = connection.cypher( withSortedLimit + "RETURN name ORDER BY name DESC", -1 ).getResult().getFrame();
            assertEquals( List.of( "Bob", "Alice" ), projectedNames( relationalFrame( reordered, true, List.of( "name" ) ) ) );

            Frame grouped = connection.cypher( withSortedLimit + """
                    WITH name AS renamed WITH renamed AS key
                    RETURN key AS result, count(*) AS hits ORDER BY result DESC
                    """, -1 ).getResult().getFrame();
            ProtoValue one = PolyValueSerializer.serialize( PolyLong.of( 1 ) );
            assertEquals( List.of( List.of( stringParameter( "Bob" ), one ), List.of( stringParameter( "Alice" ), one ) ),
                    relationalFrame( grouped, true, List.of( "result", "hits" ) ).getRowsList().stream().map( Row::getValuesList ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void returnsNullExtremaForEmptyAndAllNullGroups() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            List<String> extrema = List.of( "minimum", "maximum" );
            Frame group = connection.cypher( """
                    MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->()
                    RETURN n.name AS name, min(r.since) AS minimum, max(r.since) AS maximum ORDER BY name
                    """, -1 ).getResult().getFrame();
            RelationalFrame grouped = relationalFrame( group, true, List.of( "name", "minimum", "maximum" ) );
            assertEquals( 3, grouped.getRowsCount() );
            assertEquals( 2020, integerValue( grouped.getRows( 0 ).getValues( 1 ) ) );
            assertEquals( 2021, integerValue( grouped.getRows( 1 ).getValues( 2 ) ) );
            assertEquals( "Charlie", grouped.getRows( 2 ).getValues( 0 ).getString().getString() );
            assertTrue( grouped.getRows( 2 ).getValues( 1 ).hasNull() );
            assertTrue( grouped.getRows( 2 ).getValues( 2 ).hasNull() );

            Frame empty = connection.cypher( """
                    MATCH (n:MissingPerson) RETURN min(n.id) AS minimum, max(n.id) AS maximum
                    """, -1 ).getResult().getFrame();
            assertTrue( singleRow( relationalFrame( empty, true, extrema ) ).getValuesList().stream().allMatch( ProtoValue::hasNull ) );

            PreparedStatementSignature createSample = prepareNamedCypher( connection, WRITE_RETURN_NAMESPACE,
                    "CREATE (:PreciseExtremum {value:$value})" );
            for ( long value : List.of( 9_007_199_254_740_993L, 9_007_199_254_740_992L ) ) {
                expect( executeNamedCypher( connection, createSample.getStatementId(), Map.of( "value", PolyValueSerializer.serialize( PolyLong.of( value ) ) ) ),
                        Response.TypeCase.STATEMENT_RESULT );
            }
            Frame precise = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:PreciseExtremum) RETURN min(n.value) AS minimum, max(n.value) AS maximum
                    """, -1 ).getResult().getFrame();
            Row exact = singleRow( relationalFrame( precise, true, extrema ) );
            assertEquals( 9_007_199_254_740_992L, exact.getValues( 0 ).getLong().getLong() );
            assertEquals( 9_007_199_254_740_993L, exact.getValues( 1 ).getLong().getLong() );

            PreparedStatementSignature signature = prepareNamedCypher( connection, """
                    MATCH (n:Person) WITH $value AS sample RETURN min(sample) AS minimum, max(sample) AS maximum
                    """ );
            ProtoValue number = PolyValueSerializer.serialize( PolyLong.of( 9_007_199_254_740_993L ) );
            ProtoValue nullValue = PolyValueSerializer.serialize( PolyNull.NULL );
            for ( ProtoValue value : List.of( nullValue, number, nullValue ) ) {
                Response response = expect( executeNamedCypher( connection, signature.getStatementId(), Map.of( "value", value ) ), Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame result = relationalFrame( response.getStatementResult().getFrame(), true, extrema );
                assertEquals( List.of( value, value ), singleRow( result ).getValuesList() );
                if ( value.hasLong() ) {
                    assertEquals( ProtoPolyType.BIGINT, result.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
                    assertEquals( ProtoPolyType.BIGINT, result.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
                }
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void preservesNamedIntegralValuesInListsAndUnwind() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature projection = prepareNamedCypher( connection, "RETURN [$value] AS supplied" );
            PreparedStatementSignature unwind = prepareNamedCypher( connection, "UNWIND [$value] AS item RETURN item AS supplied" );
            for ( PolyValue number : List.of( PolyInteger.of( 42 ), PolyLong.of( 9_007_199_254_740_993L ) ) ) {
                ProtoValue value = PolyValueSerializer.serialize( number );
                Map<String, ProtoValue> bindings = Map.of( "value", value );

                Response projected = expect( executeNamedCypher( connection, projection.getStatementId(), bindings ), Response.TypeCase.STATEMENT_RESULT );
                Row list = singleRow( relationalFrame( projected.getStatementResult().getFrame(), true, List.of( "supplied" ) ) );
                assertEquals( List.of( value ), list.getValues( 0 ).getList().getValuesList() );

                Response unwound = expect( executeNamedCypher( connection, unwind.getStatementId(), bindings ), Response.TypeCase.STATEMENT_RESULT );
                Row item = singleRow( relationalFrame( unwound.getStatementResult().getFrame(), true, List.of( "supplied" ) ) );
                assertEquals( value, item.getValues( 0 ) );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void groupsEqualNumericListsAndOrdersByTheOriginalGroupingExpression() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:NumericListGroup {bucket:'a', value:[42]}),
                           (:NumericListGroup {bucket:'a', value:[42.0]}),
                           (:NumericListGroup {bucket:'b', value:[43]})
                    """, -1 );
            for ( String order : List.of( "bucket DESC", "n.bucket DESC" ) ) {
                Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                        MATCH (n:NumericListGroup)
                        RETURN n.bucket AS bucket, n.value AS key, count(*) AS hits ORDER BY
                        """ + order, -1 ).getResult().getFrame();
                RelationalFrame frame = relationalFrame( result, true, List.of( "bucket", "key", "hits" ) );
                assertEquals( List.of( "b", "a" ), projectedNames( frame ) );
                assertEquals( List.of( 1L, 2L ), frame.getRowsList().stream().map( row -> row.getValues( 2 ).getLong().getLong() ).toList() );
                assertTrue( frame.getRowsList().stream().allMatch( row -> row.getValues( 1 ).getList().getValuesCount() == 1 ) );
            }
            Frame distinct = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:NumericListGroup) RETURN count(DISTINCT n.value) AS count
                    """, -1 ).getResult().getFrame();
            RelationalFrame frame = relationalFrame( distinct, true, List.of( "count" ) );
            assertEquals( ProtoPolyType.BIGINT, frame.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertEquals( 2L, singleRow( frame ).getValues( 0 ).getLong().getLong() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void sortsStoredFloatAndDoubleValuesNumerically() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int insert = prepareNamedCypher( connection, WRITE_RETURN_NAMESPACE,
                    "CREATE (:MixedFloatSort {name:$name, value:$value})" ).getStatementId();
            Map<String, PolyValue> values = Map.of( "aDouble", PolyDouble.of( 2.5 ), "bFloat", PolyFloat.of( 0.5F ) );
            for ( Map.Entry<String, PolyValue> row : values.entrySet() ) {
                Map<String, ProtoValue> bindings = Map.of( "name", stringParameter( row.getKey() ), "value", PolyValueSerializer.serialize( row.getValue() ) );
                expect( executeNamedCypher( connection, insert, bindings ),
                        Response.TypeCase.STATEMENT_RESULT );
            }

            String sortedInput = """
                    MATCH (n:MixedFloatSort) WITH n.name AS name, n.value AS value ORDER BY name LIMIT 2
                    RETURN name, value ORDER BY
                    """;
            Frame ascending = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", sortedInput + "value", -1 ).getResult().getFrame();
            Frame descending = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", sortedInput + "value DESC, name", -1 ).getResult().getFrame();
            assertEquals( List.of( "bFloat", "aDouble" ), projectedNames( relationalFrame( ascending, true, List.of( "name", "value" ) ) ) );
            assertEquals( List.of( "aDouble", "bFloat" ), projectedNames( relationalFrame( descending, true, List.of( "name", "value" ) ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void sumsAndAveragesGraphValuesWithCypherEmptySetSemantics() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame optional = connection.cypher( """
                    MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->()
                    RETURN n.name AS name, sum(r.since) AS total, avg(r.since) AS average ORDER BY name
                    """, -1 ).getResult().getFrame();
            RelationalFrame groups = relationalFrame( optional, true, List.of( "name", "total", "average" ) );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), projectedNames( groups ) );
            assertEquals( ProtoPolyType.DOUBLE, groups.getColumnMeta( 2 ).getTypeMeta().getProtoValueType() );
            assertEquals( List.of( 2020L, 2021L, 0L ), groups.getRowsList().stream().map( row -> row.getValues( 1 ).getLong().getLong() ).toList() );
            assertTrue( groups.getRowsList().stream().allMatch( row -> row.getValues( 1 ).hasLong() ) );
            assertEquals( PolyValueSerializer.serialize( PolyDouble.of( 2020.0 ) ), groups.getRows( 0 ).getValues( 2 ) );
            assertEquals( PolyValueSerializer.serialize( PolyDouble.of( 2021.0 ) ), groups.getRows( 1 ).getValues( 2 ) );
            assertTrue( groups.getRows( 2 ).getValues( 2 ).hasNull() );

            List<String> labels = List.of( "total", "average" );
            Frame empty = connection.cypher( "MATCH (n:MissingSumInput) RETURN sum(n.value) AS total, avg(n.value) AS average", -1 ).getResult().getFrame();
            Row noInput = singleRow( relationalFrame( empty, true, labels ) );
            assertEquals( PolyValueSerializer.serialize( PolyLong.of( 0 ) ), noInput.getValues( 0 ) );
            assertTrue( noInput.getValues( 1 ).hasNull() );

            int aggregate = prepareNamedCypher( connection, "MATCH (n:Person) RETURN sum($value) AS total, avg($value) AS average" ).getStatementId();
            long precise = 9_007_199_254_740_993L;
            for ( PolyValue amount : List.of( PolyLong.of( precise ), PolyDouble.of( 2.5 ) ) ) {
                boolean integral = amount.isLong();
                Response response = expect( executeNamedCypher( connection, aggregate, Map.of( "value", PolyValueSerializer.serialize( amount ) ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame frame = relationalFrame( response.getStatementResult().getFrame(), true, labels );
                assertEquals( integral ? ProtoPolyType.BIGINT : ProtoPolyType.DOUBLE, frame.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
                assertEquals( ProtoPolyType.DOUBLE, frame.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
                assertEquals( List.of( PolyValueSerializer.serialize( integral ? PolyLong.of( 3L * precise ) : PolyDouble.of( 7.5 ) ),
                                PolyValueSerializer.serialize( PolyDouble.of( integral ? (double) precise : 2.5 ) ) ),
                        singleRow( frame ).getValuesList() );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void ordersTextBelowNumbersInGraphExtrema() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:MixedExtremaOrder {value:'text'}), (:MixedExtremaOrder {value:42}),
                           (:MixedExtremaOrder {value:-7}), (:MixedExtremaOrder {value:'aaa'})
                    """, -1 );
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:MixedExtremaOrder) RETURN min(n.value) AS minimum, max(n.value) AS maximum
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( result, true, List.of( "minimum", "maximum" ) ) );
            assertEquals( stringParameter( "aaa" ), row.getValues( 0 ) );
            assertEquals( 42L, integerValue( row.getValues( 1 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsUnsupportedCypherFormatsAndKeepsGraphDefaults() throws IOException {
        try ( PrismConnection current = new PrismConnection( true, 3 ); PrismConnection older = new PrismConnection( true ) ) {
            Response invalid = expect( setCypherResultFormat( current, "graph-only" ), Response.TypeCase.ERROR_RESPONSE );
            assertTrue( invalid.getErrorResponse().getMessage().contains( "CypherResultFormat" ) );
            Response unsupported = expect( setCypherResultFormat( older, "relational" ), Response.TypeCase.ERROR_RESPONSE );
            assertTrue( unsupported.getErrorResponse().getMessage().contains( "2.3" ) );
            for ( PrismConnection connection : List.of( current, older ) ) {
                assertEquals( 3, graphElements( connection.cypher( "MATCH (n:Person) RETURN n", -1 ).getResult().getFrame(), true ).size() );
            }
        }
    }


    @Test
    void emptyMatchPreventsWritesAfterLiteralUnwind() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String pattern : List.of( "(:UnwindMissing)", "(:UnwindMissing)-[:UNWIND_PATH_EDGE*]->(:UnwindMissing)" ) ) {
                Frame frame = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                        MATCH %s
                        UNWIND [1,2] AS i
                        CREATE (:UnwindUnexpected {i:i})
                        RETURN i
                        """.formatted( pattern ), -1 ).getResult().getFrame();

                assertEquals( 0, relationalFrame( frame, true, List.of( "i" ) ).getRowsCount(), pattern );
                connection.checkTransactions( 0 );
            }
        }
        try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:UnwindUnexpected) RETURN count(n) AS total", -1 ).getResult().getFrame();
            assertEquals( 0, integerValue( singleRow( relationalFrame( stored, true, List.of( "total" ) ) ).getValues( 0 ) ) );
            reader.checkTransactions( 0 );
        }
    }


    @Test
    void detachDeletesNamedPathsAndTheirIncidentRelationships() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:NamedDeleteNode:NamedDeleteStart)-[:NAMED_DELETE_EDGE]->
                           (middle:NamedDeleteNode)-[:NAMED_DELETE_EDGE]->(:NamedDeleteNode:NamedDeleteEnd),
                           (middle)-[:NAMED_DELETE_INCIDENT]->(:NamedDeleteRetained)
                    """, -1 );
            Frame deleted = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(:NamedDeleteStart)-[:NAMED_DELETE_EDGE*]->(:NamedDeleteEnd)
                    DETACH DELETE p RETURN count(*) AS total
                    """, -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( deleted, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            for ( String pattern : List.of( "(n:NamedDeleteNode)", "()-[n:NAMED_DELETE_EDGE]->()", "()-[n:NAMED_DELETE_INCIDENT]->()" ) ) {
                Frame remaining = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                        "MATCH " + pattern + " RETURN count(n) AS total", -1 ).getResult().getFrame();
                assertEquals( 0, integerValue( singleRow( relationalFrame( remaining, true, List.of( "total" ) ) ).getValues( 0 ) ), pattern );
            }
            Frame retained = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:NamedDeleteRetained) RETURN count(n) AS total", -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( retained, true, List.of( "total" ) ) ).getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void detachDeleteOfOptionalNullPathPreservesTheInputRow() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (:NullableDeleteKept)", -1 );
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:NullableDeleteKept)
                    OPTIONAL MATCH p=(a)-[:NULLABLE_DELETE_EDGE*]->()
                    DETACH DELETE p
                    RETURN count(*) AS total
                    """, -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( result, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            Frame retained = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:NullableDeleteKept) RETURN count(n) AS total", -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( retained, true, List.of( "total" ) ) ).getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void validatesEveryCypherBatchMemberBeforeExecutingWrites() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String firstWrite = "CREATE (:BatchRejected {reason: 'preceding write'})";
            for ( String rowProducing : List.of( "MATCH (n:Person) RETURN n", "CREATE (n:BatchRejected {reason: 'returning write'}) RETURN n",
                    "MERGE (n:BatchRejected {reason: 'returning merge'}) RETURN n",
                    "CREATE (n:BatchRejected {reason: 'unfinished write'}) WITH n" ) ) {
                Response result = expect( cypherBatch( connection, NAMESPACE, firstWrite, rowProducing ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( result.getErrorResponse().getMessage().contains( "without returning rows" ) );
                connection.checkTransactions( 0 );
                assertTrue( graphElements( connection.cypher( "MATCH (n:BatchRejected) RETURN n", -1 ).getResult().getFrame(), true ).isEmpty() );
            }
        }
    }


    @Test
    void rollsBackAutocommitBatchWhenALaterCypherMemberCannotBePlanned() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String firstWrite = "CREATE (:BatchMustRollBack)";
            for ( String unsupported : List.of( "MATCH (n:Person) SET missing.name = 'unplanned'", "MERGE (a)-[:BAD*2]->(b)" ) ) {
                expect( cypherBatch( connection, NAMESPACE, firstWrite, unsupported ), Response.TypeCase.ERROR_RESPONSE );
                connection.checkTransactions( 0 );
                try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
                    Frame stored = reader.cypher( "MATCH (n:BatchMustRollBack) RETURN n", -1 ).getResult().getFrame();
                    assertTrue( graphElements( stored, true ).isEmpty(), unsupported );
                    assertEquals( 3, graphElements( reader.cypher( "MATCH (n:Person) RETURN n", -1 ).getResult().getFrame(), true ).size() );
                }
            }
            expect( cypherBatch( connection, NAMESPACE, "MATCH (n:BatchMustRollBack) DELETE n" ), Response.TypeCase.STATEMENT_BATCH_RESPONSE );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsMixedBatchQueriesAndImplicitCommitsBeforeAnyWrite() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( Map.Entry<String, String> invalid : Map.of(
                    "SELECT 1", "without returning rows",
                    "CREATE TABLE mixedbatchrejected (id INTEGER NOT NULL, PRIMARY KEY (id))", "DDL must execute separately" ).entrySet() ) {
                Response response = expect( mixedBatch( connection, "CREATE (:MixedBatchRejected)", Catalog.DEFAULT_NAMESPACE_NAME, "sql", invalid.getKey() ),
                        Response.TypeCase.ERROR_RESPONSE );
                assertTrue( response.getErrorResponse().getMessage().contains( invalid.getValue() ), response.getErrorResponse().getMessage() );
                connection.checkTransactions( 0 );
                Frame stored = connection.cypher( "MATCH (n:MixedBatchRejected) RETURN n", -1 ).getResult().getFrame();
                assertTrue( graphElements( stored, true ).isEmpty() );
            }
        }
    }


    @Test
    void commitsCypherAndSqlUpdatesFromTheSameBatch() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE TABLE mixedbatchwritten (id INTEGER NOT NULL, PRIMARY KEY (id))", 1 );
            try {
                Response response = expect( mixedBatch( connection, "CREATE (:MixedBatchWritten)", Catalog.DEFAULT_NAMESPACE_NAME, "sql", "INSERT INTO mixedbatchwritten VALUES (7)" ),
                        Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                assertEquals( 2, response.getStatementBatchResponse().getScalarsCount() );
                assertEquals( 1, response.getStatementBatchResponse().getScalars( 1 ) );
                connection.checkTransactions( 0 );
                try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
                    Frame graph = reader.cypher( "MATCH (n:MixedBatchWritten) RETURN n", -1 ).getResult().getFrame();
                    assertEquals( 1, graphElements( graph, true ).size() );
                    Frame sql = reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "SELECT id AS id FROM mixedbatchwritten", -1 ).getResult().getFrame();
                    assertEquals( 7, integerValue( singleRow( relationalFrame( sql, true, List.of( "id" ) ) ).getValues( 0 ) ) );
                }
            } finally {
                connection.cypher( "MATCH (n:MixedBatchWritten) DELETE n", -1 );
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP TABLE mixedbatchwritten", 1 );
            }
        }
    }


    @Test
    void returnsTheDocumentUpdateCountFromAMixedBatch() throws IOException {
        String namespace = "mixedbatchdocuments";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE DOCUMENT NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "mongo", "db.createCollection(\"entries\")", 1 );
                Response response = expect( mixedBatch( connection, "CREATE (:MixedBatchWithDocument)", namespace, "mongo", "db.entries.insertOne({name:'written'})" ),
                        Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                assertEquals( 2, response.getStatementBatchResponse().getScalarsCount() );
                assertEquals( 1, response.getStatementBatchResponse().getScalars( 1 ) );
                connection.checkTransactions( 0 );
                Frame frame = connection.execute( namespace, "mongo", "db.entries.find({})", -1 ).getResult().getFrame();
                assertEquals( 1, frame.getDocumentFrame().getDocumentsCount() );
                assertEquals( "written", frame.getDocumentFrame().getDocuments( 0 ).getEntriesOrThrow( "name" ).getString().getString() );
            } finally {
                connection.cypher( "MATCH (n:MixedBatchWithDocument) DELETE n", -1 );
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    private static Response mixedBatch( PrismConnection connection, String cypher, String namespace, String language, String query ) throws IOException {
        return connection.send( Request.newBuilder().setExecuteUnparameterizedStatementBatchRequest( ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                .addStatements( statementRequest( NAMESPACE, "cypher", cypher, 1 ).getExecuteUnparameterizedStatementRequest() )
                .addStatements( statementRequest( namespace, language, query, 1 ).getExecuteUnparameterizedStatementRequest() ) ) );
    }


    @Test
    void manualCypherBatchKeepsItsWritesInTheConnectionTransaction() throws IOException {
        try ( PrismConnection connection = new PrismConnection( false, 3 ) ) {
            expect( cypherBatch( connection, NAMESPACE, "CREATE (:ManualBatch {name: 'created'})",
                    "MATCH (n:ManualBatch) SET n.name = 'updated'" ), Response.TypeCase.STATEMENT_BATCH_RESPONSE );
            Frame inTransaction = connection.cypher( "MATCH (n:ManualBatch) RETURN n.name AS name", -1 ).getResult().getFrame();
            assertEquals( "updated", singleRow( relationalFrame( inTransaction, true, List.of( "name" ) ) ).getValues( 0 ).getString().getString() );
            expect( connection.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) ), Response.TypeCase.ROLLBACK_RESPONSE );
            connection.checkTransactions( 0 );
        }
        try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
            Frame stored = reader.cypher( "MATCH (n:ManualBatch) RETURN n", -1 ).getResult().getFrame();
            assertTrue( graphElements( stored, true ).isEmpty() );
        }
    }


    @Test
    void canUpdateACommittedRowAfterRollingBackItsDeletion() throws IOException {
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
            reader.execute( namespace, "sql", "CREATE TABLE batchdeletedrow (id INTEGER NOT NULL, label VARCHAR(20), PRIMARY KEY (id))", 1 );
            try {
                reader.execute( namespace, "sql", "INSERT INTO batchdeletedrow VALUES (1, 'original')", 1 );
                try ( PrismConnection writer = new PrismConnection( false, 3 ) ) {
                    assertEquals( 1, writer.execute( namespace, "sql", "DELETE FROM batchdeletedrow WHERE id = 1", 1 ).getResult().getScalar() );
                    expect( writer.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) ), Response.TypeCase.ROLLBACK_RESPONSE );
                }
                reader.execute( namespace, "sql", "UPDATE batchdeletedrow SET label = 'updated' WHERE id = 1", 1 );
                Frame result = reader.execute( namespace, "sql", "SELECT label AS label FROM batchdeletedrow", -1 ).getResult().getFrame();
                assertEquals( "updated", singleRow( relationalFrame( result, true, List.of( "label" ) ) ).getValues( 0 ).getString().getString() );
            } finally {
                reader.execute( namespace, "sql", "DROP TABLE batchdeletedrow", 1 );
            }
        }
    }


    @Test
    void rollsBackAMixedBatchThatFailsDuringExecution() throws IOException {
        try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
            // Exercise a JDBC execution error even when the tested graph adapter
            // uses a different execution backend for SQL updates.
            reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", """
                    ALTER ADAPTERS ADD batch_error_store USING 'Hsqldb' AS 'Store'
                    WITH '{maxConnections:"25",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'
                    """, 1 );
            try {
                reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", """
                        CREATE TABLE mixedbatchdenominator (id INTEGER NOT NULL, denominator INTEGER NOT NULL, PRIMARY KEY (id)) ON STORE batch_error_store
                        """, 1 );
                try {
                    reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "INSERT INTO mixedbatchdenominator VALUES (1, 0)", 1 );
                    for ( boolean autocommit : List.of( true, false ) ) {
                        try ( PrismConnection writer = new PrismConnection( autocommit, 3 ) ) {
                            Response result = expect( mixedBatch( writer, "CREATE (:MixedBatchMustRollBack)", Catalog.DEFAULT_NAMESPACE_NAME, "sql",
                                    "UPDATE mixedbatchdenominator SET denominator = 7 / denominator" ), Response.TypeCase.ERROR_RESPONSE );
                            assertTrue( result.getErrorResponse().getMessage().startsWith( "While executing SQL" ), result.getErrorResponse().getMessage() );
                            if ( !autocommit ) {
                                expect( writer.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) ), Response.TypeCase.COMMIT_RESPONSE );
                            }
                            writer.checkTransactions( 0 );
                            Frame graph = reader.cypher( "MATCH (n:MixedBatchMustRollBack) RETURN n", -1 ).getResult().getFrame();
                            assertTrue( graphElements( graph, true ).isEmpty() );
                        }
                    }
                } finally {
                    reader.cypher( "MATCH (n:MixedBatchMustRollBack) DELETE n", -1 );
                    reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP TABLE mixedbatchdenominator", 1 );
                }
            } finally {
                reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "ALTER ADAPTERS DROP batch_error_store", 1 );
            }
        }
    }


    @Test
    void rollsBackMixedBatchWhenFileStoreRejectsTheCommit() throws IOException {
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        String marker = "BatchCommitMarker";
        try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
            reader.execute( namespace, "sql", "ALTER ADAPTERS ADD batch_commit_file USING 'File' AS 'Store' WITH '{}'", 1 );
            try {
                reader.execute( namespace, "sql", """
                        CREATE TABLE batch_commit_keys (id INTEGER NOT NULL, label VARCHAR(20), PRIMARY KEY (id)) ON STORE batch_commit_file
                        """, 1 );
                try {
                    reader.execute( namespace, "sql", "INSERT INTO batch_commit_keys VALUES (1, 'original')", 1 );
                    for ( boolean autocommit : List.of( true, false ) ) {
                        try ( PrismConnection writer = new PrismConnection( autocommit, 3 ) ) {
                            Response response = mixedBatch( writer, "CREATE (:" + marker + ")", namespace, "sql",
                                    "INSERT INTO batch_commit_keys VALUES (2, 'transient'), (1, 'duplicate')" );
                            if ( !autocommit ) {
                                expect( response, Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                                response = writer.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
                            }
                            expect( response, Response.TypeCase.ERROR_RESPONSE );
                            assertTrue( response.getErrorResponse().getMessage().contains( "same primary key" ), response.getErrorResponse().getMessage() );
                            writer.checkTransactions( 0 );

                            Frame stored = reader.cypher( "MATCH (n:" + marker + ") RETURN n", -1 ).getResult().getFrame();
                            assertTrue( graphElements( stored, true ).isEmpty() );
                            Frame rows = reader.execute( namespace, "sql", "SELECT id AS id, label AS label FROM batch_commit_keys", -1 ).getResult().getFrame();
                            Row row = singleRow( relationalFrame( rows, true, List.of( "id", "label" ) ) );
                            assertEquals( 1, integerValue( row.getValues( 0 ) ) );
                            assertEquals( "original", row.getValues( 1 ).getString().getString() );
                        }
                    }
                } finally {
                    reader.cypher( "MATCH (n:" + marker + ") DELETE n", -1 );
                    reader.execute( namespace, "sql", "DROP TABLE batch_commit_keys", 1 );
                }
            } finally {
                reader.execute( namespace, "sql", "ALTER ADAPTERS DROP batch_commit_file", 1 );
            }
        }
    }


    @Test
    void graphPlanCachesDistinguishCollidingBoundValues() throws IOException {
        String namespace = "cypherparametercachetest";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", "CREATE (:CacheMember {name: 'Aa'}), (:CacheMember {name: 'BB'})", 1 );
                for ( String query : List.of( "RETURN $value AS name",
                        "MATCH (n:CacheMember) WHERE n.name = $value RETURN n.name AS name",
                        "OPTIONAL MATCH (n:CacheMember) WHERE n.name = $value RETURN n.name AS name" ) ) {
                    int statementId = prepareNamedCypher( connection, namespace, query ).getStatementId();
                    // Aa and BB collide in String.hashCode(), so the cache key must compare the bound literals.
                    for ( String value : List.of( "Aa", "BB", "Aa" ) ) {
                        Response response = expect( executeNamedCypher( connection, statementId, Map.of( "value", stringParameter( value ) ) ),
                                Response.TypeCase.STATEMENT_RESULT );
                        RelationalFrame frame = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "name" ) );
                        assertEquals( value, singleRow( frame ).getValues( 0 ).getString().getString(), query );
                    }
                }
                int listStatement = prepareNamedCypher( connection, namespace, "MATCH (n:CacheMember {name: 'Aa'}) RETURN [$first, $second] AS members" ).getStatementId();
                for ( List<String> members : List.of( List.of( "a, b", "c" ), List.of( "a", "b, c" ) ) ) {
                    Response response = expect( executeNamedCypher( connection, listStatement, Map.of(
                            "first", stringParameter( members.get( 0 ) ), "second", stringParameter( members.get( 1 ) ) ) ), Response.TypeCase.STATEMENT_RESULT );
                    RelationalFrame frame = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "members" ) );
                    assertEquals( members, singleRow( frame ).getValues( 0 ).getList().getValuesList().stream()
                            .map( value -> value.getString().getString() ).toList() );
                }
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + namespace, 1 );
            }
        }
    }


    @Test
    void rejectsComposedWholeGraphScansBeforeAnyWrite() throws IOException {
        String namespace = "cypherbatchscantest";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                Response batch = expect( cypherBatch( connection, namespace, "CREATE (:PartialBatch)", "MATCH (*) CREATE (:IgnoredWrite)" ),
                        Response.TypeCase.ERROR_RESPONSE );
                assertTrue( batch.getErrorResponse().getMessage().contains( "standalone whole-graph scan" ) );
                connection.checkTransactions( 0 );

                for ( String query : List.of( "MATCH (*) CREATE (:IgnoredWrite) RETURN 1 AS written",
                        "CREATE (:IgnoredWrite) MATCH (*) RETURN *", "MATCH (*) RETURN * LIMIT 0" ) ) {
                    Response result = expect( connection.send( statementRequest( namespace, "cypher", query, 1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    assertTrue( result.getErrorResponse().getMessage().contains( "standalone whole-graph scan" ) );
                }
                Frame untouched = connection.execute( namespace, "cypher", "MATCH (n) RETURN n", -1 ).getResult().getFrame();
                assertTrue( graphElements( untouched, true ).isEmpty() );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + namespace, 1 );
            }
        }
    }


    @Test
    void cypherWriteBatchesReturnCountsAndCommitTheirChanges() throws IOException {
        String namespace = "cypherwritebatchtest";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                StatementResult single = connection.execute( namespace, "cypher", "CREATE (:BatchWritten {name: 'third'})", 1 ).getResult();
                Response result = expect( cypherBatch( connection, namespace, "CREATE (:BatchWritten {name: 'first'})", "CREATE (:BatchWritten {name: 'second'})" ),
                        Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                assertEquals( List.of( single.getScalar(), single.getScalar() ), result.getStatementBatchResponse().getScalarsList() );
                connection.checkTransactions( 0 );
                List<GraphElement> written = graphElements( connection.execute( namespace, "cypher", "MATCH (n:BatchWritten) RETURN n ORDER BY n.name", -1 )
                        .getResult().getFrame(), true );
                assertEquals( List.of( "first", "second", "third" ), written.stream()
                        .map( element -> element.getNode().getPropertiesOrThrow( "name" ).getString().getString() ).toList() );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + namespace, 1 );
            }
        }
    }


    @Test
    void scalarCypherWritesApplyToEveryMatchedRow() throws IOException {
        String namespace = "cypherscalarwriterows";
        try ( PrismConnection writer = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                writer.execute( namespace, "cypher", """
                        CREATE (:ScalarSet {name:'first', untouched:7, value:'old'}), (:ScalarSet {name:'second', untouched:8, value:'old'}),
                            (:BatchDelete), (:BatchDelete), (:StandaloneDelete), (:StandaloneDelete)
                        """, 1 );
                Response batch = expect( cypherBatch( writer, namespace,
                        "MATCH (n:ScalarSet) SET n.value = 'updated'",
                        "MATCH (n:BatchDelete) DELETE n",
                        "MATCH (n:Missing) SET n.value = 'ignored'",
                        "MATCH (n:Missing) DELETE n" ), Response.TypeCase.STATEMENT_BATCH_RESPONSE );
                assertEquals( List.of( 2L, 2L, 0L, 0L ), batch.getStatementBatchResponse().getScalarsList() );
                writer.checkTransactions( 0 );

                StatementResult deleted = writer.execute( namespace, "cypher", "MATCH (n:StandaloneDelete) DELETE n", 1 ).getResult();
                assertFalse( deleted.hasFrame() );
                assertEquals( 2, deleted.getScalar() );
                writer.checkTransactions( 0 );

                try ( PrismConnection reader = new PrismConnection( true, 3 ) ) {
                    Frame stored = reader.execute( namespace, "cypher", """
                            MATCH (n:ScalarSet)
                            RETURN n.name AS name, n.untouched AS untouched, n.value AS value
                            ORDER BY name
                            """, -1 ).getResult().getFrame();
                    List<Row> rows = relationalFrame( stored, true, List.of( "name", "untouched", "value" ) ).getRowsList();
                    assertEquals( List.of( "first", "second" ), rows.stream().map( row -> row.getValues( 0 ).getString().getString() ).toList() );
                    assertEquals( List.of( 7L, 8L ), rows.stream().map( row -> integerValue( row.getValues( 1 ) ) ).toList() );
                    assertEquals( List.of( "updated", "updated" ), rows.stream().map( row -> row.getValues( 2 ).getString().getString() ).toList() );
                    for ( String label : List.of( "BatchDelete", "StandaloneDelete" ) ) {
                        Frame frame = reader.execute( namespace, "cypher", "MATCH (n:" + label + ") RETURN n", -1 ).getResult().getFrame();
                        assertTrue( graphElements( frame, true ).isEmpty(), label );
                    }

                    StatementResult updated = writer.execute( namespace, "cypher", "MATCH (n:ScalarSet) SET n.value = 'standalone'", 1 ).getResult();
                    assertFalse( updated.hasFrame() );
                    assertEquals( 2, updated.getScalar() );
                    writer.checkTransactions( 0 );
                    Frame reread = reader.execute( namespace, "cypher", """
                            MATCH (n:ScalarSet) RETURN n.value AS value ORDER BY n.name
                            """, -1 ).getResult().getFrame();
                    List<Row> updatedRows = relationalFrame( reread, true, List.of( "value" ) ).getRowsList();
                    assertEquals( List.of( "standalone", "standalone" ),
                            updatedRows.stream().map( row -> row.getValues( 0 ).getString().getString() ).toList() );
                }
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    private static Response setCypherResultFormat( PrismConnection connection, String format ) throws IOException {
        return connection.send( Request.newBuilder().setSetClientInfoPropertiesRequest( ClientInfoProperties.newBuilder()
                .putProperties( PIClientInfoProperties.CYPHER_RESULT_FORMAT, format ) ) );
    }


    private static Response cypherBatch( PrismConnection connection, String namespace, String... queries ) throws IOException {
        ExecuteUnparameterizedStatementBatchRequest.Builder batch = ExecuteUnparameterizedStatementBatchRequest.newBuilder();
        for ( String query : queries ) {
            batch.addStatements( statementRequest( namespace, "cypher", query, 1 ).getExecuteUnparameterizedStatementRequest() );
        }
        return connection.send( Request.newBuilder().setExecuteUnparameterizedStatementBatchRequest( batch ) );
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


    @Test
    void returnsCreatedGraphBindingsAndKeepsTheirStoredIdentities() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            StatementResponse result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (a:ReturningWrite {name: 'Left'})-[e:RETURN_LINK]->(b:ReturningWrite {name: 'Right'}),
                           (a)<-[reverse:RETURN_REVERSE]-(c:ReturningWrite {name: 'Third'})
                    RETURN a, e, b, c, reverse, a.name AS name
                    """, 0 );
            RelationalFrame initial = relationalFrame( result.getResult().getFrame(), false, List.of( "a", "e", "b", "c", "reverse", "name" ) );
            assertEquals( 0, initial.getRowsCount() );
            connection.checkTransactions( 1 );

            Frame last = connection.fetch( result.getStatementId(), 1 );
            Row created = singleRow( relationalFrame( last, true, List.of( "a", "e", "b", "c", "reverse", "name" ) ) );
            ProtoNode a = created.getValues( 0 ).getNode();
            ProtoEdge edge = created.getValues( 1 ).getEdge();
            ProtoNode b = created.getValues( 2 ).getNode();
            ProtoNode c = created.getValues( 3 ).getNode();
            ProtoEdge reverse = created.getValues( 4 ).getEdge();
            assertFalse( a.getId().isBlank() );
            assertFalse( edge.getId().isBlank() );
            assertFalse( reverse.getId().isBlank() );
            assertNotEquals( edge.getId(), reverse.getId() );
            assertNotEquals( a.getId(), b.getId() );
            assertNotEquals( a.getId(), c.getId() );
            assertNotEquals( b.getId(), c.getId() );
            assertEquals( a.getId(), edge.getLeft() );
            assertEquals( b.getId(), edge.getRight() );
            assertEquals( c.getId(), reverse.getLeft() );
            assertEquals( a.getId(), reverse.getRight() );
            assertEquals( ProtoEdge.Direction.LEFT_TO_RIGHT, reverse.getDirection() );
            assertEquals( "Left", created.getValues( 5 ).getString().getString() );
            connection.checkTransactions( 0 );

            Frame read = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:ReturningWrite {name: 'Left'})-[e:RETURN_LINK]->(b:ReturningWrite)
                    RETURN a, e, b, a.name AS name
                    """, 1 ).getResult().getFrame();
            Row stored = singleRow( relationalFrame( read, true, List.of( "a", "e", "b", "name" ) ) );
            assertEquals( a.getId(), stored.getValues( 0 ).getNode().getId() );
            assertEquals( edge.getId(), stored.getValues( 1 ).getEdge().getId() );
            assertEquals( b.getId(), stored.getValues( 2 ).getNode().getId() );

            Frame backwards = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (c:ReturningWrite {name: 'Third'})-[reverse:RETURN_REVERSE]->(a:ReturningWrite)
                    RETURN c, reverse, a
                    """, 1 ).getResult().getFrame();
            List<GraphElement> reversed = graphElements( backwards, true );
            assertEquals( 3, reversed.size() );
            assertEquals( c.getId(), reversed.get( 0 ).getNode().getId() );
            assertEquals( reverse.getId(), reversed.get( 1 ).getEdge().getId() );
            assertEquals( a.getId(), reversed.get( 2 ).getNode().getId() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void createsOnceWhenReturnIsEmptyAndRollsBackManualWrites() throws IOException {
        try ( PrismConnection autocommit = new PrismConnection( true ) ) {
            Frame noRows = autocommit.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (:ReturningLimit) RETURN 1 AS value LIMIT 0", 2 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( noRows, true, List.of( "value" ) ).getRowsCount() );
            List<GraphElement> stored = graphElements( autocommit.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningLimit) RETURN n", 2 ).getResult().getFrame(), true );
            assertEquals( 1, stored.size() );
            autocommit.checkTransactions( 0 );
        }

        try ( PrismConnection manual = new PrismConnection( false, 3 ) ) {
            Frame returned = manual.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (n:ReturningRollback) RETURN n", 1 ).getResult().getFrame();
            List<GraphElement> created = graphElements( returned, true );
            assertEquals( 1, created.size() );
            String createdId = created.get( 0 ).getNode().getId();
            Frame fullGraph = manual.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH (*) RETURN *", 1 ).getResult().getFrame();
            assertTrue( graphElements( fullGraph, true ).stream().anyMatch( element -> element.hasNode() && element.getNode().getId().equals( createdId ) ) );
            manual.checkTransactions( 1 );
            expect( manual.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) ), Response.TypeCase.ROLLBACK_RESPONSE );
            manual.checkTransactions( 0 );
        }
        try ( PrismConnection autocommit = new PrismConnection( true ) ) {
            List<GraphElement> stored = graphElements( autocommit.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningRollback) RETURN n", 2 ).getResult().getFrame(), true );
            assertTrue( stored.isEmpty() );
            autocommit.checkTransactions( 0 );
        }
    }


    @Test
    void returnsBoundCreateValuesWithFreshIdsOnEachExecution() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            PreparedStatementSignature prepared = prepareNamedCypher( connection, WRITE_RETURN_NAMESPACE, "CREATE (n:ReturningParameter {name: $name}) RETURN n" );
            ProtoNode first = createdNode( connection, prepared.getStatementId(), "First" );
            ProtoNode second = createdNode( connection, prepared.getStatementId(), "Second" );
            assertNotEquals( first.getId(), second.getId() );

            List<GraphElement> stored = graphElements( connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningParameter) RETURN n", 3 ).getResult().getFrame(), true );
            assertEquals( 2, stored.size() );
            assertEquals( Set.of( first.getId(), second.getId() ), stored.stream()
                    .map( element -> element.getNode().getId() ).collect( Collectors.toSet() ) );
            connection.checkTransactions( 0 );
        }
    }


    private static ProtoNode createdNode( PrismConnection connection, int statementId, String name ) throws IOException {
        Response result = expect( executeNamedCypher( connection, statementId, Map.of( "name", stringParameter( name ) ) ), Response.TypeCase.STATEMENT_RESULT );
        List<GraphElement> elements = graphElements( result.getStatementResult().getFrame(), true );
        assertEquals( 1, elements.size() );
        ProtoNode node = elements.get( 0 ).getNode();
        assertEquals( name, node.getPropertiesOrThrow( "name" ).getString().getString() );
        return node;
    }


    @Test
    void createsNamedPathsAndReturnsTheirStoredIdentities() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE p = (a:NamedCreatedPath)<-[:NAMED_RETURN_REL]-(b:NamedCreatedPath) RETURN p, a, b, 1 AS marker", 1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( result, true, List.of( "p", "a", "b", "marker" ) ) );
            ProtoPath path = row.getValues( 0 ).getPath();
            assertEquals( row.getValues( 1 ).getNode().getId(), path.getNodes( 0 ).getId() );
            assertEquals( row.getValues( 2 ).getNode().getId(), path.getNodes( 1 ).getId() );
            assertEquals( path.getNodes( 1 ).getId(), path.getEdges( 0 ).getLeft() );
            assertEquals( path.getNodes( 0 ).getId(), path.getEdges( 0 ).getRight() );

            Frame read = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (:NamedCreatedPath)-[e:NAMED_RETURN_REL]->(:NamedCreatedPath) RETURN e", 2 ).getResult().getFrame();
            assertEquals( path.getEdges( 0 ).getId(), graphElements( read, true ).get( 0 ).getEdge().getId() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void createsOneDistinctNodePerUnwoundRow() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    UNWIND [7, 7, 8] AS value
                    CREATE (n:ReturningUnwind {value: value})
                    RETURN n, value ORDER BY value
                    """, -1 ).getResult().getFrame();
            List<Row> rows = relationalFrame( result, true, List.of( "n", "value" ) ).getRowsList();
            assertEquals( List.of( 7L, 7L, 8L ), rows.stream().map( row -> integerValue( row.getValues( 1 ) ) ).toList() );
            Set<String> ids = rows.stream().map( row -> row.getValues( 0 ).getNode().getId() ).collect( Collectors.toSet() );
            assertEquals( 3, ids.size() );
            for ( Row row : rows ) {
                assertEquals( integerValue( row.getValues( 1 ) ), integerValue( row.getValues( 0 ).getNode().getPropertiesOrThrow( "value" ) ) );
            }
            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningUnwind) RETURN n", -1 ).getResult().getFrame();
            assertEquals( ids, graphElements( stored, true ).stream().map( element -> element.getNode().getId() ).collect( Collectors.toSet() ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void deduplicatesNodeProjectionsBeforeReturningOrMatchingAgain() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (a:DistinctProjectionSource {name: 'source'})-[:DISTINCT_PROJECTION]->(:DistinctProjectionEnd),
                        (a)-[:DISTINCT_PROJECTION]->(:DistinctProjectionEnd)
                    """, -1 );

            String match = "MATCH (a:DistinctProjectionSource)-[:DISTINCT_PROJECTION]->(:DistinctProjectionEnd) ";
            Frame all = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", match + "RETURN a", -1 ).getResult().getFrame();
            assertEquals( 2, relationalFrame( all, true, List.of( "a" ) ).getRowsCount() );

            Frame distinct = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", match + "RETURN DISTINCT a", -1 ).getResult().getFrame();
            ProtoNode node = singleRow( relationalFrame( distinct, true, List.of( "a" ) ) ).getValues( 0 ).getNode();
            assertEquals( "source", node.getPropertiesOrThrow( "name" ).getString().getString() );

            Frame rematched = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    match + "WITH DISTINCT a MATCH (a)-[:DISTINCT_PROJECTION]->(b) RETURN count(*) AS total", -1 ).getResult().getFrame();
            Row count = singleRow( relationalFrame( rematched, true, List.of( "total" ) ) );
            assertEquals( 2L, integerValue( count.getValues( 0 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void createsAndReturnsRelationshipsForEachMatchedNode() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:ReturningMatchSeed {name: 'first'}), (:ReturningMatchSeed {name: 'second'})", 1 );
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (s:ReturningMatchSeed)
                    CREATE p = (s)-[e:RETURN_CHILD]->(n:ReturningChild {name: s.name})
                    RETURN p, n.name AS child ORDER BY child
                    """, -1 ).getResult().getFrame();
            RelationalFrame table = relationalFrame( result, true, List.of( "p", "child" ) );
            assertEquals( List.of( "first", "second" ), table.getRowsList().stream().map( row -> row.getValues( 1 ).getString().getString() ).toList() );
            Set<String> returnedEdges = table.getRowsList().stream().map( row -> row.getValues( 0 ).getPath().getEdges( 0 ).getId() ).collect( Collectors.toSet() );
            assertEquals( 2, returnedEdges.size() );
            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (:ReturningMatchSeed)-[e:RETURN_CHILD]->(:ReturningChild) RETURN e", -1 ).getResult().getFrame();
            assertEquals( returnedEdges, graphElements( stored, true ).stream().map( element -> element.getEdge().getId() ).collect( Collectors.toSet() ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void reusesBothMatchedPathEndpointsWhenCreatingARelationship() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:PathSource)-[:PATH_SEED]->(:PathTarget)", -1 );

            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p = (a:PathSource)-[:PATH_SEED]->(b:PathTarget)
                    CREATE q = (a)-[e:PATH_ADDED]->(b)
                    RETURN p, q, e
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( result, true, List.of( "p", "q", "e" ) ) );
            ProtoPath existing = row.getValues( 0 ).getPath();
            ProtoPath appended = row.getValues( 1 ).getPath();
            ProtoEdge edge = row.getValues( 2 ).getEdge();
            assertEquals( existing.getNodesList(), appended.getNodesList() );
            assertEquals( appended.getEdgesList(), List.of( edge ) );
            assertEquals( existing.getNodes( 0 ).getId(), edge.getLeft() );
            assertEquals( existing.getNodes( 1 ).getId(), edge.getRight() );

            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (a)-[e:PATH_ADDED]->(b) RETURN a, b, e", -1 ).getResult().getFrame();
            Row storedRow = singleRow( relationalFrame( stored, true, List.of( "a", "b", "e" ) ) );
            assertEquals( edge.getLeft(), storedRow.getValues( 0 ).getNode().getId() );
            assertEquals( edge.getRight(), storedRow.getValues( 1 ).getNode().getId() );
            assertTrue( storedRow.getValues( 0 ).getNode().getLabelsList().contains( "PathSource" ) );
            assertTrue( storedRow.getValues( 1 ).getNode().getLabelsList().contains( "PathTarget" ) );
            assertEquals( edge.getId(), storedRow.getValues( 2 ).getEdge().getId() );

            StatementResult mutation = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:PathSource)-[:PATH_SEED]->(b:PathTarget)
                    CREATE (a)-[:PATH_ADDED_WITHOUT_RETURN]->(b)
                    """, -1 ).getResult();
            assertFalse( mutation.hasFrame() );
            assertEquals( 1, mutation.getScalar() );
            Frame storedMutation = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH ()-[e:PATH_ADDED_WITHOUT_RETURN]->() RETURN e", -1 ).getResult().getFrame();
            ProtoEdge storedEdge = singleRow( relationalFrame( storedMutation, true, List.of( "e" ) ) ).getValues( 0 ).getEdge();
            assertEquals( edge.getLeft(), storedEdge.getLeft() );
            assertEquals( edge.getRight(), storedEdge.getRight() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void setsPropertiesOnRelationshipsBoundByAPath() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:PathEditSource)-[:PATH_EDIT {status: 'before'}]->(:PathEditTarget)", -1 );
            Frame updated = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:PathEditSource)-[e:PATH_EDIT]->(b:PathEditTarget)
                    SET e.status = 'after'
                    RETURN e, e.status AS status
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( updated, true, List.of( "e", "status" ) ) );
            ProtoEdge edge = row.getValues( 0 ).getEdge();
            assertEquals( "after", edge.getPropertiesOrThrow( "status" ).getString().getString() );
            assertEquals( "after", row.getValues( 1 ).getString().getString() );

            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH ()-[e:PATH_EDIT]->() RETURN e, e.status AS status", -1 ).getResult().getFrame();
            Row storedRow = singleRow( relationalFrame( stored, true, List.of( "e", "status" ) ) );
            assertEquals( edge.getId(), storedRow.getValues( 0 ).getEdge().getId() );
            assertEquals( "after", storedRow.getValues( 1 ).getString().getString() );

            StatementResult scalar = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (:PathEditSource)-[e:PATH_EDIT]->(:PathEditTarget)
                    SET e.status = 'scalar'
                    """, -1 ).getResult();
            assertFalse( scalar.hasFrame() );
            assertEquals( 1, scalar.getScalar() );
            Frame afterScalar = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH ()-[e:PATH_EDIT]->() RETURN e.status AS status", -1 ).getResult().getFrame();
            assertEquals( "scalar", singleRow( relationalFrame( afterScalar, true, List.of( "status" ) ) ).getValues( 0 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void setsPropertiesWithoutChangingValuesProjectedByWith() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:PriorProperty {status: 'old'})", -1 );
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:PriorProperty)
                    WITH n, n.status AS previous
                    SET n.status = 'new'
                    RETURN n.status AS current, previous
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( result, true, List.of( "current", "previous" ) ) );
            assertEquals( "new", row.getValues( 0 ).getString().getString() );
            assertEquals( "old", row.getValues( 1 ).getString().getString() );
            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:PriorProperty) RETURN n.status AS status", -1 ).getResult().getFrame();
            assertEquals( "new", singleRow( relationalFrame( stored, true, List.of( "status" ) ) ).getValues( 0 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void mergesWritesThroughIndependentMatchesToTheSameNode() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (:SharedWriteNode {a:'old', b:'old'})", -1 );
            Frame writes = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:SharedWriteNode) WITH a MATCH (b:SharedWriteNode)
                    SET a.a = 'one', b.b = 'two'
                    RETURN a.a AS aa, a.b AS ab, b.a AS ba, b.b AS bb
                    """, -1 ).getResult().getFrame();
            Row aliases = singleRow( relationalFrame( writes, true, List.of( "aa", "ab", "ba", "bb" ) ) );
            assertEquals( List.of( "one", "two", "one", "two" ), aliases.getValuesList().stream().map( value -> value.getString().getString() ).toList() );
            String read = "MATCH (n:SharedWriteNode) RETURN n.a AS a, n.b AS b";
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", read, -1 ).getResult().getFrame();
            assertEquals( List.of( stringParameter( "one" ), stringParameter( "two" ) ), singleRow( relationalFrame( stored, true, List.of( "a", "b" ) ) ).getValuesList() );

            StatementResult scalar = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (a:SharedWriteNode) OPTIONAL MATCH (b:SharedWriteNode)
                    SET a.a = 'three', b.b = 'four'
                    """, -1 ).getResult();
            assertFalse( scalar.hasFrame() );
            assertEquals( 1, scalar.getScalar() );
            stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", read, -1 ).getResult().getFrame();
            assertEquals( List.of( stringParameter( "three" ), stringParameter( "four" ) ), singleRow( relationalFrame( stored, true, List.of( "a", "b" ) ) ).getValuesList() );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void mergesRelationshipWritesAndUpdatesPathBindings() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:SharedWriteSource)-[:SHARED_WRITE_EDGE {a:'old', b:'old'}]->(:SharedWriteTarget)
                    """, -1 );
            Frame writes = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH p=(:SharedWriteSource)-[a:SHARED_WRITE_EDGE]->(:SharedWriteTarget)
                    WITH p, a
                    MATCH ()-[b:SHARED_WRITE_EDGE]->()
                    SET a.a = 'one', b.b = 'two'
                    RETURN p, a.b AS ab, b.a AS ba
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( writes, true, List.of( "p", "ab", "ba" ) ) );
            assertEquals( Map.of( "a", stringParameter( "one" ), "b", stringParameter( "two" ) ), row.getValues( 0 ).getPath().getEdges( 0 ).getPropertiesMap() );
            assertEquals( stringParameter( "two" ), row.getValues( 1 ) );
            assertEquals( stringParameter( "one" ), row.getValues( 2 ) );
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH ()-[r:SHARED_WRITE_EDGE]->() RETURN r.a AS a, r.b AS b
                    """, -1 ).getResult().getFrame();
            assertEquals( List.of( stringParameter( "one" ), stringParameter( "two" ) ), singleRow( relationalFrame( stored, true, List.of( "a", "b" ) ) ).getValuesList() );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void matchesGraphStateWrittenByEarlierClauses() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            Frame created = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (n:ReadAfterWrite {value:'created'})
                    WITH n MATCH (m:ReadAfterWrite)
                    RETURN n.value AS written, m.value AS observed
                    """, -1 ).getResult().getFrame();
            assertEquals( List.of( stringParameter( "created" ), stringParameter( "created" ) ),
                    singleRow( relationalFrame( created, true, List.of( "written", "observed" ) ) ).getValuesList() );

            Frame updated = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:ReadAfterWrite) SET n.value = 'updated'
                    WITH n MATCH (m:ReadAfterWrite {value:'updated'})
                    RETURN m.value AS value
                    """, -1 ).getResult().getFrame();
            assertEquals( stringParameter( "updated" ), singleRow( relationalFrame( updated, true, List.of( "value" ) ) ).getValues( 0 ) );
            String storedQuery = "MATCH (n:ReadAfterWrite) RETURN n.value AS value";
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", storedQuery, -1 ).getResult().getFrame();
            assertEquals( stringParameter( "updated" ), singleRow( relationalFrame( stored, true, List.of( "value" ) ) ).getValues( 0 ) );

            Frame deleted = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:ReadAfterWrite) DELETE n
                    WITH 1 AS marker OPTIONAL MATCH (m:ReadAfterWrite)
                    RETURN marker, m.value AS remaining
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( deleted, true, List.of( "marker", "remaining" ) ) );
            assertEquals( 1, integerValue( row.getValues( 0 ) ) );
            assertTrue( row.getValues( 1 ).hasNull() );
            Frame remaining = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", storedQuery, -1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( remaining, true, List.of( "value" ) ).getRowsCount() );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void executesCreateAndSetClausesWithIntermediateBindings() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            Frame result = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (n:ChainedWriteSource)
                    WITH n SET n.a = 'first'
                    WITH n, n.a AS prior SET n.b = 'second'
                    RETURN prior, n.a AS a, n.b AS b
                    """, -1 ).getResult().getFrame();
            assertEquals( List.of( stringParameter( "first" ), stringParameter( "first" ), stringParameter( "second" ) ),
                    singleRow( relationalFrame( result, true, List.of( "prior", "a", "b" ) ) ).getValuesList() );
            StatementResult scalar = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:ChainedWriteSource) SET n.a = 'third'
                    WITH n CREATE (n)-[:CHAINED_WRITE]->(:ChainedWriteTarget {saved:n.a})
                    """, -1 ).getResult();
            assertFalse( scalar.hasFrame() );
            assertEquals( 1, scalar.getScalar() );
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:ChainedWriteSource)-[:CHAINED_WRITE]->(m:ChainedWriteTarget)
                    RETURN n.a AS a, n.b AS b, m.saved AS saved
                    """, -1 ).getResult().getFrame();
            assertEquals( List.of( stringParameter( "third" ), stringParameter( "second" ), stringParameter( "third" ) ),
                    singleRow( relationalFrame( stored, true, List.of( "a", "b", "saved" ) ) ).getValuesList() );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void removesNullPropertiesWhenSettingAGraphMap() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (:WriteMap {dropMe:'old', keepMe:'kept', neverSet:null})", -1 );
            Frame updated = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:WriteMap) SET n += {dropMe:null, addMe:'new'} RETURN n
                    """, -1 ).getResult().getFrame();
            assertEquals( Map.of( "keepMe", stringParameter( "kept" ), "addMe", stringParameter( "new" ) ),
                    graphElements( updated, true ).get( 0 ).getNode().getPropertiesMap() );
            StatementResult scalar = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:WriteMap) SET n = {addMe:null, fresh:'scalar'}
                    """, -1 ).getResult();
            assertFalse( scalar.hasFrame() );
            assertEquals( 1, scalar.getScalar() );
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH (n:WriteMap) RETURN n", -1 ).getResult().getFrame();
            assertEquals( Map.of( "fresh", stringParameter( "scalar" ) ), graphElements( stored, true ).get( 0 ).getNode().getPropertiesMap() );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void projectsAllWithRowsBeforeUpdatingASharedNode() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (n:SharedProperty {status: 'old'})-[:SHARED_LINK]->(:SharedNeighbor {idx: 'one'}),
                           (n)-[:SHARED_LINK]->(:SharedNeighbor {idx: 'two'})
                    """, -1 );
            Frame result = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n:SharedProperty)-[:SHARED_LINK]->(neighbor:SharedNeighbor)
                    WITH n, neighbor.idx AS idx, n.status AS previous
                    SET n.status = 'new'
                    RETURN idx, previous, n.status AS current ORDER BY idx
                    """, -1 ).getResult().getFrame();
            var rows = relationalFrame( result, true, List.of( "idx", "previous", "current" ) ).getRowsList();
            assertEquals( 2, rows.size() );
            assertEquals( "one", rows.get( 0 ).getValues( 0 ).getString().getString() );
            assertEquals( "two", rows.get( 1 ).getValues( 0 ).getString().getString() );
            for ( Row row : rows ) {
                assertEquals( "old", row.getValues( 1 ).getString().getString() );
                assertEquals( "new", row.getValues( 2 ).getString().getString() );
            }
            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:SharedProperty) RETURN n.status AS status", -1 ).getResult().getFrame();
            assertEquals( "new", singleRow( relationalFrame( stored, true, List.of( "status" ) ) ).getValues( 0 ).getString().getString() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void setsNodeLabelsIdempotentlyAcrossStores() throws IOException {
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( writer, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            writer.execute( WRITE_RETURN_NAMESPACE, "cypher", "CREATE (:IdempotentLabel)", -1 );
            List<String> expected = List.of( "AddedLabel", "IdempotentLabel" );
            String query = """
                    MATCH (n:IdempotentLabel) WITH last([null,n]) AS target
                    SET target:IdempotentLabel:AddedLabel:AddedLabel
                    RETURN target
                    """;
            for ( int i = 0; i < 2; i++ ) {
                Frame result = writer.execute( WRITE_RETURN_NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
                ProtoNode updated = singleRow( relationalFrame( result, true, List.of( "target" ) ) ).getValues( 0 ).getNode();
                assertEquals( expected, updated.getLabelsList().stream().sorted().toList() );
            }
            Frame stored = reader.execute( WRITE_RETURN_NAMESPACE, "cypher", "MATCH (n:IdempotentLabel) RETURN n", -1 ).getResult().getFrame();
            assertEquals( expected, graphElements( stored, true ).get( 0 ).getNode().getLabelsList().stream().sorted().toList() );
            writer.checkTransactions( 0 );
        }
    }


    @Test
    void removesPropertiesAndLabelsBeforeReturningTheUpdatedNode() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (:ReturningRemove:Discarded {dropMe: 'old', keepMe: 'remove-retained'})", -1 );
            Frame updated = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    MATCH (n) WHERE n.keepMe = 'remove-retained'
                    REMOVE n.dropMe, n:Discarded
                    RETURN n
                    """, -1 ).getResult().getFrame();
            ProtoNode node = graphElements( updated, true ).get( 0 ).getNode();
            assertEquals( List.of( "ReturningRemove" ), node.getLabelsList() );
            assertEquals( Set.of( "keepMe" ), node.getPropertiesMap().keySet() );

            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningRemove) RETURN n", -1 ).getResult().getFrame();
            ProtoNode storedNode = graphElements( stored, true ).get( 0 ).getNode();
            assertEquals( node.getId(), storedNode.getId() );
            assertEquals( node.getLabelsList(), storedNode.getLabelsList() );
            assertEquals( node.getPropertiesMap(), storedNode.getPropertiesMap() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void skipsRemoveWhenOptionalMatchDoesNotBindTheTarget() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( setCypherResultFormat( connection, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            Frame frame = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    OPTIONAL MATCH (n:UnboundRemoveTarget)
                    REMOVE n.property, n:UnusedLabel
                    RETURN n
                    """, -1 ).getResult().getFrame();
            assertTrue( singleRow( relationalFrame( frame, true, List.of( "n" ) ) ).getValues( 0 ).hasNull() );
            StatementResult scalar = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    OPTIONAL MATCH (n:UnboundRemoveTarget)
                    REMOVE n.property
                    """, -1 ).getResult();
            assertFalse( scalar.hasFrame() );
            assertEquals( 0, scalar.getScalar() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void executesReturningWritesEvenWhenLimitSuppressesTheirRows() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame empty = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    UNWIND ['a', 'b'] AS name
                    CREATE (:ReturningWriteLimit {name: name})
                    RETURN name LIMIT 0
                    """, 1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( empty, true, List.of( "name" ) ).getRowsCount() );
            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningWriteLimit) RETURN n.name AS name ORDER BY name", -1 ).getResult().getFrame();
            assertEquals( List.of( "a", "b" ), projectedNames( relationalFrame( stored, true, List.of( "name" ) ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rollsBackRowDependentReturningWrites() throws IOException {
        try ( PrismConnection connection = new PrismConnection( false, 3 ) ) {
            Frame created = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "UNWIND ['pending'] AS name CREATE (n:ReturningDynamicRollback {name: name}) RETURN n", 1 ).getResult().getFrame();
            assertEquals( 1, graphElements( created, true ).size() );
            connection.checkTransactions( 1 );
            expect( connection.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) ), Response.TypeCase.ROLLBACK_RESPONSE );
            connection.checkTransactions( 0 );
        }
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame stored = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningDynamicRollback) RETURN n", 1 ).getResult().getFrame();
            assertTrue( graphElements( stored, true ).isEmpty() );
        }
    }


    @Test
    void rejectsWritesThroughDeletedGraphBindings() throws IOException {
        String namespace = "cypherstalegraphbindings";
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                writer.execute( namespace, "cypher", """
                        CREATE (:StaleTarget {value:'original'}),
                               (:StaleSource)-[:STALE_EDGE {value:'original'}]->(:StaleDestination)
                        """, -1 );
                expect( setCypherResultFormat( reader, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
                Map<String, String> staleWrites = Map.of(
                        "MATCH (n:StaleTarget) DELETE n WITH n SET n.value = 'changed' RETURN n.value AS value",
                        "Graph node disappeared before the update",
                        "MATCH (n:StaleTarget) DELETE n WITH n CREATE (n)-[:DANGLING]->(:InvalidEndpoint)",
                        "Graph relationship requires exactly one source and one target node",
                        "MATCH ()-[r:STALE_EDGE]->() DELETE r WITH r SET r.value = 'changed' RETURN r.value AS value",
                        "Graph relationship disappeared before the update" );
                for ( Map.Entry<String, String> staleWrite : staleWrites.entrySet() ) {
                    Response failure = expect( writer.send( statementRequest( namespace, "cypher", staleWrite.getKey(), -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    assertTrue( failure.getErrorResponse().getMessage().contains( staleWrite.getValue() ), failure.getErrorResponse().getMessage() );
                    writer.checkTransactions( 0 );
                    assertStoredGraph( reader, namespace, 3, 1 );
                    for ( String match : List.of( "MATCH (n:StaleTarget) RETURN n.value AS value", "MATCH ()-[r:STALE_EDGE]->() RETURN r.value AS value" ) ) {
                        Frame stored = reader.execute( namespace, "cypher", match, -1 ).getResult().getFrame();
                        assertEquals( "original", singleRow( relationalFrame( stored, true, List.of( "value" ) ) ).getValues( 0 ).getString().getString() );
                    }
                }
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void abortsManualTransactionWhenAChainedGraphWriteFails() throws IOException {
        String namespace = "cypherfailedmanualwrite";
        try ( PrismConnection reader = new PrismConnection( true, 3 ); PrismConnection writer = new PrismConnection( false, 3 ) ) {
            reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                Request.Builder commit = Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() );
                for ( String suffix : List.of( "", " RETURN a" ) ) {
                    writer.execute( namespace, "cypher", "CREATE (:EarlierPendingWrite)", -1 );
                    Response failure = expect( writer.send( statementRequest( namespace, "cypher", """
                            OPTIONAL MATCH (n:AbsentEndpoint)
                            CREATE (a:IncompleteWrite) WITH n,a
                            CREATE (n)-[:INVALID_LINK]->(a)
                            """ + suffix, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    assertTrue( failure.getErrorResponse().getMessage().contains( "CREATE requires a bound source and target node" ),
                            failure.getErrorResponse().getMessage() );
                    writer.checkTransactions( 0 );
                    expect( writer.send( commit ), Response.TypeCase.COMMIT_RESPONSE );
                    Frame stored = reader.execute( namespace, "cypher", "MATCH (n) RETURN n", -1 ).getResult().getFrame();
                    assertTrue( graphElements( stored, true ).isEmpty() );
                }

                writer.execute( namespace, "cypher", "CREATE (:RecoveredWrite)", -1 );
                expect( writer.send( commit ), Response.TypeCase.COMMIT_RESPONSE );
                Frame recovered = reader.execute( namespace, "cypher", "MATCH (n:RecoveredWrite) RETURN n", -1 ).getResult().getFrame();
                assertEquals( 1, graphElements( recovered, true ).size() );
            } finally {
                reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void rejectsDeletionOfConnectedNodesAndSupportsExplicitDetach() throws IOException {
        String namespace = "cypherconnecteddeletes";
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                writer.execute( namespace, "cypher", """
                        CREATE (:DeleteTarget {name:'isolated'}),
                               (:DeleteTarget {name:'connected'})-[:DELETE_LINK {level:7}]->(:DeleteNeighbor)
                        """, -1 );
                expect( setCypherResultFormat( reader, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
                for ( String suffix : List.of( "", " RETURN n.name AS name" ) ) {
                    Response error = expect( writer.send( statementRequest( namespace, "cypher",
                            "MATCH (n:DeleteTarget) DELETE n" + suffix, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                    assertTrue( error.getErrorResponse().getMessage().contains( "relationships" ), error.getErrorResponse().getMessage() );
                    assertStoredGraph( reader, namespace, 3, 1 );
                    writer.checkTransactions( 0 );
                }

                StatementResult detached = writer.execute( namespace, "cypher", "MATCH (n:DeleteTarget) DETACH DELETE n", -1 ).getResult();
                assertFalse( detached.hasFrame() );
                assertEquals( 2, detached.getScalar() );
                assertStoredGraph( reader, namespace, 1, 0 );
                StatementResult missing = writer.execute( namespace, "cypher", "OPTIONAL MATCH (n:DeleteTarget) DETACH DELETE n", -1 ).getResult();
                assertFalse( missing.hasFrame() );
                assertEquals( 0, missing.getScalar() );
                writer.checkTransactions( 0 );
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void deletesConnectedNodesWhenAllIncidentRelationshipsAreExplicitlySelected() throws IOException {
        String namespace = "cypherexplicitdeletes";
        try ( PrismConnection writer = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( true, 3 ) ) {
            writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                writer.execute( namespace, "cypher", """
                        CREATE (:DeleteLeaf {name:'left'})-[:DELETE_LEFT]->(n:DeleteRoot),
                               (n)-[:DELETE_RIGHT]->(:DeleteLeaf {name:'right'})
                        """, -1 );
                expect( setCypherResultFormat( reader, "relational" ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
                Response error = expect( writer.send( statementRequest( namespace, "cypher",
                        "MATCH (n:DeleteRoot)<-[r:DELETE_LEFT]-() DELETE n,r", -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "relationships" ), error.getErrorResponse().getMessage() );
                assertStoredGraph( reader, namespace, 3, 2 );
                Frame deleted = writer.execute( namespace, "cypher", """
                        MATCH (n:DeleteRoot)-[r]-(m:DeleteLeaf)
                        DELETE n, r
                        RETURN m.name AS name ORDER BY name
                        """, -1 ).getResult().getFrame();
                assertEquals( List.of( "left", "right" ), projectedNames( relationalFrame( deleted, true, List.of( "name" ) ) ) );
                assertStoredGraph( reader, namespace, 2, 0 );
                writer.checkTransactions( 0 );
            } finally {
                writer.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    private static void assertStoredGraph( PrismConnection reader, String namespace, int nodeCount, int edgeCount ) throws IOException {
        Frame frame = reader.execute( namespace, "cypher", "MATCH (*) RETURN *", -1 ).getResult().getFrame();
        var graph = frame.getRelationalFrame().getRows( 0 ).getValues( 0 ).getGraph();
        assertEquals( nodeCount, graph.getNodesCount() );
        assertEquals( edgeCount, graph.getEdgesCount() );
        Set<String> nodeIds = graph.getNodesList().stream().map( ProtoNode::getId ).collect( Collectors.toSet() );
        for ( ProtoEdge edge : graph.getEdgesList() ) {
            assertTrue( nodeIds.contains( edge.getLeft() ) );
            assertTrue( nodeIds.contains( edge.getRight() ) );
        }
    }


    @Test
    void returnsUpdatedAndPreDeleteValues() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame created = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "CREATE (n:ReturningEdit {name: 'before'}) RETURN n", 1 ).getResult().getFrame();
            String id = graphElements( created, true ).get( 0 ).getNode().getId();

            Frame updated = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningEdit) SET n.name = 'after' RETURN n, n.name AS name", 1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( updated, true, List.of( "n", "name" ) ) );
            assertEquals( id, row.getValues( 0 ).getNode().getId() );
            assertEquals( "after", row.getValues( 1 ).getString().getString() );

            Frame read = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningEdit) RETURN n.name AS persisted", 1 ).getResult().getFrame();
            assertEquals( "after", singleRow( relationalFrame( read, true, List.of( "persisted" ) ) ).getValues( 0 ).getString().getString() );

            Frame deleted = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningEdit) DELETE n RETURN n.name AS previous", 1 ).getResult().getFrame();
            assertEquals( "after", singleRow( relationalFrame( deleted, true, List.of( "previous" ) ) ).getValues( 0 ).getString().getString() );
            Frame absent = connection.execute( WRITE_RETURN_NAMESPACE, "cypher",
                    "MATCH (n:ReturningEdit) RETURN n", 1 ).getResult().getFrame();
            assertTrue( graphElements( absent, true ).isEmpty() );
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


    @Test
    void evaluatesExistsInPrismForPropertiesAndPaths() throws IOException {
        String namespace = "cypherexistscontract";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", """
                        CREATE (:Person {name:'Alice', enabled:false})-[:KNOWS]->(:Person {name:'Bob'}),
                               (:Person {name:'Carol'})
                        """, -1 );
                PreparedStatementSignature prepared = prepareNamedCypher( connection, namespace, """
                        MATCH (n:Person)
                        RETURN n.name AS name, exists(n.enabled) AS hasEnabled,
                               exists((n)-[:KNOWS]->(:Person {name:$target})) AS reaches
                        ORDER BY name
                        """ );
                Response response = expect( executeNamedCypher( connection, prepared.getStatementId(), Map.of( "target", stringParameter( "Bob" ) ) ),
                        Response.TypeCase.STATEMENT_RESULT );
                RelationalFrame people = relationalFrame( response.getStatementResult().getFrame(), true, List.of( "name", "hasEnabled", "reaches" ) );
                assertEquals( List.of( "Alice", "Bob", "Carol" ), projectedNames( people ) );
                assertEquals( ProtoPolyType.BOOLEAN, people.getColumnMeta( 1 ).getTypeMeta().getProtoValueType() );
                assertEquals( ProtoPolyType.BOOLEAN, people.getColumnMeta( 2 ).getTypeMeta().getProtoValueType() );
                ProtoValue yes = PolyValueSerializer.serialize( PolyBoolean.of( true ) );
                ProtoValue no = PolyValueSerializer.serialize( PolyBoolean.of( false ) );
                assertEquals( List.of( yes, no, no ), people.getRowsList().stream().map( row -> row.getValues( 1 ) ).toList() );
                assertEquals( List.of( yes, no, no ), people.getRowsList().stream().map( row -> row.getValues( 2 ) ).toList() );

                Frame missing = connection.execute( namespace, "cypher", """
                        OPTIONAL MATCH (n:Missing)
                        RETURN exists(n.enabled) AS propertyExists, exists((n)-[:KNOWS]->()) AS patternExists
                        """, -1 ).getResult().getFrame();
                assertEquals( List.of( no, no ), singleRow( relationalFrame( missing, true, List.of( "propertyExists", "patternExists" ) ) ).getValuesList() );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void returnsShortestPathAfterCypherPredicateAndExtendsOptionalMatches() throws IOException {
        String namespace = "cyphershortestcontract";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", """
                        CREATE (a:Stop {name:'A'})-[:ROUTE {active:false}]->(b:Stop {name:'B'}),
                               (a)-[:ROUTE {active:true}]->(:Stop {name:'C'})-[:ROUTE {active:true}]->(b),
                               (:Stop {name:'D'})
                        """, -1 );
                Frame filtered = connection.execute( namespace, "cypher", """
                        MATCH (a:Stop {name:'A'}), (b:Stop {name:'B'}), p = shortestPath((a)-[:ROUTE*..3]->(b))
                        WHERE length(p) > 1
                        RETURN p AS route, length(p) AS hops
                        """, -1 ).getResult().getFrame();
                RelationalFrame route = relationalFrame( filtered, true, List.of( "route", "hops" ) );
                assertEquals( ProtoPolyType.PATH, route.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
                Row shortest = singleRow( route );
                ProtoPath path = shortest.getValues( 0 ).getPath();
                assertEquals( List.of( "A", "C", "B" ), path.getNodesList().stream()
                        .map( node -> node.getPropertiesOrThrow( "name" ).getString().getString() ).toList() );
                assertEquals( 2, path.getEdgesCount() );
                assertEquals( 2, integerValue( shortest.getValues( 1 ) ) );

                Frame optional = connection.execute( namespace, "cypher", """
                        MATCH (a:Stop {name:'A'}), (b:Stop)
                        WHERE b.name IN ['B','D']
                        OPTIONAL MATCH p = shortestPath((a)-[:ROUTE*..3]->(b))
                        RETURN b.name AS destination, p AS route
                        ORDER BY destination
                        """, -1 ).getResult().getFrame();
                RelationalFrame destinations = relationalFrame( optional, true, List.of( "destination", "route" ) );
                assertEquals( List.of( "B", "D" ), projectedNames( destinations ) );
                assertEquals( 1, destinations.getRows( 0 ).getValues( 1 ).getPath().getEdgesCount() );
                assertTrue( destinations.getRows( 1 ).getValues( 1 ).hasNull() );

                Frame expression = connection.execute( namespace, "cypher", """
                        MATCH (a:Stop {name:'A'}), (b:Stop)
                        WHERE b.name IN ['B','D']
                        RETURN b.name AS destination, shortestPath((a)-[:ROUTE*..3]->(b)) AS route
                        ORDER BY destination
                        """, -1 ).getResult().getFrame();
                RelationalFrame expressionRows = relationalFrame( expression, true, List.of( "destination", "route" ) );
                assertEquals( List.of( "B", "D" ), projectedNames( expressionRows ) );
                assertEquals( 1, expressionRows.getRows( 0 ).getValues( 1 ).getPath().getEdgesCount() );
                assertTrue( expressionRows.getRows( 1 ).getValues( 1 ).hasNull() );

                PreparedStatementSignature prepared = prepareNamedCypher( connection, namespace, """
                        MATCH (a:Stop {name:'A'}), (b:Stop {name:'B'})
                        RETURN length(shortestPath((a)-[:ROUTE*..3 {active:$active}]->(b))) AS hops
                        """ );
                for ( boolean active : List.of( true, false ) ) {
                    Response response = expect( executeNamedCypher( connection, prepared.getStatementId(),
                            Map.of( "active", PolyValueSerializer.serialize( PolyBoolean.of( active ) ) ) ), Response.TypeCase.STATEMENT_RESULT );
                    Row row = singleRow( relationalFrame( response.getStatementResult().getFrame(), true, List.of( "hops" ) ) );
                    assertEquals( active ? 2 : 1, integerValue( row.getValues( 0 ) ) );
                }

                Frame bound = connection.execute( namespace, "cypher", """
                        MATCH (a:Stop {name:'A'})-[edges:ROUTE*2..2]->(b:Stop {name:'B'})
                        RETURN length(shortestPath((a)-[edges:ROUTE*..3]->(b))) AS hops
                        """, -1 ).getResult().getFrame();
                assertEquals( 2, integerValue( singleRow( relationalFrame( bound, true, List.of( "hops" ) ) ).getValues( 0 ) ) );

                Frame combined = connection.execute( namespace, "cypher", """
                        MATCH (a:Stop {name:'A'}), (b:Stop {name:'B'}), p = shortestPath((a)-[:ROUTE*..3]->(b))
                        WHERE exists((a)-[:ROUTE]->(b)) AND length(p) > 1
                        RETURN length(p) AS hops
                        """, -1 ).getResult().getFrame();
                assertEquals( 2, integerValue( singleRow( relationalFrame( combined, true, List.of( "hops" ) ) ).getValues( 0 ) ) );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
    }


    @Test
    void evaluatesPatternPredicatesInBooleanExpressions() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( """
                    MATCH (n:Person)
                    RETURN n.name AS name,
                           CASE WHEN (n)-[:KNOWS]->() THEN 1 ELSE 2 END AS branch,
                           NOT (n)-[:KNOWS]->() AS missing,
                           ((n)-[:KNOWS]->()) AND true AS conjunction,
                           false OR (n)-[:KNOWS]->() AS disjunction,
                           ((n)-[:KNOWS]->()) XOR true AS exclusive,
                           ((n)-[:KNOWS]->()) XOR null AS unknown
                    ORDER BY name
                    """, -1 ).getResult().getFrame();
            RelationalFrame people = relationalFrame( frame, true,
                    List.of( "name", "branch", "missing", "conjunction", "disjunction", "exclusive", "unknown" ) );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), projectedNames( people ) );
            for ( int i = 0; i < 3; i++ ) {
                Row row = people.getRows( i );
                boolean hasRelationship = i < 2;
                assertEquals( hasRelationship ? 1 : 2, integerValue( row.getValues( 1 ) ) );
                assertEquals( PolyValueSerializer.serialize( PolyBoolean.of( !hasRelationship ) ), row.getValues( 2 ) );
                assertEquals( PolyValueSerializer.serialize( PolyBoolean.of( hasRelationship ) ), row.getValues( 3 ) );
                assertEquals( PolyValueSerializer.serialize( PolyBoolean.of( hasRelationship ) ), row.getValues( 4 ) );
                assertEquals( PolyValueSerializer.serialize( PolyBoolean.of( !hasRelationship ) ), row.getValues( 5 ) );
                assertTrue( row.getValues( 6 ).hasNull() );
            }
        }
    }


    @Test
    void evaluatesPatternExistsInCorrelatedCallProjection() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame frame = connection.cypher( """
                    MATCH (n:Person)
                    CALL {
                        WITH n
                        RETURN exists((n)-[:KNOWS]->()) AS reaches
                    }
                    RETURN n.name AS name, reaches ORDER BY name
                    """, -1 ).getResult().getFrame();
            RelationalFrame people = relationalFrame( frame, true, List.of( "name", "reaches" ) );
            assertEquals( List.of( "Alice", "Bob", "Charlie" ), projectedNames( people ) );
            assertEquals( List.of( true, true, false ).stream().map( value -> PolyValueSerializer.serialize( PolyBoolean.of( value ) ) ).toList(),
                    people.getRowsList().stream().map( row -> row.getValues( 1 ) ).toList() );
        }
    }


    @Test
    void graphExpressionsSeeEarlierAssignmentsInTheSameSet() throws IOException {
        String namespace = "cyphersetgraphvisibility";
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + namespace, 1 );
            try {
                connection.execute( namespace, "cypher", "CREATE (:Start)-[:ROAD]->(:End)", -1 );
                Frame updated = connection.execute( namespace, "cypher", """
                        MATCH (a:Start)-[r:ROAD]->(b:End)
                        SET b.ready = true, r.weight = 2,
                            b.reachable = exists((a)-[:ROAD {weight:2}]->(b {ready:true})),
                            b.hops = length(shortestPath((a)-[:ROAD* {weight:2}]->(b {ready:true})))
                        RETURN b.reachable AS reachable, b.hops AS hops
                        """, -1 ).getResult().getFrame();
                Frame persisted = connection.execute( namespace, "cypher", """
                        MATCH (:Start)-[:ROAD {weight:2}]->(b:End {ready:true})
                        RETURN b.reachable AS reachable, b.hops AS hops
                        """, -1 ).getResult().getFrame();
                for ( Frame result : List.of( updated, persisted ) ) {
                    Row row = singleRow( relationalFrame( result, true, List.of( "reachable", "hops" ) ) );
                    assertEquals( PolyValueSerializer.serialize( PolyBoolean.of( true ) ), row.getValues( 0 ) );
                    assertEquals( 1, integerValue( row.getValues( 1 ) ) );
                }
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + namespace, 1 );
            }
        }
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
            case BIG_DECIMAL -> new BigDecimal( new BigInteger( value.getBigDecimal().getUnscaledValue().toByteArray() ),
                    value.getBigDecimal().getScale() ).longValueExact();
            // Cross-model graph storage can represent integral properties as strings.
            case STRING -> Long.parseLong( value.getString().getString() );
            default -> throw new AssertionError( "Expected a numeric graph property but got " + value.getValueCase() );
        };
    }


    @Test
    void sortsProjectedExpressionsBeforeApplyingSkipOrLimit() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame distinct = connection.cypher( """
                    UNWIND [1,2] AS x
                    RETURN DISTINCT x * -1 AS neg, 3 - x AS x ORDER BY x * (-1)
                    """, -1 ).getResult().getFrame();
            List<Row> sorted = relationalFrame( distinct, true, List.of( "neg", "x" ) ).getRowsList();
            assertEquals( List.of( List.of( -2L, 1L ), List.of( -1L, 2L ) ), sorted.stream()
                    .map( row -> List.of( integerValue( row.getValues( 0 ) ), integerValue( row.getValues( 1 ) ) ) ).toList() );

            Frame paths = connection.cypher( """
                    MATCH p=(:Person)-[:KNOWS*]->(:Person)
                    RETURN length(p) AS len, count(*) AS matches ORDER BY length(p)
                    """, -1 ).getResult().getFrame();
            List<Row> grouped = relationalFrame( paths, true, List.of( "len", "matches" ) ).getRowsList();
            assertEquals( List.of( List.of( 1L, 2L ), List.of( 2L, 1L ) ), grouped.stream()
                    .map( row -> List.of( integerValue( row.getValues( 0 ) ), integerValue( row.getValues( 1 ) ) ) ).toList() );

            Frame limited = connection.cypher( """
                    UNWIND [1,2] AS x
                    WITH x % 2 AS bucket, sum(x + 1) AS total ORDER BY sum(x + 1) LIMIT 1
                    RETURN bucket,total
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( limited, true, List.of( "bucket", "total" ) ) );
            assertEquals( 1, integerValue( row.getValues( 0 ) ) );
            assertEquals( 2, integerValue( row.getValues( 1 ) ) );
        }
    }


    @Test
    void rejectsAmbiguousGroupingKeysInSortExpressions() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( WRITE_RETURN_NAMESPACE, "cypher", """
                    CREATE (:GroupedSort {age:20})-[:SORT_EDGE]->(:GroupedSort {age:22})-[:SORT_EDGE]->(:GroupedSort {age:27})
                    """, -1 );
            String match = "MATCH (me:GroupedSort)--(you:GroupedSort) ";
            String ambiguous = " ORDER BY me.age + you.age + count(*)";
            for ( String projection : List.of(
                    "RETURN me.age + you.age, count(*) AS cnt" + ambiguous,
                    "WITH me.age + you.age AS age, count(*) AS cnt" + ambiguous + " RETURN age,cnt" ) ) {
                Response error = expect( connection.send( statementRequest( WRITE_RETURN_NAMESPACE, "cypher", match + projection, -1 ) ),
                        Response.TypeCase.ERROR_RESPONSE );
                assertTrue( error.getErrorResponse().getMessage().contains( "AmbiguousAggregationExpression" ) );
                connection.checkTransactions( 0 );
            }

            Frame aliased = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", match + """
                    RETURN me.age + you.age AS age, count(*) AS cnt
                    ORDER BY age + count(*)
                    """, -1 ).getResult().getFrame();
            List<Row> rows = relationalFrame( aliased, true, List.of( "age", "cnt" ) ).getRowsList();
            assertEquals( List.of( List.of( 42L, 2L ), List.of( 49L, 2L ) ), rows.stream()
                    .map( row -> List.of( integerValue( row.getValues( 0 ) ), integerValue( row.getValues( 1 ) ) ) ).toList() );

            Frame simple = connection.execute( WRITE_RETURN_NAMESPACE, "cypher", match + """
                    WITH me.age AS age, count(you.age) AS cnt
                    ORDER BY me.age + count(you.age) DESC LIMIT 2
                    RETURN age,cnt
                    """, -1 ).getResult().getFrame();
            List<Row> limited = relationalFrame( simple, true, List.of( "age", "cnt" ) ).getRowsList();
            assertEquals( List.of( List.of( 27L, 1L ), List.of( 22L, 2L ) ), limited.stream()
                    .map( row -> List.of( integerValue( row.getValues( 0 ) ), integerValue( row.getValues( 1 ) ) ) ).toList() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void sortsUsingTheProjectedVectorMetricAlias() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String query = """
                    UNWIND [[3,4],[1,2]] AS vec
                    WITH vec, 'COSINE' AS metric
                    RETURN vec AS v, '%s' AS metric
                    ORDER BY vector_distance(vec, [0,0], metric)
                    """;
            Frame ordered = connection.cypher( query.formatted( "L2" ), -1 ).getResult().getFrame();
            List<Row> rows = relationalFrame( ordered, true, List.of( "v", "metric" ) ).getRowsList();
            assertEquals( List.of( List.of( 1L, 2L ), List.of( 3L, 4L ) ), rows.stream()
                    .map( row -> row.getValues( 0 ).getList().getValuesList().stream().map( CypherTest::integerValue ).toList() ).toList() );
            assertEquals( List.of( "L2", "L2" ), rows.stream().map( row -> row.getValues( 1 ).getString().getString() ).toList() );

            Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", query.formatted( "UNKNOWN" ), -1 ) ),
                    Response.TypeCase.ERROR_RESPONSE );
            assertTrue( error.getErrorResponse().getMessage().contains( "Unknown distance metric" ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void validatesProjectionAliasesAndOrdersWildcardColumns() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame filtered = connection.cypher( "WITH 1 AS x WITH DISTINCT x + 1 AS y WHERE x+1 = 2 RETURN y", -1 ).getResult().getFrame();
            assertEquals( 2, integerValue( singleRow( relationalFrame( filtered, true, List.of( "y" ) ) ).getValues( 0 ) ) );

            Frame unwound = connection.cypher( """
                    WITH [1,2] AS xs, [3,4] AS ys, [5,6] AS zs
                    UNWIND xs AS x UNWIND ys AS y UNWIND zs AS z
                    RETURN *
                    """, -1 ).getResult().getFrame();
            assertEquals( 8, relationalFrame( unwound, true, List.of( "x", "xs", "y", "ys", "z", "zs" ) ).getRowsCount() );

            for ( String query : List.of( "WITH 1 + 2 RETURN *", "RETURN 1 AS x, 2 AS x" ) ) {
                Response error = expect( connection.send( statementRequest( NAMESPACE, "cypher", query, -1 ) ), Response.TypeCase.ERROR_RESPONSE );
                String message = error.getErrorResponse().getMessage();
                assertTrue( message.contains( "must have an alias" ) || message.contains( "Multiple result columns named" ), message );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void collectsDistinctPathsAndSkipsNulls() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame matched = connection.cypher( """
                    MATCH p=(:Person {name:'Alice'})-[:KNOWS*]->(:Person)
                    UNWIND [1,2] AS duplicate
                    WITH p ORDER BY length(p)
                    RETURN collect(DISTINCT p) AS paths, count(*) AS total
                    """, -1 ).getResult().getFrame();
            Row row = singleRow( relationalFrame( matched, true, List.of( "paths", "total" ) ) );
            assertEquals( List.of( 1, 2 ), row.getValues( 0 ).getList().getValuesList().stream()
                    .map( value -> value.getPath().getEdgesCount() ).toList() );
            assertEquals( 4, integerValue( row.getValues( 1 ) ) );

            Frame missing = connection.cypher( """
                    OPTIONAL MATCH p=(:NoSuchPathStart)-[:KNOWS*]->()
                    RETURN collect(DISTINCT p) AS paths
                    """, -1 ).getResult().getFrame();
            assertEquals( 0, singleRow( relationalFrame( missing, true, List.of( "paths" ) ) )
                    .getValues( 0 ).getList().getValuesCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void returnsNestedCypherMapsOverPrism() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Frame result = connection.cypher( """
                    MATCH p=(:Person {name:'Alice'})-[:KNOWS*1]->(b:Person)
                    RETURN {name:b.name, items:[{path:p, missing:null}], empty:{}} AS details,
                           [{person:b}] AS nested
                    """, -1 ).getResult().getFrame();
            RelationalFrame frame = relationalFrame( result, true, List.of( "details", "nested" ) );
            assertEquals( ProtoPolyType.MAP, frame.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );
            assertEquals( ProtoPolyType.MAP, frame.getColumnMeta( 1 ).getTypeMeta().getArrayMeta().getElementType().getProtoValueType() );
            Row row = singleRow( frame );
            var details = row.getValues( 0 ).getDocument();
            assertEquals( "Bob", details.getEntriesOrThrow( "name" ).getString().getString() );
            assertTrue( details.getEntriesOrThrow( "empty" ).hasDocument() );
            assertEquals( 0, details.getEntriesOrThrow( "empty" ).getDocument().getEntriesCount() );
            var item = details.getEntriesOrThrow( "items" ).getList().getValues( 0 ).getDocument();
            assertTrue( item.getEntriesOrThrow( "missing" ).hasNull() );
            ProtoPath path = item.getEntriesOrThrow( "path" ).getPath();
            assertEquals( 1, path.getEdgesCount() );
            ProtoNode person = row.getValues( 1 ).getList().getValues( 0 ).getDocument().getEntriesOrThrow( "person" ).getNode();
            assertEquals( path.getNodes( 1 ).getId(), person.getId() );
            connection.checkTransactions( 0 );
        }
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


    static Response expect( Response response, Response.TypeCase type ) {
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
