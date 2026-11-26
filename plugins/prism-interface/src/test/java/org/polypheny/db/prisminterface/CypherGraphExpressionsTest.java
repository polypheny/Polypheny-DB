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
import static org.polypheny.db.prisminterface.CypherTest.expect;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;


@Tag("adapter")
class CypherGraphExpressionsTest {

    private static final String NAMESPACE = "cyphergraphexpressions";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
            connection.execute( NAMESPACE, "cypher", """
                    CREATE (:Source {name:'start', missing:null})-[:STEP {score:2}]->(:Middle {name:'middle'})
                                -[:STEP]->(:Target), (:Isolated)
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
    void returnsGraphPropertiesAndMapKeysWithCypherNullSemantics() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row row = singleRow( rows( connection, """
                    MATCH (n:Source)-[r:STEP]->()
                    OPTIONAL MATCH (absent:Absent)
                    RETURN keys(n) AS nk, properties(n) AS np, keys(r) AS rk, properties(r) AS rp,
                           keys({missing:null}) AS mk, properties({missing:null}) AS mp,
                           keys(absent) AS ak, properties(null) AS ap
                    """, "nk", "np", "rk", "rp", "mk", "mp", "ak", "ap" ) );
            assertEquals( Set.of( "name" ), keys( row.getValues( 0 ) ) );
            assertEquals( "start", row.getValues( 1 ).getDocument().getEntriesOrThrow( "name" ).getString().getString() );
            assertEquals( 1, row.getValues( 1 ).getDocument().getEntriesCount() );
            assertEquals( Set.of( "score" ), keys( row.getValues( 2 ) ) );
            assertEquals( 2, integerValue( row.getValues( 3 ).getDocument().getEntriesOrThrow( "score" ) ) );
            assertEquals( Set.of( "missing" ), keys( row.getValues( 4 ) ) );
            assertTrue( row.getValues( 5 ).getDocument().getEntriesOrThrow( "missing" ).hasNull() );
            assertTrue( row.getValues( 6 ).hasNull() );
            assertTrue( row.getValues( 7 ).hasNull() );

            RelationalFrame empty = rows( connection, "MATCH (n:Isolated) UNWIND keys(n) AS k RETURN k", "k" );
            assertEquals( 0, empty.getRowsCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void rejectsInvalidPropertyFunctionArgumentsAtCompileTime() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String expression : List.of( "keys(1)", "properties('Cypher')", "properties([true,false])" ) ) {
                assertQueryError( connection, "RETURN " + expression, "42601", "requires a node, relationship, or map" );
            }
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void reportsInvalidPropertyOwnersAndArithmeticOperandsAtCompileTime() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            assertQueryError( connection, "WITH 1 AS n RETURN n.name", "42804", "Property access requires a map" );
            assertQueryError( connection, "MATCH p=()-[:STEP]->() WHERE p.score=2 RETURN p", "42601", "A path does not expose properties" );
            assertQueryError( connection, "RETURN none(x IN ['text'] WHERE x % 2 = 0)", "42601", "Invalid operand types for MOD" );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void returnsTheFirstListElementAndRejectsNestedBarePatterns() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row row = singleRow( rows( connection, """
                    RETURN head([7,8]) AS first, head([]) AS empty, head([null,7]) AS missing, head(null) AS absent
                    """, "first", "empty", "missing", "absent" ) );
            assertEquals( 7, integerValue( row.getValues( 0 ) ) );
            assertTrue( row.getValuesList().subList( 1, 4 ).stream().allMatch( ProtoValue::hasNull ) );
            assertQueryError( connection, "MATCH (n) SET n.prop = head(nodes(head((n)-[:STEP]->()))).name", "42601",
                    "Use exists(pattern) to evaluate a pattern as a value expression" );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void projectsBoundPathsAndLocalVariables() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row row = singleRow( rows( connection, """
                    MATCH (a:Source), (b:Target)
                    RETURN [p=(a)-[:STEP*]->(b) | p] AS paths,
                           [(a)-[r:STEP]->(m) WHERE r.score=2 | m.name] AS names,
                           [(a)-[:STEP]->(m) WHERE m:Target | m] AS empty
                    """, "paths", "names", "empty" ) );
            assertEquals( 1, row.getValues( 0 ).getList().getValuesCount() );
            ProtoPath path = row.getValues( 0 ).getList().getValues( 0 ).getPath();
            assertEquals( 2, path.getEdgesCount() );
            assertEquals( "Source", path.getNodes( 0 ).getLabels( 0 ) );
            assertEquals( "Target", path.getNodes( 2 ).getLabels( 0 ) );
            assertEquals( List.of( "middle" ), row.getValues( 1 ).getList().getValuesList().stream()
                    .map( value -> value.getString().getString() ).toList() );
            assertEquals( 0, row.getValues( 2 ).getList().getValuesCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void resolvesNestedComprehensionsAndRetainsNullProjections() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row row = singleRow( rows( connection, """
                    MATCH p=(:Source)-[:STEP]->(:Middle)
                    RETURN [x IN nodes(p) | size([(x)-[:STEP]->(:Target) | 1])] AS nested,
                           [(:Middle)-[r:STEP]->() | r.score] AS scores
                    """, "nested", "scores" ) );
            assertEquals( List.of( 0L, 1L ), row.getValues( 0 ).getList().getValuesList().stream().map( CypherTest::integerValue ).toList() );
            assertEquals( 1, row.getValues( 1 ).getList().getValuesCount() );
            assertTrue( row.getValues( 1 ).getList().getValues( 0 ).hasNull() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void looksUpRelationshipEndpointsByDirectionAfterWrites() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row matched = singleRow( rows( connection, """
                    MATCH (:Middle)<-[r:STEP]-(:Source)
                    RETURN startNode(r).name AS first, endNode(r).name AS last, startNode(null) AS absent
                    """, "first", "last", "absent" ) );
            assertEquals( "start", matched.getValues( 0 ).getString().getString() );
            assertEquals( "middle", matched.getValues( 1 ).getString().getString() );
            assertTrue( matched.getValues( 2 ).hasNull() );

            Row created = singleRow( rows( connection, """
                    CREATE (a:CreatedStart {id:2}), (b:CreatedEnd {id:1})
                    MERGE (a)-[r:NEW_STEP]-(b)
                    RETURN startNode(r).id AS first, endNode(r).id AS last
                    """, "first", "last" ) );
            assertEquals( 2, integerValue( created.getValues( 0 ) ) );
            assertEquals( 1, integerValue( created.getValues( 1 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void sortsProjectedRelationshipsAfterOrderingAnEarlierMatch() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            expect( connection.send( Request.newBuilder().setSetClientInfoPropertiesRequest( ClientInfoProperties.newBuilder()
                    .putProperties( PIClientInfoProperties.CYPHER_RESULT_FORMAT, "relational" ) ) ), Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            RelationalFrame result = rows( connection, """
                    MATCH (a)-[r:STEP]->(b)
                    WITH a, r, b, count(*) AS c ORDER BY c
                    MATCH (a)-[r:STEP]->(b)
                    RETURN r AS rel ORDER BY rel.score
                    """, "rel" );
            assertEquals( 2, result.getRowsCount() );
            assertEquals( 2, integerValue( result.getRows( 0 ).getValues( 0 ).getEdge().getPropertiesOrThrow( "score" ) ) );
            assertEquals( 0, result.getRows( 1 ).getValues( 0 ).getEdge().getPropertiesCount() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void retainsIntegerGraphPropertyTypesThroughComputedWrites() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row created = singleRow( rows( connection, """
                    UNWIND [1] AS x
                    CREATE (n:ComputedProperty {x:x})
                    RETURN properties(n).x AS property, [x] AS integers
                    """, "property", "integers" ) );
            assertInteger( created.getValues( 0 ) );
            assertInteger( created.getValues( 1 ).getList().getValues( 0 ) );

            Row loaded = singleRow( rows( connection, "MATCH (n:ComputedProperty) RETURN n.x AS property", "property" ) );
            assertInteger( loaded.getValues( 0 ) );
            connection.checkTransactions( 0 );
        }
    }


    private static void assertInteger( ProtoValue value ) {
        assertTrue( value.hasInteger() || value.hasLong(), value::toString );
        assertEquals( 1, integerValue( value ) );
    }


    private static void assertQueryError( PrismConnection connection, String query, String sqlState, String diagnostic ) throws IOException {
        Response response = expect( connection.send( Request.newBuilder().setExecuteUnparameterizedStatementRequest(
                ExecuteUnparameterizedStatementRequest.newBuilder().setNamespaceName( NAMESPACE ).setLanguageName( "cypher" )
                        .setStatement( query ).setFetchSize( -1 ) ) ), Response.TypeCase.ERROR_RESPONSE );
        ErrorResponse error = response.getErrorResponse();
        assertEquals( sqlState, error.getDetails().getState(), error::getMessage );
        assertTrue( error.getMessage().contains( diagnostic ), error::getMessage );
    }


    private static RelationalFrame rows( PrismConnection connection, String query, String... columns ) throws IOException {
        return relationalFrame( connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame(), true, List.of( columns ) );
    }


    private static Set<String> keys( ProtoValue value ) {
        return value.getList().getValuesList().stream().map( item -> item.getString().getString() ).collect( Collectors.toSet() );
    }

}
