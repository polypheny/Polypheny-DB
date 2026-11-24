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
import static org.polypheny.db.prisminterface.CypherTest.executeNamedCypher;
import static org.polypheny.db.prisminterface.CypherTest.expect;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.prepareNamedCypher;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;


@Tag("adapter")
class CypherPatternPropertyEvaluationTest {

    private static final String NAMESPACE = "cypherpatternpropertyevaluation";


    @BeforeAll
    static void createGraph() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
            connection.execute( NAMESPACE, "cypher", """
                    CREATE (a:A {divisor:0, mark:7})-[:R {score:2, other:4}]->(:B {mark:9}),
                           (a)-[:CHAIN {score:2, group:1, other:4}]->()-[:CHAIN {score:3, group:1, other:4}]->(),
                           (a)-[:LOOP]->(a)
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
    void skipsPropertyExpressionsWithoutAMatchingRelationship() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row values = row( connection, """
                    MATCH (a:A) OPTIONAL MATCH (missing:Missing)
                    WITH a, missing, a.divisor AS divisor
                    RETURN exists((a)-[:R*0 {score:1/divisor}]->(a)) AS zero,
                           exists((a)-[:MISSING {score:1/divisor}]->()) AS noEdge,
                           exists((missing)-[:R {score:1/divisor}]->()) AS noStart
                    """, "zero", "noEdge", "noStart" );
            assertBooleans( values, true, false, false );

            Row filtered = row( connection, """
                    MATCH (a:A) WITH a, a.divisor AS divisor
                    WHERE (a)-[:R*0 {score:1/divisor}]->(a) AND NOT (a)-[:MISSING {score:1/divisor}]->()
                    RETURN count(a) AS matches
                    """, "matches" );
            assertEquals( 1, integerValue( filtered.getValues( 0 ) ) );

            Row matched = row( connection, """
                    MATCH (a:A) WITH a, a.divisor AS divisor
                    MATCH p=(a)-[:R*0 {score:1/divisor}]->(a)
                    OPTIONAL MATCH (a)-[:MISSING {score:1/divisor}]->(missing)
                    RETURN length(p) AS hops, missing AS missing
                    """, "hops", "missing" );
            assertEquals( 0, integerValue( matched.getValues( 0 ) ) );
            assertTrue( matched.getValues( 1 ).hasNull() );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void evaluatesPropertiesWhenAConcreteCandidateNeedsThem() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String predicate : List.of( "RETURN exists((a)-[:R*1 {score:1/divisor}]->()) AS found",
                    "WHERE (a)-[:R*1 {score:1/divisor}]->() RETURN a",
                    "RETURN exists((a)-[:R*1 {score:2, other:1/divisor}]->()) AS found",
                    "WHERE (a)-[:R*1 {other:1/divisor, score:2}]->() RETURN a" ) ) {
                Response response = expect( connection.send( Request.newBuilder().setExecuteUnparameterizedStatementRequest(
                                ExecuteUnparameterizedStatementRequest.newBuilder().setNamespaceName( NAMESPACE ).setLanguageName( "cypher" )
                                        .setStatement( "MATCH (a:A) WITH a, a.divisor AS divisor " + predicate ).setFetchSize( -1 ) ) ),
                        Response.TypeCase.ERROR_RESPONSE );
                String message = response.getErrorResponse().getMessage();
                assertTrue( message.contains( "by zero" ), message );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void filtersLiteralPropertiesBeforeComputingOtherEntries() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            String start = "MATCH (a:A) WITH a, a.divisor AS divisor ";
            for ( String map : List.of( "{score:999, other:1/divisor}", "{other:1/divisor, score:999}" ) ) {
                String pattern = "(a)-[:R " + map + "]->()";
                assertBooleans( row( connection, start + "RETURN exists(" + pattern + ") AS found", "found" ), false );
                Row filtered = row( connection, start + "WHERE " + pattern + " RETURN count(a) AS matches", "matches" );
                assertEquals( 0, integerValue( filtered.getValues( 0 ) ) );
                Row matched = row( connection, start + "MATCH " + pattern + " RETURN count(a) AS matches", "matches" );
                assertEquals( 0, integerValue( matched.getValues( 0 ) ) );
            }

            Row candidates = row( connection, start + """
                    RETURN exists((a {mark:8, other:1/divisor})-[:R]->()) AS sourceMismatch,
                           exists((a)-[:R]->(:B {other:1/divisor, mark:8})) AS targetMismatch,
                           exists((a)-[:R {score:null, other:1/divisor}]->()) AS nullMismatch,
                           exists((a)-[:R*0 {score:999, other:1/divisor}]->(a)) AS zeroHops,
                           exists((a)-[:CHAIN*2 {score:2, other:1/divisor}]->()) AS secondHopMismatch,
                           exists((a)-[:CHAIN*2 {group:1, other:a.mark - 3}]->()) AS allHopsMatch,
                           exists((a)-[:CHAIN*2 {group:1, score:a.mark - 5}]->()) AS secondHopComputedMismatch
                    """, "sourceMismatch", "targetMismatch", "nullMismatch", "zeroHops", "secondHopMismatch", "allHopsMatch", "secondHopComputedMismatch" );
            assertBooleans( candidates, false, false, false, true, false, true, false );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void usesCurrentPropertyValuesWhenExecutingTheSamePreparedStatement() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int statement = prepareNamedCypher( connection, NAMESPACE, """
                    MATCH (a:A) WITH a, a.divisor AS divisor
                    RETURN exists((a)-[:R {score:999, other:1/divisor}]->()) AS skipped,
                           exists((a)-[:R {score:$score, other:a.mark - 3}]->()) AS mixed,
                           exists((a)-[:R {score:$score, other:4}]->()) AS literalMap
                    """ ).getStatementId();
            for ( int score : List.of( 2, 999, 2 ) ) {
                ProtoValue value = ProtoValue.newBuilder().setInteger( ProtoInteger.newBuilder().setInteger( score ) ).build();
                Response result = expect( executeNamedCypher( connection, statement, Map.of( "score", value ) ), Response.TypeCase.STATEMENT_RESULT );
                Row values = singleRow( relationalFrame( result.getStatementResult().getFrame(), true, List.of( "skipped", "mixed", "literalMap" ) ) );
                assertBooleans( values, false, score == 2, score == 2 );
                connection.checkTransactions( 0 );
            }
        }
    }


    @Test
    void preservesPropertyChecksForRepeatedBindingsAndAnonymousElements() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row values = row( connection, """
                    MATCH (a:A) WITH a, a.divisor AS divisor
                    RETURN exists((a {mark:7})-[:LOOP]->(a {mark:7})) AS repeatedMatch,
                           exists((a {mark:7})-[:LOOP]->(a {mark:8})) AS repeatedMismatch,
                           exists((a {mark:8})-[:LOOP]->(a {mark:1/divisor})) AS skippedSecondCheck,
                           exists((a)-[:R {score:a.mark - 5}]->(:B {mark:a.mark + 2})) AS anonymousMatch,
                           exists((a)-[:R {score:a.mark - 5}]->(:B {mark:a.mark + 1})) AS anonymousMismatch
                    """, "repeatedMatch", "repeatedMismatch", "skippedSecondCheck", "anonymousMatch", "anonymousMismatch" );
            assertBooleans( values, true, false, false, true, false );
            connection.checkTransactions( 0 );
        }
    }


    @Test
    void capturesQuantifierVariablesAndTheGraphInsideDeferredProperties() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Row quantified = row( connection, """
                    MATCH (a:A) WITH {items:[a]} AS box
                    RETURN any(n IN box.items WHERE exists((n)-[:R {score:n.mark - 5}]->(:B {mark:n.mark + 2}))) AS found
                    """, "found" );
            assertBooleans( quantified, true );

            Row merged = row( connection, """
                    MATCH (a:A)
                    MERGE (n:Report {present:exists((a)-[:R {score:CASE WHEN exists((a)-[:R]->()) THEN a.mark - 5 ELSE 0 END}]->())})
                    ON CREATE SET n.checked = exists((a)-[:R {score:a.mark - 5}]->(:B {mark:a.mark + 2}))
                    RETURN n.present AS present, n.checked AS checked
                    """, "present", "checked" );
            assertBooleans( merged, true, true );
            connection.checkTransactions( 0 );
        }
    }


    private static Row row( PrismConnection connection, String query, String... columns ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return singleRow( relationalFrame( frame, true, List.of( columns ) ) );
    }


    private static void assertBooleans( Row row, boolean... expected ) {
        assertEquals( expected.length, row.getValuesCount() );
        for ( int i = 0; i < expected.length; i++ ) {
            ProtoValue value = row.getValues( i );
            assertTrue( value.hasBoolean() );
            assertEquals( expected[i], value.getBoolean().getBoolean() );
        }
    }

}
