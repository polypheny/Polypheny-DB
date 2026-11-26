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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementResult;


@Tag("adapter")
@Timeout(30)
class CypherSetOrderingTest {

    private static final String NAMESPACE = "cyphersetordering";
    private PrismConnection connection;


    @BeforeEach
    @Timeout(30)
    void resetGraph() throws IOException {
        TestHelper.getInstance();
        connection = new PrismConnection( true, 3 );
        connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
        connection.execute( NAMESPACE, "cypher", "CREATE (:Source {flag:0})-[:ROUTE]->(:Target)", 1 );
    }


    @AfterEach
    @Timeout(30)
    void dropGraph() throws IOException {
        try ( PrismConnection current = connection ) {
            if ( current != null ) {
                current.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
            }
        }
    }


    @Test
    void evaluatesInterleavedAssignmentsAndRepeatedExpressionsInSourceOrder() throws IOException {
        assertOrderedValues( evaluate( """
                MATCH (a:Source), (b:Target)
                SET a.flag = 1,
                    b.first = a.flag,
                    b.found = exists((a {flag:1})-[:ROUTE]->(b)),
                    a.flag = 2,
                    b.second = a.flag,
                    b.missing = exists((a {flag:1})-[:ROUTE]->(b)),
                    a.final = b.first
                RETURN b.first AS first, b.second AS second, b.found AS found, b.missing AS missing, a.final AS finalValue
                """, "first", "second", "found", "missing", "finalValue" ) );
        assertOrderedValues( evaluate( """
                MATCH (a:Source)-[:ROUTE]->(b:Target)
                RETURN b.first AS first, b.second AS second, b.found AS found, b.missing AS missing, a.final AS finalValue
                """, "first", "second", "found", "missing", "finalValue" ) );
        connection.checkTransactions( 0 );
    }


    @Test
    void labelsAndMapUpdatesAreVisibleAcrossAliases() throws IOException {
        assertAliasValues( evaluate( """
                MATCH (a:Source), (b:Target)
                WITH a AS origin, a AS alias, b
                SET origin:Ready,
                    b.ready = exists((alias:Ready)-[:ROUTE]->(b)),
                    alias += {eligible:true},
                    b.distance = length(shortestPath((origin {eligible:true})-[:ROUTE*]->(b))),
                    origin.eligible = false,
                    b.later = length(shortestPath((alias {eligible:true})-[:ROUTE*]->(b)))
                RETURN b.ready AS ready, b.distance AS distance, b.later AS later, alias.eligible AS eligible
                """, "ready", "distance", "later", "eligible" ) );
        assertAliasValues( evaluate( """
                MATCH (a:Source:Ready)-[:ROUTE]->(b:Target)
                RETURN b.ready AS ready, b.distance AS distance, b.later AS later, a.eligible AS eligible
                """, "ready", "distance", "later", "eligible" ) );
        connection.checkTransactions( 0 );
    }


    @Test
    void preservesWriteOnlyCountsAndNullTargets() throws IOException {
        StatementResult empty = connection.execute( NAMESPACE, "cypher", """
                MATCH (a:Source)
                OPTIONAL MATCH (a)-[:ABSENT]->(n)
                SET n.p = exists((a)-[:ROUTE]->()), n.q = 3
                """, 1 ).getResult();
        assertFalse( empty.hasFrame() );
        assertEquals( 0, empty.getScalar() );

        StatementResult written = connection.execute( NAMESPACE, "cypher", """
                MATCH (a:Source), (b:Target)
                OPTIONAL MATCH (a)-[:ABSENT]->(n)
                UNWIND [1,2] AS step
                WITH a, b, n, step ORDER BY step
                SET a.flag = step,
                    n.p = true,
                    b.first = a.flag,
                    a.flag = step + 1,
                    b.second = a.flag
                """, 1 ).getResult();
        assertFalse( written.hasFrame() );
        assertEquals( 2, written.getScalar() );
        Row row = evaluate( """
                MATCH (a:Source)-[:ROUTE]->(b:Target)
                RETURN a.flag AS flag, b.first AS first, b.second AS second
                """, "flag", "first", "second" );
        assertEquals( 3, integerValue( row.getValues( 0 ) ) );
        assertEquals( 2, integerValue( row.getValues( 1 ) ) );
        assertEquals( 3, integerValue( row.getValues( 2 ) ) );
        connection.checkTransactions( 0 );
    }


    @Test
    void keepsAssignmentOrderWhenReusingAPreparedStatement() throws IOException {
        int statement = prepareNamedCypher( connection, NAMESPACE, """
                MATCH (a:Source), (b:Target)
                WITH a, a AS alias, b
                SET alias.flag = $value,
                    b.before = exists((a {flag:$value})-[:ROUTE]->(b)),
                    a.flag = $value + 1,
                    b.after = exists((alias {flag:$value})-[:ROUTE]->(b)),
                    alias.copy = a.flag
                RETURN b.before AS before, b.after AS after, alias.copy AS copied
                """ ).getStatementId();

        for ( int value : List.of( 4, 8 ) ) {
            ProtoValue parameter = ProtoValue.newBuilder().setInteger( ProtoInteger.newBuilder().setInteger( value ) ).build();
            Response response = expect( executeNamedCypher( connection, statement, Map.of( "value", parameter ) ), Response.TypeCase.STATEMENT_RESULT );
            Row row = singleRow( relationalFrame( response.getStatementResult().getFrame(), true, List.of( "before", "after", "copied" ) ) );
            assertTrue( row.getValues( 0 ).getBoolean().getBoolean() );
            assertTrue( row.getValues( 1 ).hasBoolean() );
            assertFalse( row.getValues( 1 ).getBoolean().getBoolean() );
            assertEquals( value + 1, integerValue( row.getValues( 2 ) ) );
            connection.checkTransactions( 0 );
        }
    }


    private Row evaluate( String query, String... columns ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return singleRow( relationalFrame( frame, true, List.of( columns ) ) );
    }


    private static void assertOrderedValues( Row row ) {
        assertEquals( 1, integerValue( row.getValues( 0 ) ) );
        assertEquals( 2, integerValue( row.getValues( 1 ) ) );
        assertTrue( row.getValues( 2 ).getBoolean().getBoolean() );
        assertTrue( row.getValues( 3 ).hasBoolean() );
        assertFalse( row.getValues( 3 ).getBoolean().getBoolean() );
        assertEquals( 1, integerValue( row.getValues( 4 ) ) );
    }


    private static void assertAliasValues( Row row ) {
        assertTrue( row.getValues( 0 ).getBoolean().getBoolean() );
        assertEquals( 1, integerValue( row.getValues( 1 ) ) );
        assertTrue( row.getValues( 2 ).hasNull() );
        assertTrue( row.getValues( 3 ).hasBoolean() );
        assertFalse( row.getValues( 3 ).getBoolean().getBoolean() );
    }

}
