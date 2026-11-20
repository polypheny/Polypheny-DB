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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherExpressionSemanticsTest extends CypherTestTemplate {

    @BeforeEach
    void createBranchingGraph() {
        tearDown();
        createGraph();
        execute( """
                CREATE (a:Origin {name:'A'})-[:LINK {year:1}]->(:Destination {name:'B'}),
                    (a)-[:LINK {year:2}]->(:Destination {name:'C'})
                """ );
    }


    @Test
    void missingPropertiesOnlyAffectTheirOwnBooleanBranch() {
        String match = "MATCH (:Origin)-[*]->(b:Destination) WHERE ";
        for ( String predicate : List.of( "b.missing = 1 OR b.name = 'B'",
                "b.name = 'B' OR b.missing = 1", "NOT (b.missing = 1 AND b.name = 'C')" ) ) {
            GraphResult result = execute( match + predicate + " RETURN b.name AS name" );
            assertEquals( 1, result.getData().length, predicate );
            assertEquals( "B", cell( result, 0, 0 ).asString().getValue(), predicate );
        }
        assertEquals( 0, execute( match + "b.missing = 1 AND b.name = 'B' RETURN b" ).getData().length );
    }


    @Test
    void membershipAndLastRespectEmptyListsAndNulls() {
        GraphResult membership = execute( """
                MATCH (:Origin)
                RETURN 1 IN [null, 1] AS present, 2 IN [1, null] AS unknown,
                       null IN [] AS empty, 2 IN [1, '2'] AS absent, 2 IN null AS missing
                """ );
        assertTrue( cell( membership, 0, 0 ).asBoolean().value );
        assertNull( cell( membership, 0, 1 ) );
        assertEquals( false, cell( membership, 0, 2 ).asBoolean().value );
        assertEquals( false, cell( membership, 0, 3 ).asBoolean().value );
        assertNull( cell( membership, 0, 4 ) );

        GraphResult last = execute( """
                MATCH (:Origin)-[rs:LINK*0..1]->(b)
                RETURN b.name AS endpoint, last(rs) AS relationship ORDER BY endpoint
                """ );
        assertEquals( 3, last.getData().length );
        assertNull( cell( last, 0, 1 ) );
        assertEquals( 1, cell( last, 1, 1 ).asEdge().properties.get( PolyString.of( "year" ) ).asNumber().intValue() );
        assertEquals( 2, cell( last, 2, 1 ).asEdge().properties.get( PolyString.of( "year" ) ).asNumber().intValue() );
    }


    @Test
    void checksListOperandTypesBeforeEvaluatingRows() {
        for ( String prefix : List.of( "RETURN ", "MATCH (missing:Absent) RETURN ", "MATCH (found:Origin) RETURN " ) ) {
            for ( String operand : List.of( "true", "123", "123.4", "'foo'", "{x: []}" ) ) {
                assertQueryError( prefix + "1 IN " + operand + " AS result", "IN requires a list" );
                assertQueryError( prefix + "last(" + operand + ") AS result", "last() requires a list" );
            }
        }
        assertQueryError( "MATCH (missing:Absent) WITH 3 AS operand RETURN last(operand) AS result", "last() requires a list" );
        assertQueryError( "RETURN last() AS result", "last() requires exactly one argument" );
    }


    @Test
    void evaluatesListExpressionsWithoutMatchAndAcceptsNullOperands() {
        GraphResult expressionOnly = execute( "RETURN 1 IN [1] AS member, last([1, 2]) AS tail, last([]) AS empty" );
        assertEquals( 1, expressionOnly.getData().length );
        assertTrue( cell( expressionOnly, 0, 0 ).asBoolean().value );
        assertEquals( 2, cell( expressionOnly, 0, 1 ).asNumber().intValue() );
        assertNull( cell( expressionOnly, 0, 2 ) );

        GraphResult nullOperand = execute( "WITH null AS value RETURN 1 IN value AS member, last(value) AS tail" );
        assertEquals( 1, nullOperand.getData().length );
        assertNull( cell( nullOperand, 0, 0 ) );
        assertNull( cell( nullOperand, 0, 1 ) );
    }


    @Test
    void ordersPathsAndRelationshipListsLexicographically() {
        String match = """
                MATCH p=(:Origin)-[rs*]->(:Destination {name:'B'}),
                      q=(:Origin)-[ts*]->(:Destination {name:'C'})
                """;
        GraphResult comparisons = execute( match + """
                RETURN p < q AS pathForward, q < p AS pathReverse,
                       rs < ts AS listForward, ts < rs AS listReverse
                """ );
        boolean forward = cell( comparisons, 0, 0 ).asBoolean().value;
        assertEquals( !forward, cell( comparisons, 0, 1 ).asBoolean().value );
        assertEquals( forward, cell( comparisons, 0, 2 ).asBoolean().value );
        assertEquals( !forward, cell( comparisons, 0, 3 ).asBoolean().value );

        GraphResult paths = execute( "MATCH p=(:Origin)-[*]->(:Destination) RETURN p ORDER BY p" );
        List<PolyPath> ordered = Arrays.stream( paths.getData() ).map( row -> PolyValue.fromTypedJson( row[0], PolyValue.class ).asPath() ).toList();
        assertEquals( 2, ordered.size() );
        assertTrue( ordered.get( 0 ).compareTo( ordered.get( 1 ) ) < 0 );

        GraphResult lists = execute( """
                MATCH (:Origin)
                RETURN [2] > [1, 9] AS prefixFirst, [1] < [1, null] AS shorter,
                       [1, 2] >= [1, null] AS unknown
                """ );
        assertTrue( cell( lists, 0, 0 ).asBoolean().value );
        assertTrue( cell( lists, 0, 1 ).asBoolean().value );
        assertNull( cell( lists, 0, 2 ) );
    }


    @Test
    void validatesLiteralMapsUsedAsRelationshipOrPathBindings() {
        for ( String map : List.of( "{x: 1}", "{x: []}" ) ) {
            for ( String match : List.of( "MATCH ()-[r]-() RETURN r", "MATCH r=()-[]-() RETURN r" ) ) {
                GraphResult invalid = CypherConnection.executeGetResponse( "WITH " + map + " AS r " + match );
                assertNotNull( invalid.getError() );
                assertTrue( invalid.getError().contains( "Variable 'r' has incompatible bindings" ), invalid.getError() );
            }
        }
    }


    private static void assertQueryError( String query, String expected ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.contains( expected ), error );
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        String json = result.getData()[row][column];
        return json == null ? null : PolyValue.fromTypedJson( json, PolyValue.class );
    }

}
