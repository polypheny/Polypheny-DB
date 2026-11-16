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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;

@Tag("adapter")
class OptionalMatchTest {

    private static final String NAMESPACE = "optionalmatch";


    @BeforeAll
    static void setUp() {
        TestHelper.getInstance();
        CypherTestTemplate.execute( "CREATE DATABASE " + NAMESPACE );
        execute( "CREATE (:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->"
                + "(:Person {name: 'Bob'})-[:KNOWS {since: 2021}]->(:Person {name: 'Charlie'})" );
    }


    @AfterAll
    static void tearDown() {
        CypherTestTemplate.deleteData( NAMESPACE );
    }


    @Test
    void firstOptionalMatchProvidesOneRowWhenThereAreNoMatches() {
        GraphResult result = execute( "OPTIONAL MATCH (n:Missing) RETURN n, n.name AS name" );

        assertEquals( 1, result.getData().length );
        assertNull( result.getData()[0][0] );
        assertNull( result.getData()[0][1] );
    }


    @Test
    void anonymousOptionalPatternsRetainTheirRowMultiplicity() {
        assertEquals( 1, execute( "OPTIONAL MATCH (:Missing) RETURN 1 AS result" ).getData().length );
        assertEquals( 3, execute( "OPTIONAL MATCH (:Person) RETURN 1 AS result" ).getData().length );
    }


    @Test
    void keepsBindingsAndReturnsNullablePathsAndRelationships() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH p = (n)-[r:KNOWS]->(m) "
                + "RETURN n.name AS name, p, r, m.name AS friend ORDER BY name" );

        assertEquals( 3, result.getData().length );
        String[][] rows = result.getData();
        assertEquals( "Alice", text( rows[0][0] ) );
        assertEquals( "Bob", text( rows[0][3] ) );
        assertTrue( PolyValue.fromTypedJson( rows[0][1], PolyValue.class ).isPath() );
        assertTrue( PolyValue.fromTypedJson( rows[0][2], PolyValue.class ).isEdge() );
        assertEquals( "Charlie", text( rows[2][0] ) );
        assertNull( rows[2][1] );
        assertNull( rows[2][2] );
        assertNull( rows[2][3] );
    }


    @Test
    void whereFiltersCandidatesBeforeSupplyingTheMissingOptionalRow() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS]->(m) "
                + "WHERE r.since = 2020 AND n.name = 'Alice' "
                + "RETURN n.name AS name, m.name AS friend ORDER BY name" );

        assertEquals( 3, result.getData().length );
        String[][] rows = result.getData();
        assertEquals( "Bob", text( rows[0][1] ) );
        assertEquals( "Bob", text( rows[1][0] ) );
        assertNull( rows[1][1] );
        assertEquals( "Charlie", text( rows[2][0] ) );
        assertNull( rows[2][1] );
    }


    @Test
    void chainsOptionalPatternsWithoutRebindingMissingVariables() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) "
                + "OPTIONAL MATCH (m)-[:KNOWS]->(o) RETURN n.name AS name, o.name AS friend ORDER BY name" );

        assertEquals( 3, result.getData().length );
        String[][] rows = result.getData();
        assertEquals( "Charlie", text( rows[0][1] ) );
        assertNull( rows[1][1] );
        assertNull( rows[2][1] );
    }


    @Test
    void canFilterForANullOptionalBindingInAFollowingClause() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) "
                + "WITH n, m WHERE m IS NULL RETURN n.name AS name" );

        assertEquals( 1, result.getData().length );
        assertEquals( "Charlie", text( result.getData()[0][0] ) );
    }


    @Test
    void optionalClauseRequiresAllOfItsCommaSeparatedPatterns() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m), (m)-[:KNOWS]->(o) "
                + "RETURN n.name AS name, m.name AS friend, o.name AS secondFriend ORDER BY name" );

        assertEquals( 3, result.getData().length );
        String[][] rows = result.getData();
        assertEquals( "Bob", text( rows[0][1] ) );
        assertEquals( "Charlie", text( rows[0][2] ) );
        assertNull( rows[1][1] );
        assertNull( rows[1][2] );
        assertNull( rows[2][1] );
        assertNull( rows[2][2] );
    }


    @Test
    void relationshipsCannotBeReusedWithinOneOptionalClause() {
        String match = "MATCH (n:Person {name: 'Alice'}) ";
        String first = "(n)-[:KNOWS]->(first)";
        String second = "(n)-[:KNOWS]->(second)";
        String projection = " RETURN n.name AS name, first.name AS firstFriend, second.name AS secondFriend";
        String[][] together = execute( match + "OPTIONAL MATCH " + first + ", " + second + projection ).getData();
        String[][] separately = execute( match + "OPTIONAL MATCH " + first + " OPTIONAL MATCH " + second + projection ).getData();

        assertEquals( 1, together.length );
        assertEquals( "Alice", text( together[0][0] ) );
        assertNull( together[0][1] );
        assertNull( together[0][2] );
        assertEquals( 1, separately.length );
        assertEquals( "Bob", text( separately[0][1] ) );
        assertEquals( "Bob", text( separately[0][2] ) );
    }


    @Test
    void retainingAPathDoesNotKeepItsVariablesInScope() {
        String[][] rows = execute( "MATCH p = (a:Person)-[:KNOWS]->(b) WITH p OPTIONAL MATCH (a:Person) RETURN a.name AS name ORDER BY name" ).getData();

        assertEquals( List.of( "Alice", "Alice", "Bob", "Bob", "Charlie", "Charlie" ),
                Arrays.stream( rows ).map( row -> text( row[0] ) ).toList() );
    }


    @Test
    void aRepeatedNodeVariableRequiresOneIdentity() {
        String[][] rows = execute( "MATCH (a:Person) OPTIONAL MATCH p = (a)-[:KNOWS]->(a) RETURN a.name AS name, p ORDER BY name" ).getData();

        assertEquals( 3, rows.length );
        for ( String[] row : rows ) {
            assertNull( row[1] );
        }
    }


    @Test
    void namedSingleNodePathsKeepTheNodeBindingAndPathType() {
        String[][] rows = execute( "MATCH (n:Person) OPTIONAL MATCH p = (n) RETURN n.name AS name, p ORDER BY name" ).getData();

        assertEquals( 3, rows.length );
        for ( String[] row : rows ) {
            var path = PolyValue.fromTypedJson( row[1], PolyValue.class ).asPath();
            assertEquals( 0, path.getEdges().size() );
            assertEquals( 1, path.getNodes().size() );
            assertEquals( text( row[0] ), path.getNodes().get( 0 ).getProperties().get( PolyString.of( "name" ) ).asString().getValue() );
        }
        String[][] first = execute( "OPTIONAL MATCH p = (n:Person {name: 'Alice'}) RETURN p, n.name AS name" ).getData();
        assertEquals( 1, first.length );
        assertEquals( "Alice", text( first[0][1] ) );
    }


    @Test
    void inlineNodeAndRelationshipPredicatesFilterOptionalCandidates() {
        String[][] missing = execute( "OPTIONAL MATCH (n:Person WHERE n.name = 'Missing') RETURN n" ).getData();
        assertEquals( 1, missing.length );
        assertNull( missing[0][0] );

        String[][] rows = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[r:KNOWS WHERE r.since = 2021]->(m) "
                + "RETURN n.name AS name, m.name AS friend ORDER BY name" ).getData();
        assertEquals( 3, rows.length );
        assertNull( rows[0][1] );
        assertEquals( "Charlie", text( rows[1][1] ) );
        assertNull( rows[2][1] );
    }


    @Test
    void starProjectionExposesVariablesWithoutTheAnonymousPathColumn() {
        GraphResult result = execute( "MATCH (a:Person)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:KNOWS]->(c) RETURN *" );

        assertEquals( List.of( "a", "b", "c" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        assertEquals( 2, result.getData().length );
    }


    @Test
    void groupsByGraphBindingAndCountsOnlyNonNullNeighbors() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) "
                + "RETURN count(m) AS neighbors, n AS person, count(*) AS rows" );
        assertEquals( List.of( "neighbors", "person", "rows" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        assertEquals( 3, result.getData().length );

        Map<String, Integer> neighbors = new HashMap<>();
        for ( String[] row : result.getData() ) {
            var person = PolyValue.fromTypedJson( row[1], PolyValue.class ).asNode();
            String name = person.getProperties().get( PolyString.of( "name" ) ).asString().getValue();
            neighbors.put( name, PolyValue.fromTypedJson( row[0], PolyValue.class ).asNumber().intValue() );
            assertEquals( 1, PolyValue.fromTypedJson( row[2], PolyValue.class ).asNumber().intValue() );
        }
        assertEquals( Map.of( "Alice", 1, "Bob", 1, "Charlie", 0 ), neighbors );
    }


    @Test
    void wildcardReturnsAndWithClausesGroupByEveryVisibleBinding() {
        String optional = "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) ";
        GraphResult returned = execute( optional + "RETURN *, count(m) AS neighbors" );
        GraphResult forwarded = execute( optional + "WITH *, count(m) AS neighbors RETURN n, m, neighbors" );

        for ( GraphResult result : List.of( returned, forwarded ) ) {
            assertEquals( List.of( "n", "m", "neighbors" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
            assertEquals( 3, result.getData().length );
            Map<String, Integer> neighbors = new HashMap<>();
            for ( String[] row : result.getData() ) {
                String person = nodeName( row[0] );
                neighbors.put( person, number( row[2] ) );
                switch ( person ) {
                    case "Alice" -> assertEquals( "Bob", nodeName( row[1] ) );
                    case "Bob" -> assertEquals( "Charlie", nodeName( row[1] ) );
                    case "Charlie" -> assertNull( row[1] );
                    default -> throw new AssertionError( person );
                }
            }
            assertEquals( Map.of( "Alice", 1, "Bob", 1, "Charlie", 0 ), neighbors );
        }
    }


    @Test
    void projectedAliasDoesNotShadowAnAggregateArgument() {
        GraphResult result = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) "
                + "RETURN n.name AS m, count(m) AS matches ORDER BY m" );

        assertEquals( List.of( "m", "matches" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        String[][] rows = result.getData();
        assertEquals( 3, rows.length );
        assertEquals( List.of( "Alice", "Bob", "Charlie" ), Arrays.stream( rows ).map( row -> text( row[0] ) ).toList() );
        assertEquals( List.of( 1, 1, 0 ), Arrays.stream( rows ).map( row -> number( row[1] ) ).toList() );
    }


    @Test
    void aggregateAliasesDoNotResolveToInputColumns() {
        String[][] rows = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) "
                + "RETURN count(n) AS m, count(m) AS n, count(*) AS allRows" ).getData();

        assertEquals( 1, rows.length );
        assertEquals( List.of( 3, 2, 3 ), Arrays.stream( rows[0] ).map( OptionalMatchTest::number ).toList() );
    }


    @Test
    void distinctCountDeduplicatesOptionalMatchesAndIgnoresNull() {
        execute( "CREATE (:Fan {name:'Left'})-[:FOLLOWS]->(:Fan {name:'Right'})<-[:FOLLOWS]-(:Fan {name:'Left'})" );

        GraphResult result = execute( "MATCH (n:Fan) OPTIONAL MATCH (n)-[:FOLLOWS]->(m) "
                + "RETURN n.name AS person, count(m) AS total, count(DISTINCT m) AS uniqueTargets, count(*) AS rows" );

        assertEquals( List.of( "person", "total", "uniqueTargets", "rows" ), Arrays.stream( result.getHeader() ).map( field -> field.name ).toList() );
        assertEquals( 2, result.getData().length );
        Map<String, List<Integer>> counts = new HashMap<>();
        for ( String[] row : result.getData() ) {
            counts.put( text( row[0] ), List.of( number( row[1] ), number( row[2] ), number( row[3] ) ) );
        }
        assertEquals( Map.of( "Left", List.of( 2, 1, 2 ), "Right", List.of( 0, 0, 1 ) ), counts );

        String[][] direct = execute( "MATCH (:Fan)-[:FOLLOWS]->(m:Fan) RETURN count(DISTINCT m) AS uniqueTargets" ).getData();
        assertEquals( 1, direct.length );
        assertEquals( 1, number( direct[0][0] ) );
    }


    @Test
    void groupsByAnUnnamedExpression() {
        String[][] rows = execute( "MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN 42, count(m) AS neighbors" ).getData();
        assertEquals( 1, rows.length );
        assertEquals( 42, number( rows[0][0] ) );
        assertEquals( 2, number( rows[0][1] ) );
    }


    private static int number( String cell ) {
        return PolyValue.fromTypedJson( cell, PolyValue.class ).asNumber().intValue();
    }


    private static String nodeName( String cell ) {
        var node = PolyValue.fromTypedJson( cell, PolyValue.class ).asNode();
        return node.getProperties().get( PolyString.of( "name" ) ).asString().getValue();
    }


    private static GraphResult execute( String query ) {
        return CypherTestTemplate.execute( query, NAMESPACE );
    }


    private static String text( String cell ) {
        return PolyValue.fromTypedJson( cell, PolyValue.class ).asString().getValue();
    }

}
