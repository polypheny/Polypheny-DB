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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
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


    private static GraphResult execute( String query ) {
        return CypherTestTemplate.execute( query, NAMESPACE );
    }


    private static String text( String cell ) {
        return PolyValue.fromTypedJson( cell, PolyValue.class ).asString().getValue();
    }

}
