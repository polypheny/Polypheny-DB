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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;


class CypherForeachTest extends CypherTestTemplate {

    @BeforeEach
    void resetGraph() {
        tearDown();
        createGraph();
    }


    @Test
    void createsRelationshipsForEveryListElementAndReportsWrites() {
        execute( "CREATE (:Person {name:'John Doe'})" );
        long updates = execute( """
                MATCH (p:Person {name:'John Doe'})
                FOREACH (name IN ['Jane', 'Bob', 'Alice'] | CREATE (p)-[:KNOWS]->(:Person {name:name}))
                """ ).affectedTuples;
        assertEquals( 3, updates );
        GraphResult rows = execute( "MATCH (:Person {name:'John Doe'})-[:KNOWS]->(p:Person) RETURN p.name ORDER BY p.name" );
        assertEquals( 3, rows.getData().length );
        assertEquals( List.of( "Alice", "Bob", "Jane" ),
                Arrays.stream( rows.getData() ).map( row -> PolyValue.fromTypedJson( row[0], PolyValue.class ).asString().value ).toList() );
    }


    @Test
    void preservesOuterRowsWithEmptyNullAndRepeatedElements() {
        assertEquals( 3, scalar( """
                UNWIND [[], null, [1, 1, 2]] AS items
                FOREACH (item IN items | CREATE (:Item {value:item}))
                RETURN count(*)
                """ ) );
        assertEquals( 3, scalar( "MATCH (:Item) RETURN count(*)" ) );
        assertEquals( 2, scalar( "MATCH (:Item {value:1}) RETURN count(*)" ) );
        assertEquals( 1, execute( "FOREACH (item IN [null] | CREATE (:Nullable {value:item}))" ).affectedTuples );
        assertEquals( 0, scalar( "MATCH (n:Nullable) RETURN count(n.value)" ) );
        assertEquals( 0, execute( "FOREACH (item IN null | CREATE (:Unexpected))" ).affectedTuples );
        assertEquals( 0, scalar( "MATCH (:Missing) FOREACH (item IN [1] | CREATE (:Unexpected)) RETURN count(*)" ) );
        assertEquals( 0, scalar( "MATCH (:Unexpected) RETURN count(*)" ) );
    }


    @Test
    void executesTheWholeBodyBeforeAdvancingToTheNextElement() {
        execute( "CREATE (:Counter {value:0})" );
        assertEquals( 8, scalar( """
                MATCH (n:Counter)
                FOREACH (increment IN [1, 2] |
                    SET n.value = n.value + increment
                    CREATE (:Snapshot {value:n.value})
                    SET n.value = n.value * 2
                )
                RETURN n.value
                """ ) );
        GraphResult snapshots = execute( "MATCH (n:Snapshot) RETURN n.value ORDER BY n.value" );
        assertEquals( 2, snapshots.getData().length );
        assertEquals( 1, number( snapshots, 0, 0 ) );
        assertEquals( 4, number( snapshots, 1, 0 ) );
        assertEquals( 8, scalar( "MATCH (n:Counter) RETURN n.value" ) );
    }


    @Test
    void usesPathNodeBindingsInNestedLoopsAndRemovesProperties() {
        execute( "CREATE (:End {temporary:1, counter:0})-[:LINK]->(:End {temporary:2, counter:0})" );
        GraphResult result = execute( """
                MATCH p=(:End)-[:LINK]->(:End)
                FOREACH (n IN nodes(p) |
                    REMOVE n.temporary
                    FOREACH (addend IN [1, 2] | SET n:Visited, n.counter = n.counter + addend)
                )
                RETURN nodes(p)[0].counter, nodes(p)[1].counter
                """ );
        assertEquals( 3, number( result, 0, 0 ) );
        assertEquals( 3, number( result, 0, 1 ) );
        assertEquals( 2, scalar( "MATCH (:Visited {counter:3}) RETURN count(*)" ) );
        assertEquals( 0, scalar( "MATCH (n:End) RETURN count(n.temporary)" ) );
    }


    @Test
    void keepsTheLoopVariableAndNewBindingsLocal() {
        assertEquals( 99, scalar( """
                WITH 99 AS value
                FOREACH (value IN [1, 2] |
                    CREATE (inner:Item {value:value})
                    SET inner.doubled = value * 2
                )
                RETURN value
                """ ) );
        assertEquals( 1, scalar( "MATCH (:Item {value:2, doubled:4}) RETURN count(*)" ) );
        assertNotNull( CypherConnection.executeGetResponse( """
                FOREACH (item IN [1] | CREATE (inner:Unexpected))
                RETURN inner
                """ ).getError() );
        assertEquals( 0, scalar( "MATCH (:Unexpected) RETURN count(*)" ) );
    }


    @Test
    void supportsMergeAndDeleteAndRunsBeforeDownstreamReads() {
        assertEquals( 1, scalar( """
                FOREACH (value IN [1, 1] | MERGE (:Item {value:value}))
                MATCH (:Item)
                RETURN count(*)
                """ ) );
        assertEquals( 1, execute( """
                MATCH (n:Item)
                FOREACH (item IN [n] | DELETE item)
                """ ).affectedTuples );
        assertEquals( 0, scalar( "MATCH (:Item) RETURN count(*)" ) );
    }


    @Test
    void limitsTheOutputOnlyAndRejectsReadClausesInsideTheBody() {
        assertEquals( 0, execute( "FOREACH (v IN [1, 2] | CREATE (:Item {value:v})) RETURN 1 AS ignored LIMIT 0" ).getData().length );
        assertEquals( 2, scalar( "MATCH (:Item) RETURN count(*)" ) );
        String read = CypherConnection.executeGetResponse( "FOREACH (v IN [1] | CREATE (:Unexpected) MATCH (n))" ).getError();
        assertNotNull( read );
        assertTrue( read.contains( "FOREACH only accepts updating clauses" ), read );
        String nonList = CypherConnection.executeGetResponse( "FOREACH (v IN 1 | CREATE (:Unexpected))" ).getError();
        assertNotNull( nonList );
        assertTrue( nonList.contains( "FOREACH requires a list" ), nonList );
        assertEquals( 0, scalar( "MATCH (:Unexpected) RETURN count(*)" ) );
    }


    @Test
    void rollsBackEarlierIterationsWhenAnUpdateFails() {
        String error = CypherConnection.executeGetResponse( """
                FOREACH (v IN [1, {invalid:2}] | CREATE (:Unexpected {value:v}))
                """ ).getError();
        assertNotNull( error );
        assertTrue( error.contains( "InvalidPropertyType" ), error );
        assertEquals( 0, scalar( "MATCH (:Unexpected) RETURN count(*)" ) );
    }


    private static long scalar( String query ) {
        return number( execute( query ), 0, 0 );
    }


    private static long number( GraphResult result, int row, int column ) {
        return PolyValue.fromTypedJson( result.getData()[row][column], PolyValue.class ).asNumber().longValue();
    }
}
