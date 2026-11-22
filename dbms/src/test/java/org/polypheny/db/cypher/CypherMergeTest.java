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

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.results.GraphResult;


class CypherMergeTest extends CypherTestTemplate {

    @BeforeEach
    void resetGraph() {
        tearDown();
        createGraph();
    }


    @Test
    void repeatedInputRowsShareTheCreatedNodeAndChooseTheCorrectSetBranch() {
        GraphResult result = execute( """
                UNWIND [1, 1, 2, 1] AS key
                MERGE (n:Item {key:key})
                  ON CREATE SET n.seen = 1, n:Created
                  ON MATCH SET n.seen = n.seen + 1, n:Matched
                RETURN count(*) AS rows
                """ );
        assertEquals( 4, number( result, 0, 0 ) );
        GraphResult matched = execute( "MATCH (n:Item:Created) RETURN n.key, n.seen ORDER BY n.key" );
        assertEquals( 2, matched.getData().length );
        assertEquals( 1, number( matched, 0, 0 ) );
        assertEquals( 3, number( matched, 0, 1 ) );
        assertEquals( 2, number( matched, 1, 0 ) );
        assertEquals( 1, number( matched, 1, 1 ) );
        assertEquals( 1, scalar( "MATCH (n:Item:Matched) RETURN count(n)" ) );
    }


    @Test
    void setActionsRunSequentiallyAndCanUpdateABoundNode() {
        execute( "CREATE (:Source {x:10})" );
        GraphResult result = execute( """
                MATCH (a:Source)
                MERGE (b:Target {id:a.x})
                  ON CREATE SET a.x = a.x + 1, b.x = a.x + 1
                RETURN a.x, b.x
                """ );
        assertEquals( 11, number( result, 0, 0 ) );
        assertEquals( 12, number( result, 0, 1 ) );
        assertEquals( 11, scalar( "MATCH (a:Source) RETURN a.x" ) );
        assertEquals( 12, scalar( "MATCH (b:Target {id:10}) RETURN b.x" ) );
    }


    @Test
    void repeatedMatchesUpdateTheSameEntityAcrossAliasesAndContainers() {
        execute( "CREATE (:Source {seen:0})" );
        GraphResult result = execute( """
                MATCH (n:Source)
                WITH n AS a, n AS b, {node:n} AS nested, [n] AS nodes
                UNWIND [1, 2, 3] AS ordinal
                MERGE (:Item)
                  ON CREATE SET a.seen = ordinal
                  ON MATCH SET a.seen = ordinal, b.seen = b.seen + 1
                RETURN b.seen, nested.node.seen, nodes[0].seen
                """ );
        assertEquals( 3, result.getData().length );
        for ( int i = 0; i < result.getData().length; i++ ) {
            assertEquals( 4, number( result, i, 0 ) );
            assertEquals( 4, number( result, i, 1 ) );
            assertEquals( 4, number( result, i, 2 ) );
        }
        assertEquals( 4, scalar( "MATCH (n:Source) RETURN n.seen" ) );
    }


    @Test
    void everyCompleteMatchProducesARowAndRunsOnMatch() {
        execute( "CREATE (:Item {id:1}), (:Item {id:2})" );
        GraphResult result = execute( """
                MERGE (n:Item)
                  ON CREATE SET n.seen = 10
                  ON MATCH SET n.seen = n.id + 1
                RETURN n.seen ORDER BY n.seen
                """ );
        assertEquals( 2, result.getData().length );
        assertEquals( 2, number( result, 0, 0 ) );
        assertEquals( 3, number( result, 1, 0 ) );
        assertEquals( 2, scalar( "MATCH (n:Item) RETURN count(n)" ) );
    }


    @Test
    void reusesEndpointsAndMatchesUndirectedRelationshipsInEitherDirection() {
        execute( "CREATE (a:A)-[:R {rank:1}]->(b:B), (b)-[:R {rank:2}]->(a)" );
        GraphResult result = execute( """
                MATCH (a:A), (b:B)
                MERGE p=(a)-[r:R]-(b)
                  ON MATCH SET r.seen = r.rank
                RETURN length(p), r.seen ORDER BY r.seen
                """ );
        assertEquals( 2, result.getData().length );
        for ( int i = 0; i < 2; i++ ) {
            assertEquals( 1, number( result, i, 0 ) );
            assertEquals( i + 1, number( result, i, 1 ) );
        }
        assertEquals( 2, scalar( "MATCH ()-[r:R]->() RETURN count(r)" ) );
    }


    @Test
    void createsUndirectedPatternFromLeftToRightAndReusesListProperties() {
        execute( "CREATE (:A), (:B)" );
        assertEquals( 2, scalar( """
                MATCH (a:A), (b:B)
                UNWIND [[1,2], [1,2]] AS values
                MERGE (a)-[:R {values:values}]-(b)
                RETURN count(*)
                """ ) );
        assertEquals( 1, scalar( "MATCH (:A)-[:R {values:[1,2]}]->(:B) RETURN count(*)" ) );
        assertEquals( 0, scalar( "MATCH (:A)<-[:R]-(:B) RETURN count(*)" ) );
    }


    @Test
    void copiesAndExtendsRelationshipPropertiesInBothSetBranches() {
        execute( "CREATE (:A {score:1}), (:B {score:2})" );
        execute( """
                MATCH (a:A), (b:B)
                MERGE (a)-[r:R {discard:9}]->(b)
                  ON CREATE SET r = a, r += {temporary:8}
                """ );
        assertEquals( 1, scalar( "MATCH (:A)-[r:R {temporary:8}]->(:B) RETURN r.score" ) );
        assertEquals( 0, scalar( "MATCH ()-[r:R]->() RETURN count(r.discard)" ) );

        execute( """
                MATCH (a:A), (b:B)
                MERGE (a)-[r:R]->(b)
                  ON MATCH SET r = b, r += {extra:7, removed:null}
                """ );
        assertEquals( 2, scalar( "MATCH (:A)-[r:R {extra:7}]->(:B) RETURN r.score" ) );
        GraphResult cleared = execute( "MATCH ()-[r:R]->() RETURN count(r.temporary), count(r.removed)" );
        assertEquals( 0, number( cleared, 0, 0 ) );
        assertEquals( 0, number( cleared, 0, 1 ) );
    }


    @Test
    void noPartialMatchIsReusedUnlessItsNodeIsAlreadyBound() {
        execute( "CREATE (:A), (:B)" );
        assertEquals( 1, scalar( "MERGE p=(:A)-[:R]->(:B) RETURN length(p)" ) );
        assertEquals( 4, scalar( "MATCH (n) RETURN count(n)" ) );
        assertEquals( 1, scalar( "MERGE (:A)-[:R]->(:B) RETURN count(*)" ) );
        assertEquals( 4, scalar( "MATCH (n) RETURN count(n)" ) );
    }


    @Test
    void multipleMergeClausesCanReadEarlierWritesAndPreserveDownstreamBindings() {
        GraphResult result = execute( """
                UNWIND [1, 2, 1] AS key
                MERGE (a:A {id:key})
                MERGE (b:B)
                MERGE (a)<-[r:R {key:key}]-(b)
                MATCH (b)-[r]->(a)
                RETURN count(*) AS rows
                """ );
        assertEquals( 3, number( result, 0, 0 ) );
        assertEquals( 3, scalar( "MATCH (n) RETURN count(n)" ) );
        assertEquals( 2, scalar( "MATCH (:B)-[r:R]->(:A) RETURN count(r)" ) );
    }


    @Test
    void emptyInputDoesNotCreateAnythingAndAWriteOnlyMergeCountsChanges() {
        assertEquals( 0, scalar( "MATCH (:Missing) MERGE (:Unexpected) RETURN count(*)" ) );
        assertEquals( 0, scalar( "MATCH (n) RETURN count(n)" ) );
        assertEquals( 1, execute( "MERGE (:Existing)" ).affectedTuples );
        assertEquals( 0, execute( "MERGE (:Existing)" ).affectedTuples );
    }


    @Test
    void executesWritesEvenWhenTheReturningProjectionHasLimitZero() {
        GraphResult empty = execute( "UNWIND [1,2] AS id MERGE (:Item {id:id}) RETURN id LIMIT 0" );
        assertEquals( 0, empty.getData().length );
        assertEquals( 2, scalar( "MATCH (n:Item) RETURN count(n)" ) );
    }


    @Test
    void aNullPatternValueFailsAndRollsBackEarlierRows() {
        String error = CypherConnection.executeGetResponse( """
                UNWIND [1, null] AS key
                MERGE (:Item {id:key})
                """ ).getError();
        assertNotNull( error );
        assertTrue( error.contains( "MERGE pattern properties cannot be null" ), error );
        assertEquals( 0, scalar( "MATCH (n:Item) RETURN count(n)" ) );
    }


    @Test
    void rejectsInvalidPatternsBeforeWriting() {
        for ( String pattern : List.of( "()-[]->()", "()-[:R|S]->()", "()-[:R*1]->()", "(n)-[:R]->(n:Label)" ) ) {
            String error = CypherConnection.executeGetResponse( "MERGE " + pattern ).getError();
            assertNotNull( error, pattern );
        }
        String rebound = CypherConnection.executeGetResponse( "MATCH (n) MERGE (n)" ).getError();
        assertNotNull( rebound );
        assertTrue( rebound.contains( "VariableAlreadyBound" ), rebound );
        assertEquals( 0, scalar( "MATCH (n) RETURN count(n)" ) );
    }


    private static long scalar( String query ) {
        return number( execute( query ), 0, 0 );
    }


    private static long number( GraphResult result, int row, int column ) {
        return PolyValue.fromTypedJson( result.getData()[row][column], PolyValue.class ).asNumber().longValue();
    }

}
