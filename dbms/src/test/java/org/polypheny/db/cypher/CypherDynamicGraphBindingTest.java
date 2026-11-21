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


class CypherDynamicGraphBindingTest extends CypherTestTemplate {

    @BeforeEach
    void resetGraph() {
        tearDown();
        createGraph();
        execute( "CREATE (:Start)-[:R {v:1}]->(:Target {v:2})" );
    }


    @Test
    void validatesDynamicNodeBindingsBeforeMatching() {
        for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
            for ( String label : List.of( "Start", "Missing" ) ) {
                assertError( "WITH last([null,1]) AS n %s (n:%s)-[:R*]->() RETURN count(*)".formatted( match, label ),
                        "MATCH requires a NODE binding" );
            }
        }
        assertEquals( 1, number( "WITH last([null,1]) AS x OPTIONAL MATCH (:Missing)-[x:R]->() RETURN count(x)" ) );
        assertEquals( 1, number( "OPTIONAL MATCH (n:Missing) WITH last([null,n]) AS x OPTIONAL MATCH (x)-[:R*]->() RETURN count(*)" ) );
    }


    @Test
    void doesNotValidateBindingsWhenTheMatchPredicateIsFalse() {
        assertEquals( 0, number( "WITH last([null,1]) AS n MATCH (n:Start) WHERE false RETURN count(*)" ) );
        assertEquals( 1, number( "WITH last([null,1]) AS n OPTIONAL MATCH (n:Start) WHERE false RETURN n" ) );
        assertEquals( 1, number( "WITH last([null,1]) AS n OPTIONAL MATCH (n:Start) WHERE false AND n.v = 1 RETURN n" ) );
        assertEquals( 0, number( "WITH 1 AS v OPTIONAL MATCH (n:Start) WHERE null RETURN count(n)" ) );
        assertEquals( 0, number( "MATCH (n:Start) WITH last([null,n]) AS target MATCH (target:Start) WHERE null RETURN count(*)" ) );
        assertError( "WITH last([null,1]) AS n MATCH (n:Start) WHERE null RETURN count(*)", "MATCH requires a NODE binding" );
        assertError( "WITH last([null,1]) AS n OPTIONAL MATCH (n:Start) WHERE null RETURN n", "MATCH requires a NODE binding" );
    }


    @Test
    void evaluatesInlinePredicatesBeforeRejectingMatchesByWhere() {
        for ( String match : List.of( "MATCH", "OPTIONAL MATCH" ) ) {
            for ( String guard : List.of( "null", "false" ) ) {
                assertError( "%s (n:Start WHERE 1/0 = 1) WHERE %s RETURN n".formatted( match, guard ), "/ by zero" );
            }
        }
        assertEquals( 0, number( "OPTIONAL MATCH (n:Start WHERE n.v = 1) WHERE null RETURN count(n)" ) );
    }


    @Test
    void treatsNullPredicatesAsNonMatches() {
        assertEquals( 0, number( "MATCH (n:Start) WHERE null RETURN count(n)" ) );
        assertEquals( 0, number( "MATCH p=(:Start)-[:R*]->() WHERE null RETURN count(p)" ) );
        assertEquals( 0, number( "MATCH (n:Start) WHERE [null][0] RETURN count(n)" ) );
        assertError( "WITH last([null,'x']) AS i MATCH (n:Start) WHERE [null][i] RETURN n", "A list index or slice bound must be an integer" );
        GraphResult optional = execute( """
                MATCH (n:Start) OPTIONAL MATCH (n)-[:R*]->(b) WHERE null OR false
                RETURN count(*) AS rows, count(b) AS matched
                """ );
        assertEquals( 1, value( optional, 0 ) );
        assertEquals( 0, value( optional, 1 ) );
        assertError( "WITH last([null,1]) AS n MATCH (n:Start) WHERE null OR false RETURN n", "MATCH requires a NODE binding" );
    }


    @Test
    void appliesConstantAndRowDependentGuardsBeforeCheckingBindings() {
        String with = "WITH last([null,1]) AS n, last([null,false]) AS guard ";
        for ( String where : List.of( "1=0", "false OR false", "guard", "last([null,false])" ) ) {
            assertEquals( 0, number( with + "MATCH (n:Start) WHERE " + where + " RETURN count(*)" ) );
            assertEquals( 1, number( with + "OPTIONAL MATCH (n:Start)-[:R*]->(b) WHERE " + where + " RETURN n" ) );
        }
        assertError( "WITH last([null,1]) AS n, last([true,null]) AS guard MATCH (n:Start) WHERE guard RETURN n",
                "MATCH requires a NODE binding" );
        assertEquals( 0, number( "WITH last([null,1]) AS r MATCH (:Start)-[r:R]->() RETURN count(r)" ) );
        assertEquals( 1, number( "WITH last([null,1]) AS r OPTIONAL MATCH (:Start)-[r:R]->() RETURN r" ) );
    }


    @Test
    void writesNodesAndRelationshipsFromTheSameDynamicBinding() {
        assertEquals( 2, number( """
                MATCH (:Start)-[rs:R*]->(b:Target)
                UNWIND [b,last(rs)] AS target
                SET target.v = target.v + 10
                RETURN count(*)
                """ ) );
        assertEquals( 12, number( "MATCH (b:Target) RETURN b.v" ) );
        assertEquals( 11, number( "MATCH ()-[r:R]->() RETURN r.v" ) );
    }


    @Test
    void aliasesOfUpdatedDynamicNodesSeeTheSameValue() {
        GraphResult result = execute( """
                MATCH (:Start)-[:R*]->(b:Target)
                WITH b,last([null,b]) AS target
                SET target.v = 3
                RETURN b.v AS original, target.v AS updated
                """ );
        assertEquals( 3, value( result, 0 ) );
        assertEquals( 3, value( result, 1 ) );
        assertEquals( 3, number( "MATCH (b:Target) RETURN b.v" ) );
    }


    @Test
    void validDynamicNodesCanBeCreateEndpoints() {
        assertEquals( 1, number( """
                MATCH (:Start)-[:R*]->(b:Target)
                WITH last([null,b]) AS target
                CREATE p = (target)-[:MADE]->(:New)
                RETURN length(p)
                """ ) );
        assertEquals( 1, number( "MATCH (:Target)-[r:MADE]->(:New) RETURN count(r)" ) );
    }


    @Test
    void validatesAllDynamicWriteTargetsBeforeWritingAnyRows() {
        String input = "MATCH (:Start)-[:R*]->(b:Target) UNWIND [b,1] AS target ";
        assertError( input + "SET target.v = 9", "Graph writes require a node or relationship binding" );
        assertEquals( 2, number( "MATCH (b:Target) RETURN b.v" ) );

        assertError( input + "CREATE (target)-[:MADE]->(:New)", "CREATE requires node bindings for the source and target" );
        assertEquals( 0, number( "MATCH (n:New) RETURN count(n)" ) );
        assertEquals( 0, number( "MATCH ()-[r:MADE]->() RETURN count(r)" ) );
    }


    @Test
    void labelsCanBeSetOnlyOnNodes() {
        for ( String query : List.of(
                "MATCH ()-[r:R]->() SET r:Changed",
                "MATCH ()-[r:R]->() WITH last([null,r]) AS target SET target:Changed",
                "MATCH ()-[r:R]->() WITH last([null,r]) AS target SET target:A:B" ) ) {
            assertError( query, "SET label assignment requires a node binding" );
        }
        assertEquals( 1, number( "MATCH ()-[r:R]->() RETURN r.v" ) );
        assertEquals( 0, number( "MATCH ()-[r:Changed]->() RETURN count(r)" ) );

        assertEquals( 1, number( """
                MATCH (n:Target) WITH last([null,n]) AS target
                SET target:Extra RETURN count(target)
                """ ) );
        assertEquals( 1, number( "MATCH (n:Target:Extra) RETURN count(n)" ) );
        assertEquals( 0, number( """
                OPTIONAL MATCH (n:Missing) WITH last([null,n]) AS target
                SET target:Extra RETURN count(target)
                """ ) );
    }


    private static void assertError( String query, String message ) {
        String error = CypherConnection.executeGetResponse( query ).getError();
        assertNotNull( error, query );
        assertTrue( error.contains( message ), error );
    }


    private static long number( String query ) {
        return value( execute( query ), 0 );
    }


    private static long value( GraphResult result, int index ) {
        return PolyValue.fromTypedJson( result.getData()[0][index], PolyValue.class ).asNumber().longValue();
    }

}
