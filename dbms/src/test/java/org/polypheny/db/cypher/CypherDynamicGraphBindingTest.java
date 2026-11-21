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

        assertError( "MATCH ()-[r:R]->() WITH last([null,r]) AS target SET target:A:B", "Edges require exactly one label" );
        assertEquals( 1, number( "MATCH ()-[r:R]->() RETURN count(r)" ) );

        assertError( input + "CREATE (target)-[:MADE]->(:New)", "CREATE requires node bindings for the source and target" );
        assertEquals( 0, number( "MATCH (n:New) RETURN count(n)" ) );
        assertEquals( 0, number( "MATCH ()-[r:MADE]->() RETURN count(r)" ) );
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
