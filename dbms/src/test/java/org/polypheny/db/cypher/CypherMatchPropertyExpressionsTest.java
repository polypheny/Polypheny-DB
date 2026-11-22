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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.webui.models.results.GraphResult;

class CypherMatchPropertyExpressionsTest extends CypherTestTemplate {

    @BeforeEach
    void createPaths() {
        tearDown();
        createGraph();
        execute( """
                CREATE (a:Start {name:'root', day:date('2024-02-29')})
                         -[:STEP {value:42}]->(b:Middle {name:'first'})
                         -[:STEP {value:42}]->(c:End {name:'hit'}),
                       (a)-[:STEP {value:42}]->(:Middle {name:'second'})
                          -[:STEP {value:43}]->(:End {name:'miss'}),
                       (c)-[:AFTER {value:999}]->(:Finish {name:'finish'}),
                       (:Start {name:'other', day:date('2024-02-29')}),
                       (:Start {name:'root', day:date('2024-03-01')})
                """ );
    }


    @Test
    void matchesNamedAndAnonymousNodesKeepingLiteralConstraintsAndScope() {
        GraphResult result = execute( """
                MATCH (n:Start {name:'root', day:date('2024-02-29')})
                RETURN n.day.day AS day
                """ );
        assertEquals( 1, result.getData().length );
        assertEquals( 29, cell( result, 0, 0 ).asNumber().intValue() );

        GraphResult star = execute( """
                MATCH (:Start {day:date('2024-03-01')}), (e:End {name:'hit'})
                RETURN *
                """ );
        assertEquals( 1, star.getData().length );
        assertEquals( List.of( "e" ), Arrays.stream( star.getHeader() ).map( field -> field.name ).toList() );
        assertNotNull( CypherConnection.executeGetResponse( """
                MATCH (:Start {day:date('2024-03-01')})
                RETURN `$matchProperty0`
                """ ).getError() );
    }


    @Test
    void matchesNamedAndAnonymousFixedRelationshipsAndPathNodes() {
        GraphResult named = execute( """
                WITH -43 AS expected
                MATCH (:Middle)-[r:STEP {value:abs(expected)}]->(e:End)
                RETURN e.name AS name, r.value AS value
                """ );
        assertEquals( 1, named.getData().length );
        assertEquals( "miss", cell( named, 0, 0 ).asString().value );
        assertEquals( 43, cell( named, 0, 1 ).asNumber().intValue() );

        GraphResult anonymous = execute( """
                MATCH (:Start {day:date('2024-02-29')})-[:STEP {value:toInteger('42')}]->(m:Middle)
                RETURN * ORDER BY m.name
                """ );
        assertEquals( 2, anonymous.getData().length );
        assertEquals( List.of( "m" ), Arrays.stream( anonymous.getHeader() ).map( field -> field.name ).toList() );
    }


    @Test
    void constrainsEveryRelationshipInTheSpecifiedSegmentOnly() {
        GraphResult result = execute( """
                MATCH (:Start)-[steps:STEP*2 {value:toInteger('42')}]->(:End)-[:AFTER]->(f:Finish)
                RETURN f.name AS name, size(steps) AS steps
                """ );
        assertEquals( 1, result.getData().length );
        assertEquals( "finish", cell( result, 0, 0 ).asString().value );
        assertEquals( 2, cell( result, 0, 1 ).asNumber().intValue() );

        GraphResult anonymous = execute( """
                MATCH (:Start)-[:STEP*2 {value:abs(-42)}]->(e:End)
                RETURN e.name AS name
                """ );
        assertEquals( 1, anonymous.getData().length );
        assertEquals( "hit", cell( anonymous, 0, 0 ).asString().value );

        GraphResult zero = execute( """
                MATCH (:Finish)-[:STEP*0 {value:date('invalid')}]->(f:Finish)
                RETURN f.name AS name
                """ );
        assertEquals( 1, zero.getData().length );
        assertEquals( "finish", cell( zero, 0, 0 ).asString().value );
    }


    @Test
    void appliesCorrelatedPropertyChecksBeforeOptionalNullExtension() {
        GraphResult result = execute( """
                UNWIND ['2024-02-29', '2024-03-02', null] AS expected
                OPTIONAL MATCH (n:Start {name:'other', day:date(expected)})
                RETURN expected, n.name AS name ORDER BY expected
                """ );
        assertEquals( 3, result.getData().length );
        assertEquals( "other", cell( result, 0, 1 ).asString().value );
        assertNull( cell( result, 1, 1 ) );
        assertNull( cell( result, 2, 1 ) );
    }

    @Test
    void keepsInternalBindingsOutOfReturnedPaths() {
        GraphResult result = execute( """
                MATCH p=(:Start {name:'root', day:date('2024-02-29')})-[:STEP {value:abs(-42)}]->(m:Middle)
                RETURN p AS path
                """ );
        assertEquals( 2, result.getData().length );
        PolyPath path = cell( result, 0, 0 ).asPath();
        assertNull( path.getNodes().get( 0 ).getVariableName() );
        assertNull( path.getEdges().get( 0 ).getVariableName() );
        assertEquals( "m", path.getNodes().get( 1 ).getVariableName().value );
    }


    private static PolyValue cell( GraphResult result, int row, int column ) {
        String value = result.getData()[row][column];
        return value == null ? null : PolyValue.fromTypedJson( value, PolyValue.class );
    }

}
