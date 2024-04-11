/*
 * Copyright 2019-2024 The Polypheny Project
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;


public class AggregateTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @AfterEach
    public void tearGraphDown() {
        tearDown();
    }


    @Test
    public void singleCountAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n:Person) RETURN count(*)" );

        containsRows( res, true, true,
                Row.of( TestLiteral.from( 4 ) ) );

        execute( SINGLE_EDGE_2 );

        containsRows( res, true, true,
                Row.of( TestLiteral.from( 6 ) ) );
    }


    @Test
    public void countFieldAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, count(*)" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 3 ) ),
                Row.of( TestLiteral.from( "Kira" ), TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( 2 ) ) );

    }


    @Test
    public void countRenameFieldAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, count(*) AS c" );
        assert res.getHeader()[1].name.equals( "c" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 3 ) ),
                Row.of( TestLiteral.from( "Kira" ), TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( 2 ) ) );

    }


    @Test
    public void doubleCountRenameAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age, count(*) AS c" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( null ), TestLiteral.from( 3 ) ),
                Row.of( TestLiteral.from( "Kira" ), TestLiteral.from( 3 ), TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( null ), TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( 31 ), TestLiteral.from( 1 ) ) );

    }


    @Test
    public void singleAvgAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );

        GraphResult res = execute( "MATCH (n) RETURN avg(n.age)" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( 24 ) ) );

    }


    @Test
    public void singleMinMaxAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );

        GraphResult res = execute( "MATCH (n) RETURN min(n.age)" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "MATCH (n) RETURN max(n.age)" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( 45 ) ) );

    }


    @Test
    public void singleSumAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        execute( "MATCH (n) RETURN sum(n.age)" );
    }

}
