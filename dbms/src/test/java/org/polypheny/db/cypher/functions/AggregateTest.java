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

package org.polypheny.db.cypher.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
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
                Row.of( TestLiteral.from( 5 ) ) );

        execute( SINGLE_EDGE_2 );
        res = execute( "MATCH (n:Person) RETURN count(*)" );

        containsRows( res, true, true,
                Row.of( TestLiteral.from( 7 ) ) );
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
    public void countNullAggregateTest() {
        GraphResult res = execute( "MATCH (n) RETURN  count(*)" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 0 ) ) );
    }


    @Test
    public void countRenameFieldAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, count(*) AS c" );
        assertEquals( "c", res.getHeader()[1].name );

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
    public void countPropertyAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (n) RETURN count(n.age)" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 0 ) ) );
    }


    @Test
    public void countDistinctFunctionTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN COUNT(DISTINCT n.name)" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 1 ) ) );
    }


    @Test
    public void singleAvgAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );

        GraphResult res = execute( "MATCH (n) RETURN AVG(n.age)" );
        // Printing the data using Arrays.deepToString
        //String[][] data = res.getData();
        //System.out.println( Arrays.deepToString( data ) );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 26.33333333333333 ) ) );
    }


    @Test
    public void avgNullAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN AVG(p.age)" );
        containsRows( res, true, false, Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void avgRenameAggregationTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN AVG(p.age) AS ages" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 38 ) ) );
    }


    @Test
    public void avgRenameFieldAggregationTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  p.depno As depNumber , AVG(p.age) As avgAge" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( 38 ) ),
                Row.of( TestLiteral.from( 14 ), TestLiteral.from( 32 ) ) );
    }


    @Test
    public void singleCollectAggregationTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN COLLECT(p.age) " );
        containsRows( res, true, false, Row.of( TestLiteral.from( List.of( 45, 31 ) ) ) );
    }


    @Test
    public void collectRenameAggregationTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN COLLECT(p.age) AS ages" );
        containsRows( res, true, false, Row.of( TestLiteral.from( List.of( 45, 31 ) ) ) );
    }


    @Test
    public void collectRenameFieldAggregationTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (p:Person) RETURN COLLECT(p.age) AS ages , p.depno AS depNumber" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( List.of( 45, 31 ) ), TestLiteral.from( 13 ) ),
                Row.of( TestLiteral.from( List.of( 32 ) ), TestLiteral.from( 14 ) ) );
    }


    @Test
    public void collectNullAggregationTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (p:Person) RETURN COLLECT(p.age)" );
        containsRows( res, true, false, Row.of( TestLiteral.from( List.of() ) ) );
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
    void minMaxNullAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n) RETURN  min(n.age) as ageMin" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( null ) ) );

        res = execute( "MATCH (n) RETURN  max(n.age) as ageMax" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void minMaxRenameAggregateTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (n) RETURN  min(n.age) as ageMin" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 31 ) ) );

        res = execute( "MATCH (n) RETURN  max(n.age) as ageMax" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 45 ) ) );
    }


    @Test
    public void minMaxRenameFieldAggregateTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (n) RETURN n.depno as depNumber , min(n.age) as ageMin" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( 31 ) ),
                Row.of( TestLiteral.from( 14 ), TestLiteral.from( 32 ) ) );

        res = execute( "MATCH (n) RETURN n.depno as depNumber , max(n.age) as ageMax" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( 45 ) ),
                Row.of( TestLiteral.from( 14 ), TestLiteral.from( 32 ) ) );
    }


    @Test
    public void singleSumAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n) RETURN sum(n.age)" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 34 ) ) );
    }


    @Test
    public void sumNullAggregationTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n) RETURN sum(n.age)" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 0 ) ) );
    }


    @Test
    public void sumRenameAggregationTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  sum(p.age) As totalAge " );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 76 ) ) );
    }


    @Test
    public void sumRenameFieldAggregationTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  sum(p.age) AS totalAge, p.depno AS depNumber," );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( 76 ) ),
                Row.of( TestLiteral.from( 14 ), TestLiteral.from( 32 ) ) );
    }


    @Test
    public void singleStdevAggregateTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  stdev(p.age) " );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 9.8994949 ) ) );
    }


    @Test
    public void stdevNullAggregateTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  stdev(p.age) " );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void stdevRenameAggregateTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  stdev(p.age) AS Stdev " );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 9.8994949 ) ) );
    }


    @Test
    public void stdevRenameFieldAggregateTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) RETURN  stdev(p.age) AS Stdev , n.depno As department" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 9.8994949 ), TestLiteral.from( 13 ) ) );
    }

}
