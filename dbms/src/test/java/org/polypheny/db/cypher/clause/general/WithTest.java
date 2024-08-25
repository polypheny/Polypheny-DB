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

package org.polypheny.db.cypher.clause.general;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;


public class WithTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void singleVariableWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n RETURN n.name" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void multipleRenameVariablesWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH p.name AS person_name ,  p.age AS person_age , p RETURN person_name, person_age , p.depno;" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ), TestLiteral.from( 13 ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 31 ), TestLiteral.from( 13 ) ) );
    }


    @Test
    public void renameWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name AS name, n RETURN name, n" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( "Max" ), MAX ),
                Row.of( TestLiteral.from( "Hans" ), HANS ) );
    }


    @Test
    public void startWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name, n WHERE n.name STARTS WITH 'H' RETURN n" );
        containsRows( res, true, true,
                Row.of( HANS ) );
    }


    @Test
    public void startRenameWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name as name , n WHERE name  STARTS WITH 'H' RETURN n" );
        assertNode( res, 0 );
        containsRows( res, true, true,
                Row.of( HANS ) );
    }


    @Test
    public void endWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name , n WHERE n.name  ENDS WITH 'x' RETURN name, n" );
        assertNode( res, 1 );
        containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ), MAX ) );
    }


    @Test
    public void endRenameWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name AS name, n WHERE name ENDS WITH 'x' RETURN name, n" );
        assertNode( res, 1 );
        containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ), MAX ) );
    }


    @Test
    public void containsWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n:Person)-[]->(p:Animal) WITH *, n.name AS username WHERE username CONTAINS 'a' RETURN username, p" );
        assertNode( res, 1 );
        containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ), KIRA ) );
    }


    // aggregate
    @Test
    public void avgAggregationRenameWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH avg(p.age) as ageAvg RETURN  ageAvg " );
        containsRows( res, true, true, Row.of( TestLiteral.from( 38 ) ) );
    }


    @Test
    public void maxMinAggregationRenameWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH MAX(p.age) as ageMax RETURN  ageMax " );
        containsRows( res, true, true, Row.of( TestLiteral.from( 45 ) ) );

        res = execute( "MATCH (p:Person) WITH MIN(p.age) as ageMin RETURN  ageMin " );
        containsRows( res, true, true, Row.of( TestLiteral.from( 31 ) ) );
    }


    @Test
    public void countAggregationWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH COUNT(*) as count RETURN  count " );
        containsRows( res, true, true, Row.of( TestLiteral.from( 2 ) ) );

    }


    @Test
    public void stDevAggregationWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH STDEV(p.age) as ageStdev RETURN  ageStdev " );
        containsRows( res, true, true, Row.of( TestLiteral.from( 9.8994949 ) ) );

    }


    @Test
    public void collectAggregationWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH COLLECT(p.age) as ageList RETURN  ageList " );
        containsRows( res, true, true, Row.of( TestLiteral.from( 45 ), TestLiteral.from( 31 ) ) );

    }


    @Test
    public void mapStructureRenameWithTest() {
        GraphResult res = execute( "WITH {person: {name: 'Anne', age: 25}} AS p RETURN p" );
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void filterWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (p:Person) WITH p WHERE p.age > 31 RETURN p.name, p.age" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ),
                Row.of( TestLiteral.from( "Alex" ), TestLiteral.from( 32 ) ) );
    }


    @Test
    public void mathematicalOperationWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (p:Person) WITH p, p.age * 2 AS double_age RETURN p.name, double_age" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 90.0 ) ),
                Row.of( TestLiteral.from( "Alex" ), TestLiteral.from( 62.0 ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 64.0 ) ) );
    }


    @Test
    public void listWithTest() {
        GraphResult res = execute( "WITH [1, 2, 3, 4, 5] AS numbers RETURN numbers" );
        containsRows( res, true, false, Row.of( TestLiteral.from( List.of( 1, 2, 3, 4, 5 ) ) ) );
    }


    @Test
    public void unWindListWithTest() {
        GraphResult res = execute( "WITH [1, 2, 3, 4, 5] AS numbers UNWIND numbers AS number RETURN number" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( 2 ) ),
                Row.of( TestLiteral.from( 3 ) ),
                Row.of( TestLiteral.from( 4 ) ),
                Row.of( TestLiteral.from( 5 ) ) );
    }


    @Test
    public void unWindAndFilterListWithTest() {
        GraphResult res = execute( "WITH [1, 2, 3, 4, 5] AS numbers UNWIND numbers AS number WITH number WHERE number > 3 RETURN number" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( 4 ) ),
                Row.of( TestLiteral.from( 5 ) ) );
    }


    @Test
    public void unWindAndStartListWithTest() {
        GraphResult res = execute( "WITH ['John', 'Mark', 'Jonathan', 'Bill'] AS somenames UNWIND somenames AS names WITH names AS candidate WHERE candidate STARTS WITH 'Jo' RETURN candidate" );
        containsRows( res, true, false, Row.of( TestLiteral.from( "John" ) ), Row.of( TestLiteral.from( "Jonathan" ) ) );
    }


    @Test
    public void unWindAndLogicalOperatorsListWithTest() {
        GraphResult res = execute( "WITH [2, 4, 7, 9, 12] AS numberlist UNWIND numberlist AS number WITH number WHERE number = 4 OR (number > 6 AND number < 10) RETURN number" );
        assertTrue( containsRows( res, false, false,
                Row.of( TestLiteral.from( 4 ) ),
                Row.of( TestLiteral.from( 7 ) ),
                Row.of( TestLiteral.from( 9 ) ) ) );
    }


    @Test
    public void unWindAndWhereInListWithTest() {
        GraphResult res = execute( "WITH [2, 3, 4, 5] AS numberlist UNWIND numberlist AS number WITH number WHERE number IN [2, 3, 8] RETURN number" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 2 ) ), Row.of( TestLiteral.from( 3 ) ) );
    }


    @Test
    public void createNodeWithTest() {
        execute( "WITH [1, 1.0] AS list CREATE ({l: list})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void distinctWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (p:Person) WITH Distinct(p)  Return p " );
        assertEquals( 2, res.getData().length );
    }


    @Test
    public void existsWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) WITH n as person WHERE EXISTS(person.age) RETURN person.name, person.age;" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );
    }


    @Test
    public void conditionalLogicWithTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (p:Person) WITH p, CASE WHEN p.age < 30 THEN 'Young' THEN p.age >= 30 AND p.age < 60 THEN 'Middle-aged' ELSE 'Elderly  END AS ageGroup RETURN p.name, ageGroup;" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( "Ana" ), TestLiteral.from( "Middle-aged" ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( "Middle-aged" ) ),
                Row.of( TestLiteral.from( "Alex" ), TestLiteral.from( "Middle-aged" ) ) );
    }


    @Test
    public void orderByWithTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (p:Person) WITH p ORDER BY p.name ASC RETURN p.name" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( "Hans" ) ),
                Row.of( TestLiteral.from( "Max" ) ) );
    }

}
