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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;
import java.util.Arrays;
import java.util.List;

public class FilterTest extends CypherTestTemplate {

    @BeforeEach
    public void setUp() {
        tearDown();
        createGraph();
    }

    ///////////////////////////////////////////////
    /////////// FILTER
    ///////////////////////////////////////////////


    @Test
    public void nodeLabelFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (n)\n"
                + "WHERE n:Person\n"
                + "RETURN n.name, n.age" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 31 ) ) );
    }


    @Test
    public void nodePropertyFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.age > 3 RETURN p" );

        assertNode( result, 0 );

        assert containsRows( result, true, false );

        result = execute( "MATCH (p) WHERE p.age >= 3 RETURN p" );
        assertNode( result, 0 );

        assert containsRows( result, true, false, Row.of( KIRA ) );
    }


    @Test
    public void relationPropertyFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (n:Person)-[k:KNOWS]->(f)\n"
                + "WHERE k.since < 1995\n"
                + "RETURN f.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

    }


    @Test
    public void NodePatternFilterTest() {
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (a:Person WHERE a.name = 'Max')-[:KNOWS]->(b:Person WHERE b.name = 'Hans')\n"
                + "RETURN b.name" );
        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

    }

    @Test
    public void  relationshipPatternFilterTest()
    {
        execute( SINGLE_EDGE_2 );

        GraphResult res =  execute( "MATCH (a:Person)-[r:KNOWS WHERE r.since < 2000 ]->(b:Person)\n"
                + "RETURN r.since" );
        assert  containsRows( res , true , false , Row.of( TestLiteral.from(1994  ) ) );
    }


    @Test
    public void propertyExistenceCheckFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (n:Person)\n"
                + "WHERE n.age IS NOT NULL\n"
                + "RETURN n.name, n.age" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );
    }
    @Test
    public void propertyNonExistenceCheckFilterTest()
    {
       execute( SINGLE_NODE_PERSON_1 );
       execute( SINGLE_NODE_PERSON_COMPLEX_1 );
       GraphResult  res = execute( "MATCH (n:Person)\n"
            + "WHERE n.age IS NULL\n"
            + "RETURN n.name" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" )) );
    }



    @Test
    public void rangeFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        GraphResult res = execute( "MATCH (n) WHERE 21 < n.age <= 32 RETURN n.name" );
        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Bob" ) ) );
    }


    @Test
    public void booleanConditionFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult result = execute( "MATCH (p) WHERE p.age >= 45 AND p.depno = 13 RETURN p.name" );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Ann" ) ) );

        result = execute( "MATCH (p) WHERE p.age <= 32 OR p.depno = 13 RETURN p.name " );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Ann" ) ),
                Row.of( TestLiteral.from( "Bob" ) ),
                Row.of( TestLiteral.from( "Alex" ) ) );
    }


    @Test
    public void multiBooleanOperatorsFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult res = execute( "MATCH (n:Person)\n"
                + "WHERE (n.name = 'Alex' XOR (n.age < 30 AND n.name = 'Bob')) OR NOT (n.name = 'Bob' OR n.name = 'Alex')\n"
                + "RETURN\n"
                + "  n.name AS name,\n"
                + "  n.age AS age\n" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Alex" ), TestLiteral.from( 32 ) ),
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );
    }


    @Test
    public void withFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        GraphResult res = execute( "MATCH (n:Person)\n"
                + "WITH n.name as name\n"
                + "WHERE n.age = 45\n"
                + "RETURN name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Ann" ) ) );
    }


    @Test
    public void existFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult result = execute( "MATCH (p) WHERE exists(p.age) RETURN p" );

        assertEmpty( result );

        result = execute( "MATCH (p) WHERE exists(p.name) RETURN p" );
        assertNode( result, 0 );

        assert containsRows( result, true, false, Row.of( MAX ) );
    }


    ///////////////////////////////////
    /////  STRING matching
    /////////////////////////////////////
    @Test
    public void startWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (p) WHERE p.name STARTS WITH 'M' RETURN p.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );


    }


    @Test
    public void endWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (n)\n"
                + "WHERE n.name ENDS WITH 's'\n"
                + "RETURN n.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

    }


    @Test
    public void notEndWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (n:Person)\n"
                + "WHERE NOT n.name ENDS WITH 's'\n"
                + "RETURN n.name, n.age" );
        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );

    }


    @Test
    public void containsFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.name CONTAINS 'H' RETURN p.name" );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void patternFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH\n"
                + "  (p:Person {name: 'Max'}),\n"
                + "  (other:Person)\n"
                + "WHERE (p)-->(other)\n"
                + "RETURN other.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

        res = execute( "MATCH\n"
                + "  (p:Person {name: 'Max'}),\n"
                + "  (other:Person)\n"
                + "WHERE (p)--(other)\n"
                + "RETURN other.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

        res = execute( "MATCH\n"
                + "  (p:Person {name: 'Max'}),\n"
                + "  (other:Person)\n"
                + "WHERE (p)<--(other)\n"
                + "RETURN other.name" );
        assertEmpty( res );
    }


    @Test
    public void patternWithPropertiesFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (other:Person)\n"
                + "WHERE (other)-[:KNOWS]-({name: 'Hans'})\n"
                + "RETURN other.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void listComprehensionFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (a:Person {name: 'Max'})\n"
                + "RETURN [(a)-->(b WHERE b:Person) | b.name] AS friends" );

        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( List.of( "Hans" ) ) ) );

    }


    @Test
    public void useInOperatorWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (a:Person)\n"
                + "WHERE a.name IN ['Peter', 'Max']\n"
                + "RETURN a.name" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void missingPropertyFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        GraphResult res = execute( "MATCH (n:Person)\n"
                + "WHERE n.age >= 40 \n"
                + "RETURN n.name, n.age" );
        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );

    }




}
