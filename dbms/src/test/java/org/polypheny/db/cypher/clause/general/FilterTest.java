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
        GraphResult res = execute( """
                MATCH (n)
                WHERE n:Person
                RETURN n.name, n.age""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 31 ) ) );
    }


    @Test
    public void nodePropertyFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.age > 3 RETURN p" );

        assertNode( result, 0 );

        containsRows( result, true, false );

        result = execute( "MATCH (p) WHERE p.age >= 3 RETURN p" );
        assertNode( result, 0 );

        containsRows( result, true, false, Row.of( KIRA ) );
    }


    @Test
    public void relationPropertyFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( """
                MATCH (n:Person)-[k:KNOWS]->(f)
                WHERE k.since < 1995
                RETURN f.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

    }


    @Test
    public void NodePatternFilterTest() {
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (a:Person WHERE a.name = 'Max')-[:KNOWS]->(b:Person WHERE b.name = 'Hans')\n"
                + "RETURN b.name" );
        containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

    }


    @Test
    public void relationshipPatternFilterTest() {
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (a:Person)-[r:KNOWS WHERE r.since < 2000 ]->(b:Person)\n"
                + "RETURN r.since" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 1994 ) ) );
    }


    @Test
    public void propertyExistenceCheckFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( """
                MATCH (n:Person)
                WHERE n.age IS NOT NULL
                RETURN n.name, n.age""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );
    }


    @Test
    public void propertyNonExistenceCheckFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        GraphResult res = execute( """
                MATCH (n:Person)
                WHERE n.age IS NULL
                RETURN n.name""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void rangeFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        GraphResult res = execute( "MATCH (n) WHERE 21 < n.age <= 32 RETURN n.name" );
        containsRows( res, true, false, Row.of( TestLiteral.from( "Bob" ) ) );
    }


    @Test
    public void booleanConditionFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult result = execute( "MATCH (p) WHERE p.age >= 45 AND p.depno = 13 RETURN p.name" );

        containsRows( result, true, false, Row.of( TestLiteral.from( "Ann" ) ) );

        result = execute( "MATCH (p) WHERE p.age <= 32 OR p.depno = 13 RETURN p.name " );

        containsRows( result, true, false, Row.of( TestLiteral.from( "Ann" ) ),
                Row.of( TestLiteral.from( "Bob" ) ),
                Row.of( TestLiteral.from( "Alex" ) ) );
    }


    @Test
    public void multiBooleanOperatorsFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult res = execute( """
                MATCH (n:Person)
                WHERE (n.name = 'Alex' XOR (n.age < 30 AND n.name = 'Bob')) OR NOT (n.name = 'Bob' OR n.name = 'Alex')
                RETURN
                  n.name AS name,
                  n.age AS age
                """ );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Alex" ), TestLiteral.from( 32 ) ),
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );
    }


    @Test
    public void withFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        GraphResult res = execute( """
                MATCH (n:Person)
                WITH n.name as name
                WHERE n.age = 45
                RETURN name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Ann" ) ) );
    }


    @Test
    public void existFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult result = execute( "MATCH (p) WHERE exists(p.age) RETURN p" );

        assertEmpty( result );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );

        result = execute( "MATCH (p) WHERE exists(p.name) RETURN p" );
        assertNode( result, 0 );

        containsRows( result, true, false, Row.of( MAX ) );
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

        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );


    }


    @Test
    public void endWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( """
                MATCH (n)
                WHERE n.name ENDS WITH 's'
                RETURN n.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

    }


    @Test
    public void notEndWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( """
                MATCH (n:Person)
                WHERE NOT n.name ENDS WITH 's'
                RETURN n.name, n.age""" );
        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );

    }


    @Test
    public void containsFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.name CONTAINS 'H' RETURN p.name" );

        containsRows( result, true, false, Row.of( TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void patternFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( """
                MATCH
                  (p:Person {name: 'Max'}),
                  (other:Person)
                WHERE (p)-->(other)
                RETURN other.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

        res = execute( """
                MATCH
                  (p:Person {name: 'Max'}),
                  (other:Person)
                WHERE (p)--(other)
                RETURN other.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Hans" ) ) );

        res = execute( """
                MATCH
                  (p:Person {name: 'Max'}),
                  (other:Person)
                WHERE (p)<--(other)
                RETURN other.name""" );
        assertEmpty( res );
    }


    @Test
    public void patternWithPropertiesFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( """
                MATCH (other:Person)
                WHERE (other)-[:KNOWS]-({name: 'Hans'})
                RETURN other.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void listComprehensionFilterTest() {
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (a:Person {name: 'Max'})\n"
                + "RETURN [(a)-->(b WHERE b:Person) | b.name] AS friends" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( List.of( "Hans" ) ) ) );

    }


    @Test
    public void useInOperatorWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( """
                MATCH (a:Person)
                WHERE a.name IN ['Peter', 'Max']
                RETURN a.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );
    }


    @Test
    public void missingPropertyFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        GraphResult res = execute( """
                MATCH (n:Person)
                WHERE n.age >= 40\s
                RETURN n.name, n.age""" );
        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );

    }


}
