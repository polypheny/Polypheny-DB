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

package org.polypheny.db.cypher.clause.write;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class DmlInsertTest extends CypherTestTemplate {


    public static final String CREATE_PERSON_MAX = "CREATE (p:Person {name: 'Max Muster'})";

    public static final String CREATE_COMPLEX_GRAPH_1 =
            "CREATE\n"
                    + "  (adam:User {name: 'Adam'}),\n"
                    + "  (pernilla:User {name: 'Pernilla'}),\n"
                    + "  (david:User {name: 'David'}),\n"
                    + "  (adam)-[:FRIEND]->(pernilla),\n"
                    + "  (pernilla)-[:FRIEND]->(david)";

    public static final String CREATE_COMPLEX_GRAPH_2 =
            "CREATE (adam:User {name: 'Adam'}), (pernilla:User {name: 'Pernilla'}), (david:User {name: 'David'}), (adam)-[:FRIEND]->(pernilla), (pernilla)-[:FRIEND]->(david), (david)-[:FRIEND]->(adam)";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void insertEmptyNodeWithNoVariableNoLabelTest() {
        execute( "CREATE ()" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of() ) );
    }


    @Test
    public void insertEmptyNodeWithNoVariableTest() {
        execute( "CREATE (:Person)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Person" ) ) );
    }


    @Test
    public void insertEmptyNoLabelNodeTest() {
        execute( "CREATE (p)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of() ) );
    }


    @Test
    public void insertEmptyNodeTest() {
        execute( "CREATE (p:Person)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Person" ) ) );
    }


    @Test
    public void insertNodeTest() {
        execute( "CREATE (p:Person {name: 'Max Muster'})" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( Pair.of( "name", "Max Muster" ) ) );
    }


    @Test
    public void insertNodeWithManyLabelsTest() {
        execute( "CREATE (p:Person :Employee )" );
        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true, TestNode.from( List.of( "Person", "Employee" ) ) );
    }


    @Test
    public void insertNodeWithManyLabelsAndAsPropertyTest() {
        execute( "CREATE (n:Person:Employee {name :'Max'})" );

        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true, TestNode.from( List.of( "Person", "Employee" ), Pair.of( "name", "Max" ) ) );

    }


    @Test
    public void insertBigIntegerExceeded32BitAsPropertyValeTest() {
        execute( "CREATE (n : node { id  : 4294967296 })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 4294967296L ) ) );
    }


    @Test
    public void insertMaxLongAsPropertyValueTest() {
        execute( "CREATE (n : node { id  : 9223372036854775807})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 9223372036854775807L ) ) );

    }


    @Test
    public void insertResultOfMathematicalOperationsTest() {
        execute( "CREATE (n : node { id  : 1*2+6/3 })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 9223372036854775807L ) ) );

    }


    @Test
    public void insertENotationAsPropertyValueTest() {
        execute( "CREATE (n : node { id  : 1e2})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 100 ) ) );


    }


    @Test
    public void insertMaxLongAsResultOfMathematicalOperationsTest() {

        execute( "CREATE (n : node { id  : 2^63-1})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 4 ) ) );

    }


    @Test
    public void insertMinLongAsPropertyValueTest() {

        execute( "CREATE (n : node { id  : -9223372036854775808L})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", -9223372036854775808L ) ) );
    }


    @Test
    public void insertMaxDoubleAsPropertyValueTest() {
        execute( "CREATE (n : node { id  : 1.7976931348623157e+308})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 1.7976931348623157e+308 ) ) );

    }


    @Test
    public void insertMinDoubleAsPropertyValueTest() {

        execute( "CREATE (n : node { id  : 4.9e-324 })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( res.getData().length, 1 );
        containsNodes( res, true, TestNode.from( List.of( "node" ), Pair.of( "id", 4.9e-324 ) ) );
    }


    @Test
    public void insertHexadecimalIntegerAsPropertyValueTest() {
        execute( "CREATE (n : node { id  : 0x13af})" );
        execute( "CREATE (n : node { id  : -0x66eff})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "id", 5039 ) ),
                TestNode.from( List.of( "node" ), Pair.of( "id", -421631 ) ) );


    }


    @Test
    public void insertOctalIntegerAsPropertyValueTest() {

        execute( "CREATE (n : node { id  : 0o1372})" );
        execute( "CREATE (n : node { id  :-0o5671 })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "id", 762 ) ),
                TestNode.from( List.of( "node" ), Pair.of( "id", -3001 ) ) );
    }


    @Test
    public void insertTabCharWithPropertyValueTest() {
        execute( "CREATE (n : node { title : \"Hello\\tWorld\" })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "title", "Hello\tWorld" ) ) );
    }


    @Test
    public void insertBackspaceCharWithPropertyValueTest() {
        execute( "CREATE (n : node { title : \"Hello\\bWorld\" })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "title", "Hello\bWorld" ) ) );
    }


    @Test
    public void insertNewlineWithPropertyValueTest() {
        execute( "CREATE (n : node { title : \"Hello\\nWorld\" })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "title", "Hello\nWorld" ) ) );
    }


    @Test
    public void insertSingleQuoteCharWithPropertyValueTest() {
        execute( "CREATE (n : node { title : 'Hello\\'World' })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "title", "Hello'World" ) ) );
    }


    @Test
    public void insertMultiplePropertiesWithSingleQuoteValuesTest() {
        execute( " CREATE (n:node { name: \"Xi'an\", url: \"http://dbpedia.org/resource/Xi'an\"})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "name", "Xi'an" ),
                        Pair.of( "url", "http://dbpedia.org/resource/Xi'an" ) ) );
    }


    @Test
    public void insertDoubleQuoteCharWithPropertyValueTest() {
        execute( "CREATE (n : node { title : \"Hello\\\"World\" })" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "node" ), Pair.of( "title", "Hello\"World" ) ) );
    }


    @Test
    public void insertNodeWithPropertyContainsListTest() {
        execute( "CREATE ({l: [1 ,2,3]})" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );

    }


    @Test
    public void insertTwoNodeTest() {
        execute( CREATE_PERSON_MAX );
        execute( CREATE_PERSON_MAX );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true,
                TestNode.from( Pair.of( "name", "Max Muster" ) ),
                TestNode.from( Pair.of( "name", "Max Muster" ) ) );
    }


    @Test
    public void insertMultipleNodesTest() {
        execute( "CREATE (p),(n),(m)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from(), TestNode.from(), TestNode.from() );
    }


    @Test
    public void insertPropertyTypeTest() {
        execute( "CREATE (p:Person {name: 'Max Muster', age: 13, height: 185.3, nicknames: [\"Maxi\",\"Musti\"]})" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true,
                TestNode.from(
                        Pair.of( "name", "Max Muster" ),
                        Pair.of( "age", 13 ),
                        Pair.of( "height", 185.3 ),
                        Pair.of( "nicknames", List.of( "Maxi", "Musti" ) )
                ) );
    }


    @Test
    @Disabled
    public void insertReturnNodeTest() {
        GraphResult res = execute(
                "CREATE (p:Person {name: 'Max Muster'})\n"
                        + "RETURN p" );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of() ) );
    }


    @Test
    public void insertSingleHopPathNoVariableTest() {
        execute( "CREATE (p:Person {name :'Max'}) -[ : OWNER_OF] ->(a: Animal {name :'Kira' , age: 3 , type :'dog'})" );
        GraphResult res = matchAndReturnAllNodes();
        // only select all nodes
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                TestNode.from(
                        List.of( "Animal" ),
                        Pair.of( "name", "Kira" ),
                        Pair.of( "age", 3 ),
                        Pair.of( "type", "dog" ) ) );

        // only select all edges
        res = execute( "MATCH ()-[r]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );

    }


    @Test
    public void insertSingleHopPathTest() {
        execute( "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})" );
        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                TestNode.from(
                        List.of( "Animal" ),
                        Pair.of( "name", "Kira" ),
                        Pair.of( "age", 3 ),
                        Pair.of( "type", "dog" ) ) );

    }


    @Test
    public void insertSingleHopPathEdgesTest() {
        execute( "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})" );
        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
    }


    @Test
    public void insertSingleHopPathWithMultiplePropertiesTest() {
        execute( "CREATE (p:Person {name: 'Max'})-[rel:KNOWS {since: 1994 , relation : 'friend'}]->(a:Person {name:'Hans', age:31})" );

        // only select all nodes
        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                TestNode.from(
                        List.of( "Person" ),
                        Pair.of( "name", "Hans" ),
                        Pair.of( "age", 31 ) ) );

        // only select all edges
        res = execute( "MATCH ()-[r]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "KNOWS" ),
                Pair.of( "since", 1994 ), Pair.of( "relation", "friend" ) ) );

    }


    @Test
    public void insertSingleHopPathWithListPropertyTest() {
        execute( "Create (p:Person:Employee {name: 'Max'})-[role:ACTED_IN {roles:['Neo', 42, 'Thomas Anderson']}]->(matrix:Movie {title: 'The Matrix'})" );
        // only select all nodes
        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person", "Employee" ), Pair.of( "name", "Max" ) ),
                TestNode.from(
                        List.of( "Movie" ),
                        Pair.of( "title", "The Matrix" ) ) );

        // only select all edges
        res = execute( "MATCH ()-[r]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "ACTED_IN" ),
                Pair.of( "roles", List.of( "Neo", 42, "Thomas Anderson" ) ) ) );
    }


    @Test
    public void insertMultipleHopPathTest() {
        execute( "CREATE (n:Person)-[f:FRIEND_OF]->(p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira'})" );

        // only select all nodes
        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ) ),
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) );

        // only select all edges
        res = execute( "MATCH ()-[r]->() RETURN r" );
        containsRows( res, true, false,
                Row.of( TestEdge.from( List.of( "OWNER_OF" ) ) ),
                Row.of( TestEdge.from( List.of( "FRIEND_OF" ) ) ) );
    }


    @Test
    public void insertAllInOneTest() {
        execute( CREATE_COMPLEX_GRAPH_1 );
        GraphResult res = execute( "MATCH (n) RETURN n" );
        assertEquals( 3, res.getData().length );
        assertNode( res, 0 );

        res = execute( "MATCH ()-[r]-() RETURN r" );
        assertEdge( res, 0 );
        // double matches of same path is correct, as no direction is specified
        assertEquals( 4, res.getData().length );

        res = execute( "MATCH ()-[r]->() RETURN r" );
        assertEdge( res, 0 );
        // double matches of same path is correct, as no direction is specified
        assertEquals( 2, res.getData().length );
    }


    @Test
    public void insertAllInOneCircleTest() {
        execute( CREATE_COMPLEX_GRAPH_2 );
        GraphResult res = execute( "MATCH (n) RETURN n" );
        assertEquals( 3, res.getData().length );
        assertNode( res, 0 );

        res = execute( "MATCH ()-[r]-() RETURN r" );
        assertEdge( res, 0 );
        assertEquals( 6, res.getData().length );
    }


    @Test
    public void insertAdditionalEdgeTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( "MATCH (max:Person {name: 'Max'}), (hans:Person {name: 'Hans'})\n"
                + "CREATE (max)-[:KNOWS]->(hans)" );

        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );

        containsRows( res, true, true,
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ) );
    }


    @Test
    public void insertAdditionalEdgeOneSideTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( "MATCH (max:Person {name: 'Max'})\n"
                + "CREATE (max)-[:KNOWS]->(hans:Person {name: 'Hans'})" );

        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );

        containsRows( res, true, true,
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ) );

    }


    @Test
    public void insertAdditionalEdgeOneSideBothSideTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( "MATCH (max:Person {name: 'Max'})\n"
                + "CREATE (max)-[:KNOWS]->(hans:Person {name: 'Hans'})-[:KNOWS]->(peter:Person {name: 'Peter'})" );

        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );

        containsRows( res, true, true,
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ),
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ) );

    }

}
