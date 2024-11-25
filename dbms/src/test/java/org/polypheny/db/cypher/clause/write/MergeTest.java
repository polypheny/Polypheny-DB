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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;


@Slf4j
public class MergeTest extends CypherTestTemplate {

    protected static final String SINGLE_NODE_PERSON_COMPLEX_4 = "CREATE (charlie:Person {name: 'Charlie Sheen', bornIn: 'New York', chauffeurName: 'John Brown'})";
    protected static final String SINGLE_NODE_PERSON_COMPLEX_5 = "CREATE (martin:Person {name: 'Martin Sheen', bornIn: 'Ohio', chauffeurName: 'Bob Brown'})";
    protected static final String SINGLE_NODE_PERSON_COMPLEX_6 = "CREATE (michael:Person {name: 'Michael Douglas', bornIn: 'New Jersey', chauffeurName: 'John Brown'})";

    protected static final String SINGLE_NODE_MOVIE = "CREATE (wallStreet:Movie {title: 'Wall Street'})";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void singleNodeMergeTest() {
        execute( "MERGE (P)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of() ) );
    }


    @Test
    public void returnSingleNodeMergeTest() {
        GraphResult res = execute( "MERGE (P) Return P " );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of() ) );
    }


    @Test
    public void singleNodeWithLabelMergeTest() {
        // new node
        execute( "MERGE (p:Person)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Person" ) ) );
        // System.out.print( res.getData().length );  return zero

        // exist node
        execute( "MERGE (n:Person)" );
        res = matchAndReturnAllNodes();

        // num of nodes should be one
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void singleNodeWithMultipleLabelsMergeTest() {
        // new node
        execute( "MERGE (robert:Critic:Viewer)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Critic", "Viewer" ) ) );

        // exist node
        execute( "MERGE (robert:Critic:Viewer)" );
        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void singleNodeWithSinglePropertyMergeTest() {
        execute( "MERGE (charlie {name: 'Charlie Sheen'})" );

        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of(),
                Pair.of( "name", "Charlie Sheen" ) ) );

        execute( "MERGE (charlie {name: 'Charlie Sheen'})" );
        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void singleNodeWithMultiplePropertiesMergeTest() {
        execute( "MERGE (charlie {name: 'Charlie Sheen', age: 10})" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of(),
                Pair.of( "name", "Charlie Sheen" ),
                Pair.of( "age", 10 ) ) );

        execute( "MERGE (charlie {name: 'Charlie Sheen', age: 10})" );
        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void singleNodeWithPropertiesAndLabelMergeTest() {
        execute( "MERGE (michael:Person {name: 'Michael Douglas' , age : 10})" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Person" ),
                Pair.of( "name", "Michael Douglas" ),
                Pair.of( "age", 10 ) ) );

        execute( "MERGE (michael:Person {name: 'Michael Douglas' , age : 10})" );
        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void singleNodeDerivedFromExistingNodeMergeTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_4 );
        execute( SINGLE_NODE_PERSON_COMPLEX_5 );
        execute( SINGLE_NODE_PERSON_COMPLEX_6 );
        execute( """
                MATCH (person:Person)
                MERGE (location:Location {name: person.bornIn})
                """ );

        GraphResult res = execute( "MATCH (location:Location) RETURN location.name" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( "New York" ) ),
                Row.of( TestLiteral.from( "Ohio" ) ),
                Row.of( TestLiteral.from( "New Jersey" ) ) );

        execute( """
                MATCH (person:Person)
                MERGE (location:Location {name: person.bornIn})
                """ );

        res = execute( "MATCH (location:Location) RETURN location" );
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void createWithMergeTest() {
        execute( "MERGE (keanu:Person {name: 'Keanu Reeves', bornIn: 'Beirut', chauffeurName: 'Eric Brown'})\n"
                + "ON CREATE SET keanu.age = 20"
        );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true,
                (TestNode.from( List.of( "Person" ), Pair.of( "name", "Keanu Reeves" ), Pair.of( "age", 20 ) )) );
        execute( "MERGE (keanu:Person {name: 'Keanu Reeves', bornIn: 'Beirut', chauffeurName: 'Eric Brown'})\n"
                + "ON CREATE SET keanu.age = 20"
        );
        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void matchWithMergeTest() {
        execute( """
                MERGE (person:Person{ found : false})
                ON MATCH SET person.found = true
                """ );

        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from(
                List.of( "Person" ),
                Pair.of( "found", false ) ) );

        execute( """
                MERGE (person:Person{ found : false})
                ON MATCH SET person.found = true
                """ );

        res = matchAndReturnAllNodes();
        containsNodes( res, true, TestNode.from(
                List.of( "Person" ),
                Pair.of( "found", true ) ) );

        assertEquals( 1, res.getData().length );
    }


    @Test
    public void createOrMatchNodeWithMergeTest() {
        // Create new node
        execute( "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 ON MATCH SET n.age = 35" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from(
                List.of( "Person" ),
                Pair.of( "age", 30 ),
                Pair.of( "name", "Alice" ) ) );
        // Updated the Matched  node
        execute( "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 ON MATCH SET n.age = 35 " );
        res = matchAndReturnAllNodes();

        containsNodes( res, true, TestNode.from(
                List.of( "Person" ),
                Pair.of( "age", 35 ),
                Pair.of( "name", "Alice" ) ) );

        assertEquals( 1, res.getData().length );
    }


    @Test
    public void singleRelationshipMergeTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_4 );
        execute( SINGLE_NODE_MOVIE );

        execute( """
                MATCH
                  (charlie:Person {name: 'Charlie Sheen'}),
                  (wallStreet:Movie {title: 'Wall Street'})
                MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
                """ );

        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Charlie Sheen" ) ),
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Wall Street" ) ) );

        res = execute( "MATCH ()-[r]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "ACTED_IN" ) ) );
        execute( """
                MATCH
                  (charlie:Person {name: 'Charlie Sheen'}),
                  (wallStreet:Movie {title: 'Wall Street'})
                MERGE (charlie)-[r:ACTED_IN]->(wallStreet)
                """ );

        res = execute( "MATCH ()-[r]->() RETURN r" );
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void multipleRelationshipsMergeTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_4 );
        execute( SINGLE_NODE_PERSON_COMPLEX_5 );

        execute( """
                MATCH
                  (charlie:Person {name: 'Charlie Sheen'}),
                  (martin:Person {name: 'Martin Sheen'})
                MERGE (oliver)-[:DIRECTED]->(movie:Movie)<-[:DIRECTED]-(reiner)
                """ );

        GraphResult res = execute( """
                MATCH (p1:Person)-[:DIRECTED]->(movie:Movie)<-[:DIRECTED]-(p2:Person)
                RETURN p1, p2, movie
                """ );

        containsNodes( res, true,
                TestNode.from( Pair.of( "name", "Charlie Sheen" ) ),
                TestNode.from( Pair.of( "name", "Martin Sheen" ) ),
                TestNode.from( List.of( "Movie" ) ) );

        execute( """
                MATCH
                  (charlie:Person {name: 'Charlie Sheen'}),
                  (martin:Person {name: 'Martin Sheen'})
                MERGE (oliver)-[:DIRECTED]->(movie:Movie)<-[:DIRECTED]-(reiner)
                """ );

        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void undirectedRelationshipMergeTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_4 );
        execute( SINGLE_NODE_PERSON_COMPLEX_5 );

        execute( """
                MATCH
                  (charlie:Person {name: 'Charlie Sheen'}),
                  (martin:Person {name: 'Martin Sheen'})
                MERGE (charlie)-[r:KNOWS]-(oliver)
                """ );

        GraphResult res = execute( "MATCH (p1:Person)-[r:KNOWS]-(p2:Person)\n"
                + "RETURN KNOWS" );

        containsEdges( res, true, TestEdge.from( List.of( "KNOWS" ) ) );

        execute( """
                MATCH
                  (charlie:Person {name: 'Charlie Sheen'}),
                  (martin:Person {name: 'Martin Sheen'})
                MERGE (charlie)-[r:KNOWS]-(oliver)
                """ );

        res = execute( "MATCH (p1:Person)-[r:KNOWS]-(p2:Person)\n"
                + "RETURN KNOWS" );

        assertEquals( 1, res.getData().length );
    }


    @Test
    public void relationshipOnTwoExistingNodeMergeTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_4 );
        execute( SINGLE_NODE_PERSON_COMPLEX_5 );
        execute( SINGLE_NODE_PERSON_COMPLEX_6 );

        execute( """
                MATCH (person:Person)
                MERGE (location:Location {name: person.bornIn})
                MERGE (person)-[r:BORN_IN]->(location)
                """ );

        GraphResult res = execute( "MATCH (location:Location) RETURN location.name" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( "New York" ) ),
                Row.of( TestLiteral.from( "Ohio" ) ),
                Row.of( TestLiteral.from( "New Jersey" ) ) );

        res = execute( "MATCH ()-[BORN_IN]->() RETURN BORN_IN" );
        assertEquals( 3, res.getData().length );
        containsEdges( res, true, TestEdge.from( List.of( "BORN_IN" ) ) );

        execute( """
                MATCH (person:Person)
                MERGE (location:Location {name: person.bornIn})
                MERGE (person)-[r:BORN_IN]->(location)
                """ );

        GraphResult edges = execute( "MATCH ()-[BORN_IN]->() RETURN BORN_IN" );
        GraphResult nodes = execute( "MATCH (location:Location) RETURN Location  " );

        assertTrue( edges.getData().length == 3 && nodes.getData().length == 3 );
    }


    @Test
    public void relationshipOnExistingNodeAndMergeNodeDerivedFromAnodeProperty() {
        execute( SINGLE_NODE_PERSON_COMPLEX_4 );
        execute( SINGLE_NODE_PERSON_COMPLEX_5 );
        execute( SINGLE_NODE_PERSON_COMPLEX_6 );

        execute( """
                MATCH (person:Person)
                MERGE (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur {name: person.chauffeurName})
                """ );

        GraphResult res = execute( "MATCH ( chauffeur :Chauffeur) Return Chauffeur.name" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( "John Brown" ) ),
                Row.of( TestLiteral.from( "Bob Brown" ) ),
                Row.of( TestLiteral.from( "John Brown" ) ) );

        res = execute( "MATCH ()-[HAS_CHAUFFEUR]->() RETURN HAS_CHAUFFEUR" );
        assertEquals( 3, res.getData().length );
        containsEdges( res, true, TestEdge.from( List.of( "HAS_CHAUFFEUR" ) ) );
        execute( """
                MATCH (person:Person)
                MERGE (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur {name: person.chauffeurName})
                """ );
        GraphResult edges = execute( "MATCH ()-[HAS_CHAUFFEUR]->() RETURN HAS_CHAUFFEUR" );
        GraphResult nodes = execute( "MATCH (n:Chauffeur) RETURN Chauffeur" );
        assertTrue( edges.getData().length == 3 && nodes.getData().length == 3 );
    }

}
