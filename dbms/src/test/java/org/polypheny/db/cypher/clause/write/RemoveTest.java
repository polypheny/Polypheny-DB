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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.webui.models.results.GraphResult;


@Slf4j
public class RemoveTest extends CypherTestTemplate {

    protected static final String SINGLE_NODE_PERSON_EMPLOYEE = "CREATE (n:Person:Employee) ";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void labelRemoveTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (n:Person {name: 'Max'})\n"
                + "REMOVE n:Person " );
        GraphResult res = execute( "MATCH (n :Person) RETURN n" );
        assertEquals( 0, res.getData().length );
    }


    @Test
    public void returnWithRemoveTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (n:Person {name: 'Max'})\n"
                + "REMOVE n:Person RETURN n  " );
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void multipleLabelsRemoveTest() {
        execute( SINGLE_NODE_PERSON_EMPLOYEE );

        GraphResult res = matchAndReturnAllNodes();
        containsNodes( res, true, TestNode.from( List.of( "Person", "Employee" ) ) );

        execute( "MATCH (n) REMOVE n:Person:Employee " );
        res = execute( "MATCH (n :Person:Employee) RETURN n" );
        assertEquals( 0, res.getData().length );
    }


    @Test
    public void singlePropertyNodeRemoveTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( "MATCH (n : Person {name: 'Ann'}) REMOVE a.age " );
        GraphResult res = execute( "MATCH (n : Person) RETURN n.age  , n.name" );
        containsRows( res, true, true, Row.of( TestLiteral.from( null ), TestLiteral.from( "Ann" ) ) );
    }


    @Test
    public void multiplePropertiesRemoveTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( """
                MATCH (n:Person {name: 'Ann'})
                REMOVE n.age, n.depno
                """ );
        GraphResult res = execute( "MATCH (n : Person) RETURN n.age  , n.depno , n.name " );
        containsRows( res, true, true, Row.of( TestLiteral.from( null ), TestLiteral.from( null ), TestLiteral.from( "Ann" ) ) );
    }


    @Test
    public void allPropertiesNodeRemoveTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (n:Person) SET n = {}" );
        GraphResult res = matchAndReturnAllNodes();
        assertEquals( 0, res.getData().length );
    }


    @Test
    public void singlePropertyRelationshipRemoveTest() {
        execute( SINGLE_EDGE_2 );
        execute( "MATCH(p1:Person)-[rel:KNOWS]->(p2:Person)\n"
                + "REMOVE rel.since" );
        GraphResult res = execute( "MATCH ()-[r:KNOWS]->() RETURN r.since" );
        containsRows( res, true, true, Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void multiplePropertiesRelationshipRemoveTest() {
        execute( "Create (p:Person {name: 'Max'})-[rel:KNOWS {since: 1994 , relation : 'friend'}]->(a:Person {name:'Hans', age:31})" );
        execute( "MATCH(p1:Person)-[rel:KNOWS]->(p2:Person)\n"
                + "REMOVE rel.since , rel.relation" );
        GraphResult res = execute( "MATCH ()-[r:KNOWS]->() RETURN r.since , r.relation" );
        containsRows( res, true, true,
                Row.of( TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void allPropertiesRelationshipRemoveTest() {
        execute( SINGLE_EDGE_2 );
        execute( "MATCH ()-[r:KNOWS]->() RETURN SET r = {}" );
        GraphResult res = execute( "MATCH ()-[r:KNOWS]->() RETURN r.since" );
        containsRows( res, true, true, Row.of( TestLiteral.from( null ) ) );
    }

}
