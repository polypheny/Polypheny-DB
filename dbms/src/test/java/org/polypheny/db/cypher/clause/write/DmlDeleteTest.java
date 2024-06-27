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
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

public class DmlDeleteTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleEmptyNodeDeleteTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (p:Person)\n"
                + "DELETE p" );
        GraphResult res = matchAndReturnAllNodes();
        assertEmpty( res );
    }


    @Test
    public void simpleNodeDeleteTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (p:Person {name: 'Max'})\n"
                + "DELETE p" );
        GraphResult res = matchAndReturnAllNodes();
        assertEmpty( res );
    }


    @Test
    public void simpleFilteredNodeDeleteTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( "MATCH (p:Person {name: 'Max'})\n"
                + "DELETE p" );
        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, false,
                Row.of( TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ) ) ) );
    }


    @Test
    public void twoNodeDeleteTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( "MATCH (p:Person {name: 'Max'}), (h:Person {name: 'Hans'})\n"
                + "DELETE p, h" );
        GraphResult res = matchAndReturnAllNodes();
        assertEmpty( res );
    }


    @Test
    public void simpleRelationshipDeleteTest() {
        execute( SINGLE_EDGE_1 );
        execute( "MATCH (:Person {name: 'Max'})-[rel:OWNER_OF]->(:Animal {name: 'Kira'}) \n"
                + "DELETE rel" );

        GraphResult res = matchAndReturnAllNodes();
        assert containsRows( res, true, false,
                Row.of( TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) ),
                Row.of( TestNode.from(
                        List.of( "Animal" ),
                        Pair.of( "name", "Kira" ),
                        Pair.of( "age", 3 ),
                        Pair.of( "type", "dog" ) ) ) );

    }

    @Test
    public void relationshipWithPropertiesDeleteTest() {
        execute( SINGLE_EDGE_2 );
        execute( "MATCH (p:Person) -[rel:OWNER_OF]->(A:Animal) \n"
                + "DELETE rel" );

        GraphResult res =  execute( "MATCH (p:Person) -[rel:OWNER_OF]->(A:Animal)");
         assertEmpty( res  );

    }


    @Test
    public void pathDeleteTest()
    {

        execute( SINGLE_EDGE_1 );

         execute( "MATCH p =  (person:Person {name: 'Max'})-[rel:OWNER_OF]->( animal :Animal {name: 'Kira'})\n"
                 + "DELETE p\n" );

       GraphResult res =  matchAndReturnAllNodes() ;
       GraphResult edges = execute(  "MATCH (p:Person) -[rel:OWNER_OF]->(A:Animal)");
       assert  res.getData().length == 0 && edges.getData().length == 0 ;


    }

    @Test
    public void  NodeWithAllRelationshipsDeleteTest()
    {
        execute( SINGLE_EDGE_2 );
        execute( "MATCH (n:Person {name: 'MAX'})\n"
                + "DETACH DELETE n" );


        GraphResult res =  execute( "MATCH (p:Person) -[rel:OWNER_OF]->(A:Animal)");
        assert  res.getData().length == 0 ;

    }
    @Test
    public void allNodesAndRelationshipsDeleteTest ()
    {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( "MATCH (n)\n"
                + "DETACH DELETE n" ) ;


        GraphResult res =  matchAndReturnAllNodes() ;
        GraphResult edges = execute(  "MATCH (p:Person) -[rel:OWNER_OF]->(A:Animal)");
        assert  res.getData().length == 0 && edges.getData().length == 0 ;

    }




}
