/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.Result;

@Slf4j
public class MatchTest extends CypherTestTemplate {

    private static final String SINGLE_NODE_PERSON_1 = "CREATE (p:Person {name: 'Max'})";

    private static final String SINGLE_NODE_PERSON_2 = "CREATE (p:Person {name: 'Hans'})";

    private static final String SINGLE_NODE_ANIMAL = "CREATE (a:ANIMAL {name:'Kira', age:3, type:'dog'})";

    private static final String SINGLE_EDGE_1 = "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})";

    private static final String SINGLE_EDGE_2 = "CREATE (p:Person {name: 'Max'})-[rel:KNOWS {since: 1994}]->(a:Person {name:'Hans', age:31})";

    private static final String MULTIPLE_HOP_EDGE = "CREATE (n:Person)-[f:FRIEND_OF {since: 1995}]->(p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira'})";


    @Before
    public void reset() {
        tearDown();
        createSchema();
    }


    ///////////////////////////////////////////////
    ///////// MATCH
    ///////////////////////////////////////////////
    @Test
    public void simpleMatchTest() {
        Result res = execute( "MATCH (n)\nRETURN n" );
        assert isNode( res );
        assert isEmpty( res );
    }


    @Test
    public void simpleMatchNoneTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        Result res = execute( "MATCH (n:Villain) RETURN n" );
        isNode( res );
        isEmpty( res );

    }


    @Test
    public void simpleMatchLabelTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        Result res = execute( "MATCH (n:Person) RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of(), Pair.of( "name", "Max" ) ) );

    }


    @Test
    public void simpleMatchSinglePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        Result res = execute( "MATCH (n {name: 'Max'}) RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );

        res = execute( "MATCH (n {name: 'Hans'}) RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ) ) );

        res = execute( "MATCH (n {name: 'David'}) RETURN n" );
        isNode( res );
        isEmpty( res );
    }


    @Test
    public void simpleMatchMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        Result res = execute( "MATCH (n {name: 'Kira', age: 21}) RETURN n" );
        isNode( res );
        isEmpty( res );

        res = execute( "MATCH (n {name: 'Kira', age: 3}) RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ), Pair.of( "age", 3 ) ) );

    }

    ///////////////////////////////////////////////
    ///////// PROJECT
    ///////////////////////////////////////////////


    @Test
    public void simpleMultiplePropertyTest() {
        Result res = execute( "MATCH (n:Person)\n" +
                "RETURN n.name, n.age" );

    }


    @Test
    public void simplePropertyTest() {
        Result res = execute( "MATCH (n:Person)\n" +
                "RETURN n.name" );

    }

    ///////////////////////////////////////////////
    ///////// EDGE
    ///////////////////////////////////////////////


    @Test
    public void simpleEdgeTest() {
        execute( SINGLE_EDGE_1 );
        Result res = execute( "MATCH ()-[r]-() RETURN r" );
        isEdge( res );
        containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
    }


    @Test
    public void emptyEdgeTest() {
        execute( SINGLE_EDGE_1 );
        Result res = execute( "MATCH ()-[r:KNOWS]->() RETURN r" );
        isEdge( res );
        isEmpty( res );
    }


    @Test
    public void singleEdgeFilterTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        Result res = execute( "MATCH ()-[r:KNOWS {since: 1995}]->() RETURN r" );
        isEdge( res );
        isEmpty( res );

        res = execute( "MATCH ()-[r:KNOWS {since: 1994}]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "KNOWS" ), Pair.of( "since", 1994 ) ) );
    }


    @Test
    public void singleEdgeFilterMatchNodeTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        Result res = execute( "MATCH (n:Person)-[r:KNOWS {since: 1994}]->() RETURN n" );
        containsNodes( res, true, TestEdge.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );
    }


    @Test
    public void multipleHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        Result res = execute( "MATCH (n)-[]->()-[]-() RETURN n" );
        containsNodes( res, true, TestEdge.from( List.of( "Person" ) ) );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:2000}]->()-[]-() RETURN r" );
        isEdge( res );
        isEmpty( res );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:1995}]->()-[]-() RETURN r" );
        isEdge( res );
        containsEdges( res, true, TestEdge.from( List.of( "FRIEND_OF" ), Pair.of( "since", 1995 ) ) );
    }

    ///////////////////////////////////////////////
    ///////// EDGE
    ///////////////////////////////////////////////


    @Test
    public void simpleMixedRelationshipTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        Result res = CypherConnection.executeGetResponse(
                "MATCH (:Person)-[r:LIVE_TOGETHER]-(:ANIMAL)\n" +
                        "RETURN r" );

    }


    @Test
    public void simpleMixedDirectedRelationshipTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (p:Person)-[:OWNER_OF]->(:ANIMAL)\n" +
                        "RETURN p" );

    }


    @Test
    public void simpleWholeRelationshipTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (p:Person)-[r:OWNER_OF]->(a:ANIMAL)\n" +
                        "RETURN p, r, a" );

    }

}
