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
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.Result;

@Slf4j
public class MatchTest extends CypherTestTemplate {

    private static final String SINGLE_NODE_PERSON_1 = "CREATE (p:Person {name: 'Max'})";

    private static final String SINGLE_NODE_PERSON_2 = "CREATE (p:Person {name: 'Hans'})";

    private static final String SINGLE_NODE_ANIMAL = "CREATE (a:Animal {name:'Kira', age:3, type:'dog'})";

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
        assertNode( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void simpleMatchNoneTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        Result res = execute( "MATCH (n:Villain) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );

    }


    @Test
    public void simpleMatchLabelTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        Result res = execute( "MATCH (n:Person) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of(), Pair.of( "name", "Max" ) ) );

    }


    @Test
    public void simpleMatchSinglePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        Result res = execute( "MATCH (n {name: 'Max'}) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );

        res = execute( "MATCH (n {name: 'Hans'}) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ) ) );

        res = execute( "MATCH (n {name: 'David'}) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void simpleMatchMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        Result res = execute( "MATCH (n {name: 'Kira', age: 21}) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );

        res = execute( "MATCH (n {name: 'Kira', age: 3}) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ), Pair.of( "age", 3 ) ) );

    }

    ///////////////////////////////////////////////
    ///////// PROJECT
    ///////////////////////////////////////////////


    @Test
    public void simplePropertyTest() {
        execute( SINGLE_NODE_PERSON_1 );
        Result res = execute( "MATCH (n:Person) RETURN n.name" );
        assert is( res, Type.STRING, 0 );
        assert containsIn( res, true, 0, "n.name", TestLiteral.from( "Max" ) );
    }


    @Test
    public void simpleMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        Result res = execute( "MATCH (n:Animal) RETURN n.name, n.age" );
        assert is( res, Type.STRING, 0 );
        assert is( res, Type.STRING, 1 );
        assert containsIn( res, true, 0, "n.name", TestLiteral.from( "Kira" ) );
        assert containsIn( res, true, 1, "n.age", TestLiteral.from( "3" ) );
    }


    @Test
    public void mixedNodeAndPropertyProjectTest() {
        execute( SINGLE_NODE_ANIMAL );
        Result res = execute( "MATCH (n:Person)\n" +
                "RETURN n, n.age" );
        assertNode( res, 0 );
        assert is( res, Type.STRING, 1 );
    }

    ///////////////////////////////////////////////
    ///////// EDGE
    ///////////////////////////////////////////////


    @Test
    public void simpleEdgeTest() {
        execute( SINGLE_EDGE_1 );
        Result res = execute( "MATCH ()-[r]-() RETURN r" );
        assertEdge( res, 0 );
        containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
    }


    @Test
    public void emptyEdgeTest() {
        execute( SINGLE_EDGE_1 );
        Result res = execute( "MATCH ()-[r:KNOWS]->() RETURN r" );
        assertEdge( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void singleEdgeFilterTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        Result res = execute( "MATCH ()-[r:KNOWS {since: 1995}]->() RETURN r" );
        assertEdge( res, 0 );
        assertEmpty( res );

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
        assertEdge( res, 0 );
        assertEmpty( res );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:1995}]->()-[]-() RETURN r" );
        assertEdge( res, 0 );
        containsEdges( res, true, TestEdge.from( List.of( "FRIEND_OF" ), Pair.of( "since", 1995 ) ) );
    }

    ///////////////////////////////////////////////
    ///////// MIXED
    ///////////////////////////////////////////////


    @Test
    public void simpleMixedRelationshipTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        Result res = execute( "MATCH (h:Person)-[r:OWNER_OF]-(p:Animal) RETURN h,p" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        assert containsIn( res, true, 0, TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );
        assert containsIn( res, true, 1, TestNode.from(
                List.of( "Animal" ),
                Pair.of( "name", "Kira" ),
                Pair.of( "age", 3 ),
                Pair.of( "type", "dog" ) ) );

    }


    @Test
    public void simpleMixedDirectedRelationshipTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (p:Person)-[:OWNER_OF]->(:Animal)\n" +
                        "RETURN p" );

    }


}
