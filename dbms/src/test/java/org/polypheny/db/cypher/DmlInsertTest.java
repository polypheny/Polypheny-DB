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

package org.polypheny.db.cypher;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

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
    public void insertEmptyNode() {
        execute( "CREATE (p)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        assert containsNodes( res, true, TestNode.from( List.of() ) );
    }


    @Test
    public void insertNodeTest() {
        execute( "CREATE (p:Person {name: 'Max Muster'})" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        assert containsNodes( res, true, TestNode.from( Pair.of( "name", "Max Muster" ) ) );
    }


    @Test
    public void insertTwoNodeTest() {
        execute( CREATE_PERSON_MAX );
        execute( CREATE_PERSON_MAX );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        assert containsNodes( res, true,
                TestNode.from( Pair.of( "name", "Max Muster" ) ),
                TestNode.from( Pair.of( "name", "Max Muster" ) ) );
    }


    @Test
    public void insertMultipleNodesTest() {
        execute( "CREATE (p),(n),(m)" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        assert containsNodes( res, true, TestNode.from(), TestNode.from(), TestNode.from() );
    }


    @Test
    public void insertPropertyTypeTest() {
        execute( "CREATE (p:Person {name: 'Max Muster', age: 13, height: 185.3, nicknames: [\"Maxi\",\"Musti\"]})" );
        GraphResult res = matchAndReturnAllNodes();
        assertNode( res, 0 );
        assert containsNodes( res, true,
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
    }


    @Test
    public void insertSingleHopPathTest() {
        execute( "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})" );
        GraphResult res = matchAndReturnAllNodes();
        assert containsNodes( res, true,
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
        assert containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
    }


    @Test
    public void insertMultipleHopPathTest() {
        execute( "CREATE (n:Person)-[f:FRIEND_OF]->(p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira'})" );

        // only select all nodes
        GraphResult res = matchAndReturnAllNodes();
        assert containsNodes( res, true,
                TestNode.from( List.of( "Person" ) ),
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) );

        // only select all edges
        res = execute( "MATCH ()-[r]->() RETURN r" );
        assert containsRows( res, true, false,
                Row.of( TestEdge.from( List.of( "OWNER_OF" ) ) ),
                Row.of( TestEdge.from( List.of( "FRIEND_OF" ) ) ) );
    }


    @Test
    public void insertAllInOneTest() {
        execute( CREATE_COMPLEX_GRAPH_1 );
        GraphResult res = execute( "MATCH (n) RETURN n" );
        assert res.getData().length == 3;
        assertNode( res, 0 );

        res = execute( "MATCH ()-[r]-() RETURN r" );
        assertEdge( res, 0 );
        // double matches of same path is correct, as no direction is specified
        assert res.getData().length == 4;

        res = execute( "MATCH ()-[r]->() RETURN r" );
        assertEdge( res, 0 );
        // double matches of same path is correct, as no direction is specified
        assert res.getData().length == 2;
    }


    @Test
    public void insertAllInOneCircleTest() {
        execute( CREATE_COMPLEX_GRAPH_2 );
        GraphResult res = execute( "MATCH (n) RETURN n" );
        assert res.getData().length == 3;
        assertNode( res, 0 );

        res = execute( "MATCH ()-[r]-() RETURN r" );
        assertEdge( res, 0 );
        assert res.getData().length == 6;
    }


    @Test
    public void insertAdditionalEdgeTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( "MATCH (max:Person {name: 'Max'}), (hans:Person {name: 'Hans'})\n"
                + "CREATE (max)-[:KNOWS]->(hans)" );

        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );

        assert containsRows( res, true, true,
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ) );
    }


    @Test
    public void insertAdditionalEdgeOneSideTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( "MATCH (max:Person {name: 'Max'})\n"
                + "CREATE (max)-[:KNOWS]->(hans:Person {name: 'Hans'})" );

        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );

        assert containsRows( res, true, true,
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ) );

    }


    @Test
    public void insertAdditionalEdgeOneSideBothSideTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        execute( "MATCH (max:Person {name: 'Max'})\n"
                + "CREATE (max)-[:KNOWS]->(hans:Person {name: 'Hans'})-[:KNOWS]->(peter:Person {name: 'Peter'})" );

        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );

        assert containsRows( res, true, true,
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ),
                Row.of( TestEdge.from( List.of( "KNOWS" ) ) ) );

    }

}
