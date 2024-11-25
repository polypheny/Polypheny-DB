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

package org.polypheny.db.cypher.clause.read;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.cypher.helper.TestPath;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;


@Slf4j
public class MatchTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    ///////////////////////////////////////////////
    ///////// MATCH
    ///////////////////////////////////////////////


    @Test
    public void simpleMatchTest() {
        GraphResult res = execute( "MATCH (n) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void simpleMatchNoneTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n:Villain) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void simpleMatchLabelTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n:Person) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, MAX );
    }


    @Test
    public void simpleMatchSinglePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n {name: 'Max'}) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, MAX );

        res = execute( "MATCH (n {name: 'Hans'}) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, HANS );

        res = execute( "MATCH (n {name: 'David'}) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void simpleMatchMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n {name: 'Kira', age: 21}) RETURN n" );
        assertNode( res, 0 );
        assertEmpty( res );

        res = execute( "MATCH (n {name: 'Kira', age: 3}) RETURN n" );
        assertNode( res, 0 );
        containsNodes( res, true, TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ), Pair.of( "age", 3 ), Pair.of( "type", "dog" ) ) );
    }

    ///////////////////////////////////////////////
    ///////// PROJECT
    ///////////////////////////////////////////////

    @Test
    public void noLabelPropertyTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        GraphResult res = execute( "MATCH (n) RETURN n.age" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 45 ) ),
                Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void simplePropertyTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (n:Person) RETURN n.name" );
        is( res, Type.ANY, 0 );
        containsIn( res, true, 0, "n.name", TestLiteral.from( "Max" ) );
    }


    @Test
    public void simpleMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (n:Animal) RETURN n.name, n.age" );
        is( res, Type.ANY, 0 ); // we never have guaranties for the types
        is( res, Type.ANY, 1 );
        containsIn( res, true, 0, "n.name", TestLiteral.from( "Kira" ) );
        containsIn( res, true, 1, "n.age", TestLiteral.from( "3" ) );
    }


    @Test
    public void mixedNodeAndPropertyProjectTest() {
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (n:Person) RETURN n, n.age" );
        assertNode( res, 0 );
        is( res, Type.ANY, 1 );
    }

    ///////////////////////////////////////////////
    ///////// EDGE
    ///////////////////////////////////////////////

    @Test
    public void simpleEdgeTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );
        assertEdge( res, 0 );
        containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
    }


    @Test
    public void emptyEdgeTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH ()-[r:KNOWS]->() RETURN r" );
        assertEdge( res, 0 );
        assertEmpty( res );
    }


    @Test
    public void singleEdgeFilterTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH ()-[r:KNOWS {since: 1995}]->() RETURN r" );
        assertEdge( res, 0 );
        assertEmpty( res );

        res = execute( "MATCH ()-[r:KNOWS {since: 1994}]->() RETURN r" );
        containsEdges( res, true, TestEdge.from( List.of( "KNOWS" ), Pair.of( "since", 1994 ) ) );
    }


    @Test
    public void singleEdgeFilterMatchNodeTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (n:Person)-[r:KNOWS {since: 1994}]->() RETURN n" );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );
    }


    @Test
    public void multipleHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        GraphResult res = execute( "MATCH (n)-[]->()-[]-() RETURN n" );
        containsNodes( res, true, TestEdge.from( List.of( "Person" ) ) );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:2000}]->()-[]-() RETURN r" );
        assertEdge( res, 0 );
        assertEmpty( res );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:1995}]->()-[]-() RETURN r" );
        assertEdge( res, 0 );
        containsEdges( res, true, TestEdge.from( List.of( "FRIEND_OF" ), Pair.of( "since", 1995 ) ) );
    }


    @Test
    public void repeatingHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        GraphResult res = execute( "MATCH ()-[*2]->(n) RETURN n" );
        assertNode( res, 0 );
        containsRows( res, true, true, Row.of( TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) ) );
    }


    @Test
    public void variableHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        GraphResult res = execute( "MATCH ()-[*1..2]->(n) RETURN n" );
        assertNode( res, 0 );
        containsRows( res, true, false,
                Row.of( KIRA ),
                Row.of( HANS_AGE ),
                Row.of( MAX ),
                // single hop match
                Row.of( TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) ),
                // double hop match
                Row.of( TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) ) );
    }

    ///////////////////////////////////////////////
    ///////// MIXED
    ///////////////////////////////////////////////

    @Test
    public void simpleMixedRelationshipTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (h:Person)-[r:OWNER_OF]-(p:Animal) RETURN h,p" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        containsIn( res, true, 0, MAX );
        containsIn( res, true, 1, TestNode.from(
                List.of( "Animal" ),
                Pair.of( "name", "Kira" ),
                Pair.of( "age", 3 ),
                Pair.of( "type", "dog" ) ) );
    }

    ///////////////////////////////////////////////
    ///////// "CROSS PRODUCT" MATCH
    ///////////////////////////////////////////////

    @Test
    public void simpleCrossProductMatchTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (p:Person),(n) RETURN p,n" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        containsRows( res, true, true, Row.of( MAX, MAX ) );
    }


    @Test
    public void simpleCrossProductBiMatchTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (p:Person),(n) RETURN p,n" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        containsRows( res, true, false,
                Row.of( MAX, MAX ),
                Row.of( HANS, MAX ),
                Row.of( MAX, HANS ),
                Row.of( HANS, HANS ) );
    }


    @Test
    public void simpleTriCrossProductMatchTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (p:Person),(n),(d) RETURN p,n,d" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        assertNode( res, 2 );
        containsRows( res, true, true, Row.of( MAX, MAX, MAX ) );
    }


    @Test
    public void simpleNeighborMatchTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_EDGE_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_EDGE_1 );

        GraphResult res = execute( "MATCH (p:Person {name:'Max'})-[]-(t) RETURN t" );
        assertNode( res, 0 );
        containsRows( res, true, false,
                Row.of( KIRA ),
                Row.of( TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ), Pair.of( "age", 31 ) ) ) );

        res = execute( "MATCH (p:Person {name:'Max'})-->(t) RETURN t " );
        containsRows( res, true, false,
                Row.of( KIRA ),
                Row.of( TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ), Pair.of( "age", 31 ) ) ) );
    }


    @Test
    public void triCrossProductMatchTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (p:Person),(n),(d) RETURN p,n,d" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        assertNode( res, 2 );

        containsRows( res, true, false,
                Row.of( MAX, MAX, MAX ),
                Row.of( HANS, MAX, MAX ),
                Row.of( MAX, HANS, MAX ),
                Row.of( HANS, HANS, MAX ),
                Row.of( MAX, MAX, HANS ),
                Row.of( HANS, MAX, HANS ),
                Row.of( MAX, HANS, HANS ),
                Row.of( HANS, HANS, HANS ) );
    }

    ///////////////////////////////////////////////
    ///////// PATH
    ///////////////////////////////////////////////

    @Test
    public void matchPathTest() {
        execute( SINGLE_EDGE_1 );

        GraphResult res = execute( "MATCH (m {name:'Max'}), (k {name:'Kira'}), p = (m)-[]-(k)\n"
                + "RETURN p" );
        containsRows( res, true, true,
                Row.of( TestPath.of(
                        TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                        TestEdge.from( List.of( "OWNER_OF" ) ),
                        KIRA ) ) );

        res = execute( "MATCH Path = (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'}) RETURN Path " );
        containsRows( res, true, true,
                Row.of( TestPath.of(
                        TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                        TestEdge.from( List.of( "OWNER_OF" ) ),
                        KIRA ) ) );
    }


    @Test
    public void returnPathsByAsteriskMatchTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH Path = () -[]->() RETURN *" );
        containsRows( res, true, false,
                Row.of( TestPath.of( MAX, TestEdge.from( List.of( "OWNER_OF" ) ), KIRA ) ) );
    }


    @Test
    public void returnNodesByAsteriskMatchTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (p)  RETURN *" );
        containsRows( res, true, false,
                Row.of( MAX ),
                Row.of( KIRA ) );
    }


    @Test
    public void returnBothSideByAsteriskMatchTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (p) -[]->(m) RETURN *" );
        containsRows( res, true, false,
                Row.of( MAX ),
                Row.of( KIRA ) );
    }


    @Test
    public void returnAllElementsByAsteriskMatchTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH Path = (p) -[r] ->(m) RETURN *" );
        containsRows( res, true, false,
                Row.of(
                        TestPath.of( MAX, TestEdge.from( List.of( "OWNER_OF" ) ), KIRA ),
                        MAX,
                        TestEdge.from( List.of( "OWNER_OF" ) ), KIRA ) );
    }


    @Test
    public void returnEdgesByAsteriskMatchTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH ()-[r]-() RETURN *" );
        containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
    }


    @Test
    public void shortestPathMatchTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (b:Person {name: 'Max'}), (a:Animal {name: 'Kira'}) "
                + "MATCH p = shortestPath((b)-[*]-(a)) "
                + "RETURN p" );
        containsRows( res, true, true,
                Row.of( TestPath.of(
                        TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                        TestEdge.from( List.of( "OWNER_OF" ) ),
                        KIRA ) ) );
    }

}
