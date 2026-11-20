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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.cypher.helper.TestPath;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

@Slf4j
public class MatchTest extends CypherTestTemplate {


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    /// ////////////////////////////////////////////
    /// ////// MATCH
    /// ////////////////////////////////////////////
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
        assert containsNodes( res, true, MAX );

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
        assert containsNodes( res, true, TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ), Pair.of( "age", 3 ), Pair.of( "type", "dog" ) ) );

    }


    /// ////////////////////////////////////////////
    /// ////// PROJECT
    /// ////////////////////////////////////////////


    @Test
    public void simplePropertyTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (n:Person) RETURN n.name" );
        assert is( res, Type.ANY, 0 );
        assert containsIn( res, true, 0, "n.name", TestLiteral.from( "Max" ) );
    }


    @Test
    public void simpleMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (n:Animal) RETURN n.name, n.age" );
        assert is( res, Type.ANY, 0 ); // we never have guaranties for the types
        assert is( res, Type.ANY, 1 );
        assert containsIn( res, true, 0, "n.name", TestLiteral.from( "Kira" ) );
        assert containsIn( res, true, 1, "n.age", TestLiteral.from( "3" ) );
    }


    @Test
    public void mixedNodeAndPropertyProjectTest() {
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (n:Person) RETURN n, n.age" );
        assertNode( res, 0 );
        assert is( res, Type.ANY, 1 );
    }


    /// ////////////////////////////////////////////
    /// ////// EDGE
    /// ////////////////////////////////////////////


    @Test
    public void simpleEdgeTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH ()-[r]->() RETURN r" );
        assertEdge( res, 0 );
        assert containsEdges( res, true, TestEdge.from( List.of( "OWNER_OF" ) ) );
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
        assert containsEdges( res, true, TestEdge.from( List.of( "KNOWS" ), Pair.of( "since", 1994 ) ) );
    }


    @Test
    public void singleEdgeFilterMatchNodeTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (n:Person)-[r:KNOWS {since: 1994}]->() RETURN n" );
        assert containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );
    }


    @Test
    public void multipleHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        GraphResult res = execute( "MATCH (n)-[]->()-[]-() RETURN n" );
        containsNodes( res, true, TestNode.from( List.of( "Person" ) ) );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:2000}]->()-[]-() RETURN r" );
        assertEdge( res, 0 );
        assertEmpty( res );

        res = execute( "MATCH ()-[r:FRIEND_OF {since:1995}]->()-[]-() RETURN r" );
        assertEdge( res, 0 );
        assert containsEdges( res, true, TestEdge.from( List.of( "FRIEND_OF" ), Pair.of( "since", 1995 ) ) );
    }


    @Test
    public void repeatingHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        GraphResult res = execute( "MATCH ()-[*2]->(n) RETURN n" );
        assertNode( res, 0 );
        assert containsRows( res, true, true, Row.of( TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) ) );
    }


    @Test
    public void variableHopTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        execute( MULTIPLE_HOP_EDGE );

        GraphResult res = execute( "MATCH ()-[*1..2]->(n) RETURN n" );
        assertNode( res, 0 );
        assert containsRows( res, true, false,
                Row.of( KIRA ),
                Row.of( HANS_AGE ),
                Row.of( MAX ),
                // single hop match
                Row.of( TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) ),
                // double hop match
                Row.of( TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ) ) ) );
    }


    @Test
    void unboundedHopsReturnEveryNonemptyTrailWithoutReusingAnEdge() {
        createHopGraph();

        assertPaths( "MATCH p=(:Place {name:'A'})-[*]->(:Place) RETURN p", List.of(
                List.of( "A", "B" ), List.of( "A", "B", "C" ), List.of( "A", "B", "C", "A" ),
                List.of( "A", "B", "D" ), List.of( "A", "B", "E" ) ) );
        assertPaths( "MATCH p=(:Place {name:'Isolated'})-[*]->(:Place) RETURN p", List.of() );
    }


    @Test
    void openHopBoundsRespectDirectionAndRelationshipConstraints() {
        createHopGraph();

        assertPaths( "MATCH p=(:Place {name:'A'})<-[:ROAD*2.. {open:true}]-(:Place) RETURN p", List.of(
                List.of( "A", "C", "B" ), List.of( "A", "C", "B", "A" ) ) );
        assertPaths( "MATCH p=(:Place {name:'A'})-[:ROAD*1.. {open:true}]->(:Place) RETURN p", List.of(
                List.of( "A", "B" ), List.of( "A", "B", "C" ), List.of( "A", "B", "C", "A" ) ) );
        assertPaths( "MATCH p=(:Place {name:'A'})-[:ROAD*..2 {open:true}]->(:Place) RETURN p", List.of(
                List.of( "A", "B" ), List.of( "A", "B", "C" ) ) );
        assertPaths( "MATCH p=(:Place {name:'Isolated'})-[*0..]->(:Place) RETURN p", List.of( List.of( "Isolated" ) ) );
    }


    @Test
    void joinsAnUnboundedSegmentWithAFixedHopAndSharedEndpoint() {
        createHopGraph();

        assertPaths( "MATCH p=(a:Place {name:'A'})-[:ROAD]->()-[:ROAD*]->(a) RETURN p",
                List.of( List.of( "A", "B", "C", "A" ) ) );
    }


    private static void createHopGraph() {
        execute( """
                CREATE (a:Place {name:'A'})-[:ROAD {open:true}]->(b:Place {name:'B'})
                    -[:ROAD {open:true}]->(c:Place {name:'C'})-[:ROAD {open:true}]->(a),
                    (b)-[:RAIL {open:true}]->(:Place {name:'D'}),
                    (b)-[:ROAD {open:false}]->(:Place {name:'E'}), (:Place {name:'Isolated'})
                """ );
    }


    private static void assertPaths( String query, List<List<String>> expected ) {
        List<List<String>> names = Arrays.stream( execute( query ).getData() )
                .map( row -> PolyValue.fromTypedJson( row[0], PolyValue.class ).asPath() )
                .map( path -> {
                    assertEquals( path.getNodes().size() - 1, path.getEdges().size() );
                    assertEquals( path.getEdges().size(), path.getEdges().stream().map( edge -> edge.id ).distinct().count() );
                    return path.getNodes().stream()
                            .map( node -> node.getProperties().get( PolyString.of( "name" ) ).asString().getValue() )
                            .toList();
                } )
                .toList();
        assertEquals( expected.size(), names.size(), query );
        assertEquals( Set.copyOf( expected ), Set.copyOf( names ), query );
    }


    /// ////////////////////////////////////////////
    /// ////// MIXED
    /// ////////////////////////////////////////////


    @Test
    public void simpleMixedRelationshipTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );
        GraphResult res = execute( "MATCH (h:Person)-[r:OWNER_OF]-(p:Animal) RETURN h,p" );
        assertNode( res, 0 );
        assertNode( res, 1 );
        assert containsIn( res, true, 0, MAX );
        assert containsIn( res, true, 1, TestNode.from(
                List.of( "Animal" ),
                Pair.of( "name", "Kira" ),
                Pair.of( "age", 3 ),
                Pair.of( "type", "dog" ) ) );

    }


    /// ////////////////////////////////////////////
    /// ////// "CROSS PRODUCT" MATCH
    /// ////////////////////////////////////////////


    @Test
    public void simpleCrossProductMatchTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (p:Person),(n) RETURN p,n" );
        assertNode( res, 0 );
        assertNode( res, 1 );

        assert containsRows( res, true, true, Row.of( MAX, MAX ) );
    }


    @Test
    public void simpleCrossProductBiMatchTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        GraphResult res = execute( "MATCH (p:Person),(n) RETURN p,n" );
        assertNode( res, 0 );
        assertNode( res, 1 );

        assert containsRows( res, true, false,
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

        assert containsRows( res, true, true, Row.of( MAX, MAX, MAX ) );

    }


    @Test
    public void simpleNeighborMatchTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_EDGE_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (p:Person {name:'Max'})-[]-(t) RETURN t" );
        assertNode( res, 0 );

        assert containsRows( res, true, false,
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

        assert containsRows( res, true, false,
                Row.of( MAX, MAX, MAX ),
                Row.of( HANS, MAX, MAX ),
                Row.of( MAX, HANS, MAX ),
                Row.of( HANS, HANS, MAX ),
                Row.of( MAX, MAX, HANS ),
                Row.of( HANS, MAX, HANS ),
                Row.of( MAX, HANS, HANS ),
                Row.of( HANS, HANS, HANS ) );
    }


    /// ////////////////////////////////////////////
    /// ////// PATH
    /// ////////////////////////////////////////////


    @Test
    public void matchPathTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (m {name:'Max'}), (k {name:'Kira'}), p = (m)-[]-(k)\n"
                + "RETURN p" );
        assert containsRows( res, true, true,
                Row.of( TestPath.of(
                        TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ),
                        TestEdge.from( List.of( "OWNER_OF" ) ),
                        KIRA ) ) );
    }


}
