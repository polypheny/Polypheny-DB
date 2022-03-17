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
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.Result;

@Slf4j
public class MatchTest extends CypherTestTemplate {

    private static String SINGLE_NODE_PERSON = "CREATE (p:Person {name: 'Max'})";

    private static String SINGLE_NODE_PERSON_1 = "CREATE (p:Person {name: 'Hans'})";

    private static String SINGLE_NODE_ANIMAL = "CREATE (a:ANIMAL {name:'Kira', age:3, type:'dog'})";

    private static String SINGLE_EDGE = "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})";

    private static String MULTIPLE_HOP_EDGE = "CREATE (n:Person)-[f:FRIEND_OF]->(p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira'})";


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
        execute( SINGLE_NODE_PERSON );

        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Villain) RETURN n" );
        isNode( res );
        isEmpty( res );

    }


    @Test
    public void simpleMatchLabelTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON );

        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Person) RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of(), Pair.of( "name", "Max" ) ) );

    }


    @Test
    public void simpleMatchSinglePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON );
        execute( SINGLE_NODE_PERSON_1 );

        Result res = CypherConnection.executeGetResponse(
                "MATCH (n {name: 'Max'})\n" +
                        "RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ) ) );

        res = CypherConnection.executeGetResponse(
                "MATCH (n {name: 'Hans'})\n" +
                        "RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of( "Person" ), Pair.of( "name", "Hans" ) ) );

        res = CypherConnection.executeGetResponse(
                "MATCH (n {name: 'David'})\n" +
                        "RETURN n" );
        isNode( res );
        isEmpty( res );
    }


    @Test
    public void simpleMatchMultiplePropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON );
        execute( SINGLE_NODE_PERSON_1 );

        Result res = CypherConnection.executeGetResponse(
                "MATCH (n {name: 'Kira', age: 21})\n" +
                        "RETURN n" );
        isNode( res );
        isEmpty( res );

        res = CypherConnection.executeGetResponse(
                "MATCH (n {name: 'Kira', age: 3})\n" +
                        "RETURN n" );
        isNode( res );
        containsNodes( res, true, TestNode.from( List.of( "Animal" ), Pair.of( "name", "Kira" ), Pair.of( "age", 3 ) ) );

    }

    ///////////////////////////////////////////////
    ///////// PROJECT
    ///////////////////////////////////////////////


    @Test
    public void simpleMultiplePropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Person)\n" +
                        "RETURN n.name, n.age" );

    }


    @Test
    public void simplePropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Person)\n" +
                        "RETURN n.name" );

    }

    ///////////////////////////////////////////////
    ///////// EDGE
    ///////////////////////////////////////////////


    @Test
    public void simpleRelationshipTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH ()-[r:FRIEND_OF]-()\n" +
                        "RETURN r" );

    }


    @Test
    public void simpleDirectedRelationshipTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH ()-[r:KNOWS]->()\n" +
                        "RETURN r" );

    }


    @Test
    public void simpleMixedRelationshipTest() {
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
