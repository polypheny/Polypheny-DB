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

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.webui.models.Result;

@Slf4j
public class MatchTest extends CypherTestTemplate {

    ///////////////////////////////////////////////
    ///////// NODE
    ///////////////////////////////////////////////
    @Test
    public void simpleMatchTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (n)\n" +
                        "RETURN n" );

    }

    @Test
    public void simpleMatchLabelTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Person)\n" +
                        "RETURN n" );

    }

    @Test
    public void simpleMatchSinglePropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Person {name: 'Max Muster'})\n" +
                        "RETURN n" );

    }

    @Test
    public void simpleMatchMultiplePropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (n:Person {name: 'Max Muster', age: 3})\n" +
                        "RETURN n" );

    }

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
    ///////// RELATIONSHOP
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
