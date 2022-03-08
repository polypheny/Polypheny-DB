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

import org.junit.Test;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.webui.models.Result;

public class DmlInsertTest extends CypherTestTemplate {

    @Test
    public void insertNodeTest() {
        Result res = execute( "CREATE (p:Person {name: 'Max Muster'})" );
    }


    @Test
    public void insertReturnNodeTest() {
        Result res = execute(
                "CREATE (p:Person {name: 'Max Muster'})\n"
                        + "RETURN p" );
    }


    @Test
    public void insertRelationshipTest() {
        Result res = execute(
                "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:ANIMAL {name:'Kira', age:3, type:'dog'})" );
    }

    @Test
    public void insertAdditionalRelationshipTest() {
        Result createPerson = CypherConnection.executeGetResponse(
                "CREATE (p:Person {name: 'Max'})" );

        Result createAnimal = CypherConnection.executeGetResponse(
                "CREATE (a:ANIMAL {name:'Kira', age:3, type:'dog'})" );

        Result createRel = CypherConnection.executeGetResponse(
                "MATCH (max:Person {name: 'Max'})\n"
                        + "MATCH (kira:ANIMAL {name: 'Kira'})\n"
                        + "CREATE (max)-[rel:OWNER_OF]->(kira)" );
    }

}
