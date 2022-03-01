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

public class DmlUpdateTest extends CypherTestTemplate {

    @Test
    public void updatePropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (a:Animal {name: 'Kira'})\n"
                        + "SET a.age = 4" );
    }


    @Test
    public void updatePropertyReturnTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (a:Animal {name: 'Kira'})\n"
                        + "SET a.age = 4\n"
                        + "RETURN a" );
    }


    @Test
    public void updateRelationshipExistingPropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (:Person {name:'Max Muster'})-[rel:OWNER_OF]->(a:Animal {name: 'Kira'})\n"
                        + "SET rel.since = 2018" );
    }


    @Test
    public void updateRelationshipNewPropertyTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (:Person {name:'Max Muster'})-[rel:OWNER_OF]->(a:Animal {name: 'Kira'})\n"
                        + "SET rel.status = 'fresh'" );
    }

}
