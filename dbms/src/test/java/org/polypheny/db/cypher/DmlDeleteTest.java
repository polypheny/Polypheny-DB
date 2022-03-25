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

public class DmlDeleteTest extends CypherTestTemplate {

    @Test
    public void simpleNodeDeleteTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (p:Person {name: 'Alice'})\n"
                + "DELETE p" );
        Result res = matchAndReturnAllNodes();
        assertEmpty( res );
    }


    @Test
    public void prohibitedDeleteTest() {
        try {
            Result res = CypherConnection.executeGetResponse(
                    "MATCH (a:Animal {name: 'Kira'})\n"
                            + "DELETE a" );
        } catch ( Exception e ) {

        }
    }


    @Test
    public void simpleRelationshipDeleteTest() {
        Result res = CypherConnection.executeGetResponse(
                "MATCH (:Person {name: 'Max Muster'})-[rel:OWNER_OF]->[:Animal {name: 'Kira'}] \n"
                        + "DELETE rel" );
    }


}
