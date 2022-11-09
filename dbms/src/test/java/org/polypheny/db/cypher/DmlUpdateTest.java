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
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.Result;

public class DmlUpdateTest extends CypherTestTemplate {

    @Before
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void updatePropertyTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a.age = 25" );

        Result res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ), Pair.of( "age", 25 ) ) ) );
    }


    @Test
    public void updateLabelTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a:Swiss" );

        Result res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person", "Swiss" ), Pair.of( "name", "Max" ) ) ) );
    }


    @Test
    public void updateLabelsTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a:Swiss:German" );

        Result res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person", "Swiss", "German" ), Pair.of( "name", "Max" ) ) ) );
    }


    @Test
    public void updateVariablesReplaceTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a = {} " );

        Result res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from( List.of( "Person" ) ) ) );
    }


    @Test
    public void updateVariablesIncrementTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( "MATCH (a:Person {name: 'Max'})\n"
                + "SET a = { age: 13, job: 'Developer'} " );

        Result res = matchAndReturnAllNodes();
        assert containsRows( res, true, true,
                Row.of( TestNode.from(
                        List.of( "Person" ),
                        Pair.of( "age", 13 ),
                        Pair.of( "job", "Developer" ) ) ) );
    }


    @Test
    @Ignore
    public void updatePropertyReturnTest() {
        execute( "MATCH (a:Animal {name: 'Kira'})\n"
                + "SET a.age = 4\n"
                + "RETURN a" );
    }


    @Test
    @Ignore
    public void updateRelationshipExistingPropertyTest() {
        execute( "MATCH (:Person {name:'Max Muster'})-[rel:OWNER_OF]->(a:Animal {name: 'Kira'})\n"
                + "SET rel.since = 2018" );
    }


    @Test
    @Ignore
    public void updateRelationshipNewPropertyTest() {
        execute( "MATCH (:Person {name:'Max Muster'})-[rel:OWNER_OF]->(a:Animal {name: 'Kira'})\n"
                + "SET rel.status = 'fresh'" );
    }

}
