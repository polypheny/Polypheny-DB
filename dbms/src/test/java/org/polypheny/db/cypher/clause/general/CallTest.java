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

package org.polypheny.db.cypher.clause.general;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class CallTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleCallProcedureTest() {
        GraphResult res = execute( "CALL db.labels()" );
        assertEmpty (res);

        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
         res = execute( "CALL db.labels()" );

         containsRows( res, true, false,
                Row.of( TestLiteral.from( "Person" ) ),
                Row.of( TestLiteral.from( "Animal" ) ) );


        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );

        res = execute( "CALL db.labels()" );
         containsRows( res, true, false,
                Row.of( TestLiteral.from( "Person" ) ),
                Row.of( TestLiteral.from( "Person" ) ),
                Row.of( TestLiteral.from( "Animal" ) ),
                Row.of( TestLiteral.from( "Animal" ) ));

    }



    @Test
    public void callLabelsYieldTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res =  execute( "CALL db.labels() YIELD label" );
         containsRows( res, true, false,
                Row.of( TestLiteral.from( "Person" ) ),
                Row.of( TestLiteral.from( "Animal" ) ) );
    }

    @Test
    public void renameCallLabelsYieldTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res =  execute( "CALL db.labels() YIELD label As Label" );
         containsRows( res, true, false,
                Row.of( TestLiteral.from( "Person" ) ),
                Row.of( TestLiteral.from( "Animal" ) ) );
    }


    @Test
    public void callLabelsYieldCountTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res =  execute( "CALL db.labels() YIELD *" );
         containsRows( res, true, false,
                Row.of( TestLiteral.from( 2 ) ));
    }

    @Test
    public void returnCallLabelsYieldTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res =  execute( "CALL db.labels() YIELD label\n"
                + "RETURN label" );
         containsRows( res, true, false,
                Row.of( TestLiteral.from( 2 ) ) );
    }

    @Test
    public void returnFilterCallLabelsYieldTest() {
        execute(SINGLE_NODE_PERSON_1);
        execute(SINGLE_NODE_ANIMAL);
        GraphResult res = execute("CALL db.labels() YIELD label WHERE label = 'Person' RETURN count(label) AS numLabels");

         containsRows(res, true, false,
                Row.of(TestLiteral.from(1)));
    }

    @Test
    public void callLabelsYieldWithOrderingTest() {
        execute(SINGLE_NODE_PERSON_1);
        execute(SINGLE_NODE_PERSON_2);
        execute(SINGLE_NODE_ANIMAL);
        GraphResult res = execute("CALL db.labels() YIELD label RETURN label ORDER BY label");

          containsRows(res, true, false,
                Row.of(TestLiteral.from("Animal")),
                Row.of(TestLiteral.from("Person")),
                Row.of(TestLiteral.from("Person")));
    }

    @Test
    public void callLabelsYieldWithAggregationTest() {
        execute(SINGLE_NODE_PERSON_1);
        execute(SINGLE_NODE_ANIMAL);
        execute(SINGLE_NODE_PERSON_2);
        GraphResult res = execute("CALL db.labels() YIELD label RETURN label, count(*) AS labelCount");

          containsRows(res, true, false,
                Row.of(TestLiteral.from("Person"), TestLiteral.from(2)),
                Row.of(TestLiteral.from("Animal"), TestLiteral.from(1)));
    }

    @Test
    public void simpleCallPropertyKeysYieldTest() {
        execute(SINGLE_NODE_PERSON_1);
        execute(SINGLE_NODE_ANIMAL);
        execute(SINGLE_NODE_PERSON_2);
        GraphResult res = execute("CALL db.propertyKeys() YIELD propertyKey ");
          containsRows(res, true, false,
                Row.of(TestLiteral.from("name")),
                Row.of(TestLiteral.from("age")),
                Row.of(TestLiteral.from("type")));

    }

    @Test
    public void renameCallPropertyKeysYieldTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res =  execute( "CALL db.propertyKeys() YIELD propertyKey AS prop" );
          containsRows( res, true, false,
                Row.of( TestLiteral.from( "Person" ) ),
                Row.of( TestLiteral.from( "Animal" ) ) );
    }
    @Test
    public void callPropertyKeysYieldWithMatchTest()
    {

        execute(SINGLE_NODE_PERSON_1);
        execute(SINGLE_NODE_ANIMAL);
        execute(SINGLE_NODE_PERSON_2);
        GraphResult res =  execute( "CALL db.propertyKeys() YIELD propertyKey AS prop\n"
                 + "MATCH (n)\n"
                 + "WHERE n[prop] IS NOT NULL\n"
                 + "RETURN prop, count(n) AS numNodes" );


          containsRows(res, true, false,
                Row.of(TestLiteral.from("name") , TestLiteral.from( 3 )),
                Row.of(TestLiteral.from("age") ,TestLiteral.from( 1)),
                Row.of(TestLiteral.from("type") ,TestLiteral.from( 1 )));
    }



}
