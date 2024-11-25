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


public class CaseTest extends CypherTestTemplate {

    protected static final String PERSON_NODE_ALICE = "CREATE (:Person {name:'Alice', age: 38, eyes: 'brown'})";
    protected static final String PERSON_NODE_BOB = "CREATE (:Person {name: 'Bob', age: 25, eyes: 'blue'})";
    protected static final String PERSON_NODE_CHARLIE = "CREATE (:Person {name: 'Charlie', age: 53, eyes: 'green'})";


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleCaseTest() {
        execute( PERSON_NODE_ALICE );
        execute( PERSON_NODE_BOB );
        execute( PERSON_NODE_CHARLIE );
        GraphResult res = execute( """
                MATCH (n:Person)
                RETURN
                CASE n.eyes
                  WHEN 'blue'  THEN 1
                  WHEN 'brown' THEN 2
                  ELSE 3
                END AS result, n.eyes""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Alice" ), TestLiteral.from( 2 ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( "Charlie" ), TestLiteral.from( 3 ) ) );
    }


    @Test
    public void genericCaseTest() {
        execute( PERSON_NODE_ALICE );
        execute( PERSON_NODE_BOB );
        execute( PERSON_NODE_CHARLIE );

        GraphResult res = execute( """
                MATCH (n:Person)
                RETURN
                CASE
                  WHEN n.eyes = 'blue' THEN 1
                  WHEN n.age < 40      THEN 2
                  ELSE 3
                END AS result, n.eyes, n.age""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Alice" ), TestLiteral.from( 2 ) ), Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 1 ) ), Row.of( TestLiteral.from( "Charlie" ), TestLiteral.from( 3 ) ) );
    }


    @Test

    public void nullWithCaseTest() {
        execute( PERSON_NODE_ALICE );
        execute( PERSON_NODE_BOB );
        execute( PERSON_NODE_CHARLIE );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( """
                MATCH (n:Person)
                RETURN n.name,
                CASE n.age
                  WHEN null THEN -1
                  ELSE n.age - 10
                END AS age_10_years_ago""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Alice" ), TestLiteral.from( 28 ) ), Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 15 ) ), Row.of( TestLiteral.from( "Charlie" ), TestLiteral.from( 43 ) ), Row.of( TestLiteral.from( "MAX" ), TestLiteral.from( null ) ) );
    }


    @Test
    public void expressionsAndSucceedingClauses() {
        execute( PERSON_NODE_ALICE );
        execute( PERSON_NODE_BOB );
        execute( PERSON_NODE_CHARLIE );

        GraphResult res = execute( """
                MATCH (n:Person)
                WITH n,
                CASE n.eyes
                  WHEN 'blue'  THEN 1
                  WHEN 'brown' THEN 2
                  ELSE 3
                END AS colorCode
                SET n.colorCode = colorCode
                RETURN n.name, n.colorCode""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Alice" ), TestLiteral.from( 2 ) ), Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 1 ) ), Row.of( TestLiteral.from( "Charlie" ), TestLiteral.from( 3 ) ) );
    }

}
