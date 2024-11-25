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

package org.polypheny.db.cypher.subqueries;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;


public class CallSubqueriesTest extends CypherTestTemplate {


    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleCallTest() {
        GraphResult res = execute( " CALL { RETURN 'hello' AS innerReturn} RETURN innerReturn" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "hello" ) ) );
    }


    @Test
    public void repeatCallTest() {
        GraphResult res = execute( """
                UNWIND [0, 1, 2] AS x
                CALL { RETURN 'hello' AS innerReturn }
                RETURN innerReturn""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "hello" ) ),
                Row.of( TestLiteral.from( "hello" ) ),
                Row.of( TestLiteral.from( "hello" ) ) );
    }


    @Test
    public void unwindVariablesAsInputsIntoCallTest() {
        GraphResult res = execute( """
                UNWIND [0, 1, 2] AS x
                CALL { WITH x RETURN x * 10 AS y }
                RETURN x, y""" );

        containsRows( res, true, true,
                Row.of( TestLiteral.from( 0 ), TestLiteral.from( 0 ) ),
                Row.of( TestLiteral.from( 1 ), TestLiteral.from( 10 ) ),
                Row.of( TestLiteral.from( 2 ), TestLiteral.from( 20 ) ) );
    }


    @Test
    public void returnMatchNodesCallTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "CALL { MATCH (p:Person) RETURN p} RETURN p " );
        containsRows( res, true, true, Row.of( MAX ) );
    }


    @Test
    public void countNodesCallTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                CALL {
                    MATCH (p)
                    RETURN count(p) AS totalPeople}
                RETURN totalPeople
                """ );

        containsRows( res, true, false, Row.of( TestLiteral.from( 2 ) ) );
    }


    @Test
    public void countRelationshipsCallTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( """
                CALL {
                    MATCH ()-[r]->()
                    RETURN count(r) AS totalRelationships }
                RETURN totalRelationships
                """ );

        containsRows( res, true, false, Row.of( TestLiteral.from( 2 ) ) );
    }


    @Test
    public void useMatchedNodesAsInputsIntoCallTest() {
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( """
                MATCH (p:Person)
                CALL {
                  WITH p
                  MATCH (p)-[:KNOWS]-(c:Person)
                  RETURN c.name AS friend
                }
                RETURN p.name, friend""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "Hans" ) ) );
    }


    @Test
    public void filterMatchedNodesByOutputOfCallTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( """
                CALL {
                  MATCH (p:Person { name: 'Bob' })
                  RETURN p.age AS age}
                MATCH (p:Person)
                WHERE p.age > age
                RETURN p
                """ );

        assertEquals( 2, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Ann" ) ),
                TestNode.from( List.of( "Person" ), Pair.of( "name", "Alex" ) ) );
    }


    @Test
    public void unionCallTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( """
                CALL { MATCH (p:Person)
                  RETURN p
                UNION
                  MATCH (p:Person)
                  RETURN p
                }
                RETURN p.name, p.age""" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ),
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 31 ) ) );
    }


    @Test
    public void unitSubQueryCallTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                MATCH (p:Person) CALL { 
                  WITH p
                  CREATE (:Person {name: p.name})
                } RETURN count(*)""" );

        // The number of rows present after the subquery is the same as was going into the subquery
        containsRows( res, true, false, Row.of( TestLiteral.from( 2 ) ) );
    }

}
