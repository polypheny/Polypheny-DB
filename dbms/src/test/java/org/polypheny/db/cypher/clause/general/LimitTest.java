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
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LimitTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleLimitTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age LIMIT 3" );

        assertEquals( 3, res.getData().length );
    }


    @Test
    public void InsertNodeLimitTest() {
        GraphResult res = execute( """
                CREATE (n:person)
                RETURN n
                LIMIT 0""" );

        assertEquals( 0, res.getData().length );
        res = matchAndReturnAllNodes();
        assertEquals( 1, res.getData().length );
    }


    @Test
    public void updateNodeLimitTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( """
                MATCH (n {name: 'Max'})
                SET n.age = 60
                RETURN n
                LIMIT 0""" );
        assertEquals( 0, res.getData().length );
        res = matchAndReturnAllNodes();
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ),
                        Pair.of( "name", "Max" ) ) );

        res = execute( """
                MATCH (n {name: 'Max'})
                SET n.age = 60
                RETURN n
                LIMIT 1""" );
        assertEquals( 1, res.getData().length );
        containsNodes( res, true,
                TestNode.from( List.of( "Person" ),
                        Pair.of( "name", "Max" ),
                        Pair.of( "age", 60 ) ) );
    }


    @Test
    public void orderByLimitTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name ORDER BY n.name DESC LIMIT 3" );

        assertEquals( 3, res.getData().length );

        containsRows( res, true, true,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Kira" ) ) );

    }


    @Test
    public void withLimitTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (n) WITH n LIMIT 2 RETURN n.name, n.age;" );
        assertEquals( 2, res.getData().length );
    }


    @Test
    public void withAndOrderByLimitTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( """
                MATCH (n)
                WITH n ORDER BY n.name LIMIT 1
                RETURN n""" );

        assertEquals( 1, res.getData().length );

        containsNodes( res, true, TestNode.from( Pair.of( "name", "Ann" ),
                Pair.of( "age", 45 ),
                Pair.of( "depno", 13 ) ) );


    }


    @Test
    public void numberOfUpdatesLimitTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( """
                MATCH (n)
                WITH n ORDER BY n.name LIMIT 1
                SET n.locked = true
                RETURN n.name""" );

        containsRows( res, true, false, Row.of( TestLiteral.from( "Max" ) ) );
    }


}
