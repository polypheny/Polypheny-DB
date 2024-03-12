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

package org.polypheny.db.cypher;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class OrderByTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void singleOrderByTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n) RETURN n.name ORDER BY n.name ASC" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Hans" ) ),
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Max" ) ) );

        res = execute( "MATCH (n) RETURN n.name ORDER BY n.name DESC" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Hans" ) ) );

    }


    @Test
    public void doubleOrderByTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age ORDER BY n.age ASC, n.name ASC" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Bob" ), TestLiteral.from( 31 ) ),
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );

        res = execute( "MATCH (n) RETURN n.depno, n.name ORDER BY n.depno ASC, n.name DESC" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( "Bob" ) ),
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( "Ann" ) ) );

    }


    @Test
    public void nullOrderByTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age ORDER BY n.age ASC" );

        assertTrue( containsRows( res, true, true,
                Row.of( TestLiteral.from( "Kira" ), TestLiteral.from( 3 ) ),
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( null ) ) ) );

        res = execute( "MATCH (n) RETURN n.name, n.age ORDER BY n.age DESC" );

        assertTrue( containsRows( res, true, true,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( null ) ),
                Row.of( TestLiteral.from( "Kira" ), TestLiteral.from( 3 ) ) ) );

    }


    @Test
    public void limitWithoutSortTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age LIMIT 3" );

        assert res.getData().length == 3;
    }


    @Test
    public void limitWithSortTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name ORDER BY n.name DESC LIMIT 3" );

        assert res.getData().length == 3;

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Kira" ) ) );

    }


    @Test
    public void skipWithoutSortTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age SKIP 3" );

        assert res.getData().length == 2;
    }


    @Test
    public void skipWithSortTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name ORDER BY n.name DESC SKIP 3" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Kira" ) ),
                Row.of( TestLiteral.from( "Kira" ) ) );
    }


    @Test
    public void skipAndLimitWithoutSortTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name SKIP 3 LIMIT 1" );

        assert res.getData().length == 1;
    }


    @Test
    public void skipAndLimitWithSortTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name ORDER BY n.name DESC SKIP 1 LIMIT 2" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Max" ) ),
                Row.of( TestLiteral.from( "Kira" ) ) );
    }


    @Test
    public void orderByNotReturnedPropertyTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n ORDER BY n.name DESC" );

        assert containsRows( res, true, true,
                Row.of( MAX ),
                Row.of( MAX ),
                Row.of( KIRA ),
                Row.of( KIRA ),
                Row.of( KIRA ) );
    }

}
