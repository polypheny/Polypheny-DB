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
import org.polypheny.db.adapter.java.Array;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;
import java.util.Arrays;

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
    @Test
    public void renameWithClauseSortTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        GraphResult res =  execute( "MATCH (p:Person) WITH  p.age AS personAge RETURN  personAge ORDER BY personAge" );
        assert  containsRows( res , true ,true ,
                Row.of( TestLiteral.from( 31 ) ),
                Row.of(TestLiteral.from( 45 )) );

    }
    @Test
    public void renameWithClauseOrderByWithLimitTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult res =  execute( "MATCH (p:Person) WITH  p.age AS age ORDER BY age DESC LIMIT 2 RETURN age" );

        assert  containsRows( res , true ,true ,
                Row.of( TestLiteral.from( 45  ) ),
                Row.of(TestLiteral.from( 32 )) );



    }

    @Test
    public void renameWithClauseDoubleOrderByWithLimitTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res =  execute( "MATCH (p:Person) WITH p.depno AS department , p.name  AS name  ORDER BY department ASC,  name ASC LIMIT 3 RETURN department , name " );


        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( "Ann" ) ),
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( "Bob" ) ) ,
                Row.of( TestLiteral.from( 14 ),TestLiteral.from( "Alex" )));




    }

    @Test
    public void renameWithClauseDoubleOrderByTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res =  execute( "MATCH (p:Person) WITH p.depno AS department , p.name  AS name  ORDER BY department ASC,  name ASC  RETURN department , name" );


        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( "Ann" ) ),
                Row.of( TestLiteral.from( 13 ), TestLiteral.from( "Bob" ) ) ,
                Row.of( TestLiteral.from( 14 ),TestLiteral.from( "Alex" )));




    }
    @Test
    public void renameUnwindSortTest() {
        GraphResult res = execute( "UNWIND [1, true,3.14] AS i RETURN i ORDER BY i" );
        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( true ) ),
                Row.of( TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( 3.14 ) ) );


    }
    @Test
    public void renameWithClauseWithRenameUnwindSortTest ()
    {

     GraphResult res = execute( "WITH  [1 ,2 ,3] AS number UNWIND  number AS n RETURN  n ORDER BY n" );

     assert containsRows( res , true , true ,
             Row.of( TestLiteral.from( 1 ) ),
             Row.of( TestLiteral.from( 2 ) ),
             Row.of( TestLiteral.from( 3 ) ) );
    }

    @Test
    public void renameWithMixedTypesWithRenameUnwindSortTest ()
    {


        GraphResult res = execute( "WITH  [1 ,2 ,'4'] AS number UNWIND  number AS n RETURN n ORDER BY n" );

        assert containsRows( res , true , true ,
                Row.of( TestLiteral.from( '4') ),
                Row.of( TestLiteral.from( 1) ),
                Row.of( TestLiteral.from( 2 ) ) );
    }

    @Test
    public void renameAvgAggregateFieldSortTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult res = execute( "MATCH (p:Person)RETURN p.depno As Department ,avg(p.age) AS averageAge ORDER BY averageAge" );
        assert containsRows( res , true , true ,
                Row.of( TestLiteral.from( 14)  , TestLiteral.from( 32 )),
                Row.of( TestLiteral.from( 13 ) ,TestLiteral.from( 38 )));

    }

    @Test
    public void renameWithClauseOrderByWithSkipTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult res =  execute( "MATCH (p:Person) WITH  p.age AS age ORDER BY age DESC SKIP 1 Limit 1 RETURN age" );

        assert  containsRows( res , true ,true ,
                Row.of( TestLiteral.from( 32  ) ),
                Row.of(TestLiteral.from( 31)) );

    }

    @Test
    public void renameWithClauseOrderByWithSkipLimitTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult res =  execute( "MATCH (p:Person) WITH  p.age AS age ORDER BY age DESC SKIP 1 Limit 1 RETURN age" );

        assert  containsRows( res , true ,true ,
                Row.of( TestLiteral.from( 32  ) ));


    }




}
