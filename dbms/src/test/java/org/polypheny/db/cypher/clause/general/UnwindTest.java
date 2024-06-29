package org.polypheny.db.cypher.clause.general;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class UnwindTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleUnwindTest() {
        GraphResult res = execute( "UNWIND [1, 3, null] AS x RETURN x, 'val' AS y" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 1 ), TestLiteral.from( "val" ) ),
                Row.of( TestLiteral.from( 3 ), TestLiteral.from( "val" ) ),
                Row.of( TestLiteral.from( null ), TestLiteral.from( "val" ) ) );

    }


    @Test
    public void emptyUnwind() {
        GraphResult res = execute( "UNWIND [] AS x RETURN x, 'val' AS y" );

        assertEmpty( res );
    }


    @Test
    public void nullUnwind() {
        GraphResult res = execute( "UNWIND null AS x RETURN x, 'val' AS y" );

        assertEmpty( res );
    }


    @Test
    public void listOfListUnwind() {
        GraphResult res = execute( "WITH [[1], [2, 4], 3] AS nested UNWIND nested AS x UNWIND x AS y RETURN y" );

        containsRows( res, true, true,
                Row.of( TestLiteral.from( 1 ) ),
                Row.of( TestLiteral.from( 2 ) ),
                Row.of( TestLiteral.from( 4 ) ),
                Row.of( TestLiteral.from( 3 ) ) );
    }


    @Test
    public void nodePropertyUnwind() {
        execute( "CREATE (n {key: [3,1]})" );
        GraphResult res = execute( "MATCH (n) UNWIND n.key AS x RETURN x" );

        containsRows( res, true, false,
                Row.of( TestLiteral.from( 3 ) ),
                Row.of( TestLiteral.from( 1 ) ) );
    }


    @Test
    public void minMaxAggregateSimpleUnWind() {
        GraphResult res = execute( "UNWIND [1, 'a', NULL, 0.2, 'b', '1', '99'] AS val RETURN min(val)" );
        containsRows( res, true, false, Row.of( TestLiteral.from( '1' ) ) );

        res = execute( "UNWIND [1, 'a', NULL, 0.2, 'b', '1', '99'] As val RETURN max(val)" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 1 ) ) );

    }



    @Test
    public void minMaxAggregateListOfListUnwind() {
        GraphResult res = execute( "UNWIND ['d', [1, 2], ['a', 'c', 23]] AS val RETURN  min(val)" );

        res = execute( "UNWIND ['d', [1, 2], ['a', 'c', 23]] AS val RETURN max(val)" );


    }


    @Test
    public void maxMinAggregateNodePropertyUnWind() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (n) UNWIND n.age  AS age  RETURN max(age)" );
        assert containsRows( res, true, false, Row.of( TestLiteral.from( 45 ) ) );
        res = execute( "MATCH (n) UNWIND n.age  AS age  RETURN min(age)" );

        assert containsRows( res, true, false, Row.of( TestLiteral.from( 31 ) ) );

    }


    @Test
    public void sumAggregateNodePropertyUnWind() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (n) UNWIND n.age  AS age  RETURN sum(age)" );
        assert containsRows( res, true, false, Row.of( TestLiteral.from( 76 ) ) );

    }


    @Test
    public void AvgAggregateNodePropertyUnWind() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );

        GraphResult res = execute( "MATCH (n) UNWIND n.age  AS age  RETURN avg(age)" );
        assert containsRows( res, true, false, Row.of( TestLiteral.from( 38 ) ) );

    }


    @Test
    public void CollectAggregateUnWind ()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute(SINGLE_NODE_PERSON_COMPLEX_2);
        GraphResult  res =  execute( "MATCH (n) UNWIND n.age AS age RETURN Collect(age)" );

    }





    @Test
    public void countUnWind() {
        GraphResult res = execute( "UNWIND [2, 1 , 1] AS i  RETURN count( i)" );


        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( 3)));

    }

    @Test
    public void distinctUnWind() {
        GraphResult res = execute( "UNWIND [3, 3 ,2 ,1 ] AS i  RETURN DISTINCT i" );
        assert res.getData().length == 3;
        assert containsRows( res , true , false  ,
                Row.of(TestLiteral.from(3)) ,
                Row.of( TestLiteral.from( 2 )) ,Row.of( TestLiteral.from( 1) ));


    }




    @Test
    public void ConditionalLogicUnWind()
    {
        GraphResult res =  execute( "UNWIND [1, 2, 3] AS number RETURN number, CASE WHEN number % 2 = 0 THEN 'even' ELSE 'odd' END AS type" );
        assert containsRows( res , true , true ,
                Row.of( TestLiteral.from( 1 ) ,TestLiteral.from("odd"  ) ),
                Row.of( TestLiteral.from( 2 ) ,TestLiteral.from( "even" )),
                Row.of( TestLiteral.from( 3 ) ,TestLiteral.from(  "odd")));

    }




    @Test
    public void mapStructureUnWind()
    {
        GraphResult res =  execute( "UNWIND [{name: 'Alice', age: 30}] AS person  RETURN person.name  , person.age" );
        assert  containsRows( res , true , false , Row.of( TestLiteral.from( "Alice" ) ),Row.of( TestLiteral.from( 30 ) ) );

    }




}
