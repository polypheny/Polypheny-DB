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

package org.polypheny.db.cypher.Functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class NumericFunTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void absFunTest() {
        GraphResult res = execute( "RETURN ABS(-5) " );

          containsRows( res, true, true, Row.of( TestLiteral.from( 5 ) ) );

        res = execute( "RETURN ABS(5)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 5 ) ) );

        res = execute( "RETURN ABS(0)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 0 ) ) );
    }


    @Test
    public void roundFunTest() {
        GraphResult res = execute( "RETURN ROUND(3)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "RETURN ROUND(-3)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( -3 ) ) );

        res = execute( "RETURN ROUND(3.4)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "RETURN ROUND(3.5)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 4 ) ) );

        res = execute( "RETURN ROUND(-3.5)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( -4 ) ) );


    }


    @Test
    public void floorFunTest() {

        GraphResult res = execute( "RETURN FLOOR(3)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "RETURN FLOOR(-3)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( -3 ) ) );

        res = execute( "RETURN FLOOR(3.16)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "RETURN FLOOR(3.9)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "RETURN FLOOR(-3.16)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( -4 ) ) );
    }


    @Test
    public void ceilFunTest() {
        GraphResult res = execute( "RETURN CEIL(3)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 4 ) ) );

        res = execute( "RETURN CEIL(-3)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 4 ) ) );

        res = execute( "RETURN CEIL(3.16)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 4 ) ) );

        res = execute( "RETURN CEIL(3.5)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 4 ) ) );

    }


    @Test
    public void sqrtFunTest() {
        GraphResult res = execute( "RETURN SQRT(9)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 3 ) ) );

        res = execute( "RETURN SQRT(0)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( 0 ) ) );
    }


    @Test
    public void nonPerfectSquareSqrtFunTest() {
        GraphResult res = execute( "RETURN SQRT(8)" );
          containsRows( res, true, true, Row.of( TestLiteral.from( Math.sqrt( 8 ) ) ) );
    }


    @Test
    public void sqrtFunTestNegative() {
        GraphResult res = execute( "RETURN SQRT(-9)" );
        //   containsRows(res, true, true, Row.of(TestLiteral.from(null)));
    }

}


