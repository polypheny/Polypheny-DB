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

package org.polypheny.db.cypher.operators;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;


public class MathematicalOperatorsTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void additionOperator() {
        GraphResult res = execute( "RETURN 2 + 3" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 5 ) ) );
    }


    @Test
    public void minisOperatorTest() {
        GraphResult res = execute( "RETURN 3 - 2" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 1 ) ) );
    }


    @Test
    public void multiplicationOperatorTest() {
        GraphResult res = execute( "RETURN  2 * 3" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 6 ) ) );
    }


    @Test
    public void divisionOperatorTest() {
        GraphResult res = execute( "RETURN 6 / 3 " );
        containsRows( res, true, false, Row.of( TestLiteral.from( 2 ) ) );
    }


    @Test
    public void moduleOperatorTest() {
        GraphResult res = execute( "RETURN 3 % 2 " );
        containsRows( res, true, false, Row.of( TestLiteral.from( 1 ) ) );
    }


    @Test
    public void exponentiationOperator() {
        GraphResult res = execute( "RETURN 2 ^ 3" );
        containsRows( res, true, false, Row.of( TestLiteral.from( 8.0 ) ) );
    }

}
