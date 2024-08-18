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

package org.polypheny.db.cypher.Operators;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class BooleanOperatorsTest extends CypherTestTemplate {

    @BeforeEach
    public void setUp() {
        tearDown();
        createGraph();
    }


    @Test
    public void conjunctionOperatorTest() {
        GraphResult res = execute( "WITH true as a , false as b RETURN a AND  b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );
        res = execute( "WITH true as a , true  as b RETURN a AND  b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );

    }


    @Test
    public void disjunctionOperatorTest() {
        GraphResult res = execute( "WITH true as a , false as b RETURN a OR  b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );
        res = execute( "WITH false as a , false  as b RETURN a OR b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );

    }


    @Test
    public void exclusiveDisjunctionOperatorTest() {
        GraphResult res = execute( "WITH true as a , false as b RETURN a XOR b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );
        res = execute( "WITH true as a , true  as b RETURN a XOR b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );
        res = execute( "WITH false as a , false  as b RETURN a XOR b " );
        containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );

    }


    @Test
    public void negationOperatorTest() {
        GraphResult res = execute( "WITH true as a RETURN  NOT a  " );
        containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );

        res = execute( "WITH false as a RETURN  NOT a  " );
        containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );

    }

}
