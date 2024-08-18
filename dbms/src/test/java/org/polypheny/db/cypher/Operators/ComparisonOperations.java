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

public class ComparisonOperations extends CypherTestTemplate {

    @BeforeEach
    public void setUp() {
        tearDown();
        createGraph();
    }


    @Test
    public void IsNullOperatorTest() {
        GraphResult res = execute( "Return null is not null as  Result" );
          containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );
    }


    @Test
    public void IsNotNullFunction() {
        GraphResult res = execute( "Return null is null as  Result" );
          containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );

    }


    @Test
    public void greaterThanOperatorTest() {
        GraphResult res = execute( "Return 1 > 2 as result " );
          containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );
    }


    @Test
    public void smallerThanOperatorTest() {
        GraphResult res = execute( "Return 1 < 2 as result " );
          containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );
    }


    @Test
    public void greaterThanOrEqualOperatorTest() {
        GraphResult res = execute( "Return 1 >= 2 as result " );
          containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );
    }


    @Test
    public void smallerThanOrEqualOperatorTest() {
        GraphResult res = execute( "Return 1 <= 2 as result " );
          containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );
    }


    @Test
    public void equalityOperatorTest() {
        GraphResult res = execute( "Return 2 = 2 as result " );
          containsRows( res, true, false, Row.of( TestLiteral.from( true ) ) );
    }


    @Test
    public void inequalityOperatorTest() {
        GraphResult res = execute( "Return 1 <> 2 as result " );
          containsRows( res, true, false, Row.of( TestLiteral.from( false ) ) );
    }

}
