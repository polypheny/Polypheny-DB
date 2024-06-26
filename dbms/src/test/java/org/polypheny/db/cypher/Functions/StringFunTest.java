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

public class StringFunTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void upperFunTest() {
        GraphResult res = execute( "RETURN UPPER('hello')" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "HELLO" ) ) );
         res = execute( "RETURN UPPER('hElLo')" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "HELLO" ) ) );



    }


    @Test
    public void LowerFunTest() {
        GraphResult res = execute( "RETURN LOWER('WORLD')" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "world" ) ) );
        res = execute( "RETURN LOWER('WOrLd')" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "world" ) ) );


    }

    @Test
    public void substringFunTest()
    {
        GraphResult res = execute( "RETURN SUBSTRING('Hello, world!', 0, 5)" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "Hello" ) ) );

    }

    @Test
    public void trimFunTest()
    {
        GraphResult res = execute( "RETURN TRIM('  hello  ')" );
        containsRows( res, true, true, Row.of( TestLiteral.from( "hello" ) ) );

    }

    @Test
    public void replaceFunTest()
    {
        GraphResult res = execute( "RETURN REPLACE('Hello, world!', 'world', 'Cypher') " );
        containsRows( res, true, true, Row.of( TestLiteral.from( "Hello, Cypher!" ) ) );

    }

}

