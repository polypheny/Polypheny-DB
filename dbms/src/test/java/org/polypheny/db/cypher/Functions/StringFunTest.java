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
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "HELLO" ) ) );
        res = execute( "RETURN UPPER('hElLo')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "HELLO" ) ) );


    }


    @Test
    public void EmptyUpperFunTest() {
        GraphResult res = execute( "RETURN UPPER('')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "" ) ) );
    }


    @Test
    public void nullUpperFunTest() {
        GraphResult res = execute( "RETURN UPPER(null)" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void LowerFunTest() {
        GraphResult res = execute( "RETURN LOWER('WORLD')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "world" ) ) );

        res = execute( "RETURN LOWER('WOrLd')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "world" ) ) );
    }


    @Test
    public void emptyLowerFunTest() {
        GraphResult res = execute( "RETURN LOWER('')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "" ) ) );
    }


    @Test
    public void NullLowerFunTest() {
        GraphResult res = execute( "RETURN LOWER(null)" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void normalSubstringFunTest() {
        GraphResult res = execute( "RETURN SUBSTRING('Hello, world!', 0, 5)" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Hello" ) ) );

        res = execute( "RETURN SUBSTRING('Hello, world!', 7, 5)" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "world" ) ) );

        res = execute( "RETURN SUBSTRING('Hello, world!', 7, 0)" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "" ) ) );
    }


    @Test
    public void exceedLengthSubstringFunTest() {
        GraphResult res = execute( "RETURN SUBSTRING('Hello', 0, 10)" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Hello" ) ) );
    }


    @Test
    public void trimFunTest() {
        GraphResult res = execute( "RETURN TRIM('  hello  ')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "hello" ) ) );

        res = execute( "RETURN TRIM('hello')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "hello" ) ) );

        res = execute( "RETURN TRIM('  ')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "" ) ) );
    }


    @Test
    public void EmptyTrimFunTest() {
        GraphResult res = execute( "RETURN TRIM('')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "" ) ) );
    }


    @Test
    public void NormalReplaceFunTest() {
        GraphResult res = execute( "RETURN REPLACE('Hello, world!', 'world', 'Cypher') " );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Hello, Cypher!" ) ) );

    }


    @Test
    public void caseSensitiveReplaceFunTest() {
        GraphResult res = execute( "RETURN REPLACE('Hello, world!', 'WORLD', 'Cypher')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Hello, world!" ) ) );
    }


    @Test
    public void removeSpacesReplaceFunTest() {
        GraphResult res = execute( "RETURN REPLACE('Hello, world!', ' ', '')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Hello,world!" ) ) );
    }


    @Test
    public void removeSubstringReplaceFunTest() {
        GraphResult res = execute( "RETURN REPLACE('Hello, world!', 'world', '')" );
        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Hello, !" ) ) );
    }
//    @Test
//    public void concatenateFunTest() {
//        GraphResult res = execute("RETURN 'Hello' + ' ' + 'world!'");
//        assert containsRows(res, true, true, Row.of(TestLiteral.from("Hello world!")));
//    }
//    @Test
//    public void LengthFunTest() {
//        GraphResult res = execute("RETURN LENGTH('Hello, world!')");
//        assert containsRows(res, true, true, Row.of(TestLiteral.from(13)));
//    }


}

