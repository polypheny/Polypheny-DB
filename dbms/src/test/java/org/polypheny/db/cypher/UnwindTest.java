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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

}
