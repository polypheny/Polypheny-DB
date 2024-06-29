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

public class ListFunTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void simpleSizeFunTest() {
        GraphResult res = execute( "RETURN size([1, 2, 3])" );
        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 3 ) ) );
    }


    @Test
    public void nullSizeFunTest() {
        GraphResult res = execute( "RETURN size(null)" );
        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( null ) ) );
    }


    @Test
    public void patternExpressionSizeFunTest() {
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (a)\n"
                + "WHERE a.name = 'Max'\n"
                + "RETURN size((a)-[]->())) AS fof" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 2 ) ) );

    }


    @Test
    public void stringSizeFunTest() {
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (a)  RETURN size(a.name)" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 3 ) ) );
    }


    @Test
    public void simpleRangeFunTest() {

        GraphResult res = execute( "RETURN RANGE(1, 3)" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( 1 ), TestLiteral.from( 2 ), TestLiteral.from( 3 ) ) );
    }

}
