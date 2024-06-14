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

public class SkipTest  extends  CypherTestTemplate{

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }

    @Test
    public void simpleSkipTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name, n.age SKIP 3" );

        assert res.getData().length == 2;
    }

    @Test
    public void orderBySkipTest() {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name ORDER BY n.name DESC SKIP 3" );

        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "Kira" ) ),
                Row.of( TestLiteral.from( "Kira" ) ) );
    }

    @Test
    public void withAndSkipTest()
    {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 ) ;
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );

        GraphResult res = execute( "MATCH (n) WITH n SKIP 2 RETURN n.name, n.age;" );
    }

    @Test
    public void returnSubsetSkipTest()
    {
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_ANIMAL );
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_1 );

        GraphResult res = execute( "MATCH (n) RETURN n.name SKIP 3 LIMIT 1" );

        assert res.getData().length == 1;
    }

    @Test
    public void expressionAndSkipTest()
    {
        execute( "MATCH (n)\n"
                + "RETURN n.name\n"
                + "ORDER BY n.name\n"
                + "SKIP 1 + toInteger(3*rand())" );
    }


}
