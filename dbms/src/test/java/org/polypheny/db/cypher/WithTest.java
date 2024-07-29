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


public class WithTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void addVariableTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name, n WHERE n.name STARTS WITH 'H' RETURN n" );
        assertNode( res, 0 );

        assert containsRows( res, true, true,
                Row.of( HANS ) );

    }


    @Test
    public void renameVariableTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );

        GraphResult res = execute( "MATCH (n:Person) WITH n.name AS name, n WHERE name ENDS WITH 'x' RETURN name, n" );
        assertNode( res, 1 );

        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ), MAX ) );

    }


    @Test
    public void starTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_EDGE_1 );
        execute( SINGLE_EDGE_2 );

        GraphResult res = execute( "MATCH (n:Person)-[]->(p:Animal) WITH *, n.name AS username WHERE username CONTAINS 'a' RETURN username, p" );
        assertNode( res, 1 );

        assert containsRows( res, true, true, Row.of( TestLiteral.from( "Max" ), KIRA ) );

    }

    // aggregate

}
