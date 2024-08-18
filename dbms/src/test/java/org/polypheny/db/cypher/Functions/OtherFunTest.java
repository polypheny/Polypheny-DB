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

import org.checkerframework.checker.units.qual.K;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;
import java.util.List;

public class OtherFunTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    //////////////////////////
    /// Scalar Functions
    ///////////////////////////
    @Test
    public void typeFunTest() {
        execute( SINGLE_EDGE_1 );
        GraphResult res = execute( "MATCH (a)-[r]->(b)\n"
                + "RETURN TYPE(r)\n" );

          containsRows( res, true, true, Row.of( TestLiteral.from( "OWNER_OF" ) ) );

    }


    @Test
    public void idFunTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (p:Person { name: 'Max' })\n"
                + "RETURN ID(p)\n" );

    }


    @Test
    public void testCoalesceFunTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        GraphResult result = execute( "MATCH (p) RETURN p.name, coalesce(p.age, 0) AS age" );

          containsRows( result, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( 0 ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( 0 ) ),
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( 45 ) ) );
    }


    @Test
    public void testDefaultValuesWithCoalesceFunTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        GraphResult result = execute( "MATCH (p) RETURN p.name, coalesce(p.age, 'unknown') AS age" );

        assertNode( result, 0 );

          containsRows( result, true, false,
                Row.of( TestLiteral.from( "Max" ), TestLiteral.from( "unknown" ) ),
                Row.of( TestLiteral.from( "Hans" ), TestLiteral.from( "unknown" ) ),
                Row.of( TestLiteral.from( "Ann" ), TestLiteral.from( "unknown" ) ) );
    }


    ///////////////////////////////
    // Predicate Functions
    /////////////////////////////
    @Test
    public void ExistFunTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult res = execute( "MATCH (p:Person { name: 'Max' })\n"
                + "RETURN EXISTS(p.age)\n" );

          containsRows( res, true, true, Row.of( TestLiteral.from( false ) ) );

        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( "MATCH (p:Person { name: 'Ann' })\n"
                + "RETURN EXISTS(p.age)\n" );
          containsRows( res, true, true, Row.of( TestLiteral.from( true ) ) );
    }


}
