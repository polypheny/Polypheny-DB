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

package org.polypheny.db.cypher.clause.general;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class FilterTest extends CypherTestTemplate {

    @BeforeEach
    public void setUp() {
        tearDown();
        createGraph();
    }

    ///////////////////////////////////////////////
    /////////// FILTER
    ///////////////////////////////////////////////


    @Test
    public void simpleConditionFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.age > 3 RETURN p" );

        assertNode( result, 0 );

        assert containsRows( result, true, false );

        result = execute( "MATCH (p) WHERE p.age >= 3 RETURN p" );
        assertNode( result, 0 );

        assert containsRows( result, true, false, Row.of( KIRA ) );
    }


    @Test
    public void multipleConditionsFilterTest() {
        execute( SINGLE_NODE_PERSON_COMPLEX_1 );
        execute( SINGLE_NODE_PERSON_COMPLEX_2 );
        execute( SINGLE_NODE_PERSON_COMPLEX_3 );
        GraphResult result = execute( "MATCH (p) WHERE p.age >= 45 AND p.depno = 13 RETURN p.name" );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Ann" ) ) );

        result = execute( "MATCH (p) WHERE p.age <= 32 OR p.depno = 13 RETURN p.name " );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Ann" ) ),
                Row.of( TestLiteral.from( "Bob" ) ),
                Row.of( TestLiteral.from( "Alex" ) ) );
    }


    @Test
    public void existFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        GraphResult result = execute( "MATCH (p) WHERE exists(p.age) RETURN p" );

        assertEmpty( result );

        result = execute( "MATCH (p) WHERE exists(p.name) RETURN p" );
        assertNode( result, 0 );

        assert containsRows( result, true, false, Row.of( MAX ) );
    }


    @Test
    public void startWithFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.name STARTS WITH 'M' RETURN p.name" );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Max" ) ) );


    }


    @Test
    public void containsFilterTest() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_PERSON_2 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult result = execute( "MATCH (p) WHERE p.name CONTAINS 'H' RETURN p.name" );

        assert containsRows( result, true, false, Row.of( TestLiteral.from( "Hans" ) ) );
    }


}
