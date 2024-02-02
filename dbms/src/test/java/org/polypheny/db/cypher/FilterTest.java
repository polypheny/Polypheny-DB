/*
 * Copyright 2019-2022 The Polypheny Project
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

public class FilterTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }

    ///////////////////////////////////////////////
    ///////// FILTER
    ///////////////////////////////////////////////


    @Test
    public void simplePropertyFilter() {
        execute( SINGLE_NODE_PERSON_1 );
        execute( SINGLE_NODE_ANIMAL );
        GraphResult res = execute( "MATCH (p) WHERE p.age > 3 RETURN p" );
        assertNode( res, 0 );

        assert containsRows( res, true, false );

        res = execute( "MATCH (p) WHERE p.age >= 3 RETURN p" );
        assertNode( res, 0 );

        assert containsRows( res, true, false, Row.of( KIRA ) );
    }

    @Test
    public void functionFilterTest() {
        execute( SINGLE_NODE_GEOM );
        GraphResult res = execute( "MATCH (c:City) WHERE GEO_CONTAINS('POLYGON ((7.579962 47.551795, 7.579962 47.559905, 7.600045 47.559905, 7.600045 47.551795, 7.579962 47.551795))', c.location) RETURN c.name" );
        assert is( res, Type.ANY, 0 );
        assert containsIn( res, true, 0, "c.name", TestLiteral.from( "Basel" ) );
    }

}
