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
import org.polypheny.db.webui.models.results.GraphResult;

public class CypherGeoFunctionsTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void createPointTest() {
        execute( """
                CREATE
                (adam:User {name: 'Adam'}),
                (pernilla:User {name: 'Pernilla'}),
                (david:User {name: 'David'}),
                (adam)-[:FRIEND]->(pernilla),
                (pernilla)-[:FRIEND]->(david)""" );
        GraphResult res = execute( "MATCH (n) RETURN point({longitude: 56.7, latitude: 12.78}) AS point" );
        assertNode( res, 0 );
        assertEmpty( res );
    }

}
