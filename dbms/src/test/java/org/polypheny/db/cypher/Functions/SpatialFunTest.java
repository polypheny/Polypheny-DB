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

import javassist.bytecode.CodeIterator.Gap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;

public class SpatialFunTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void cartesian2DPointFunTest() {

        GraphResult res = execute( "WITH point({x: 3, y: 4}) AS p\n"
                + "RETURN\n"
                + "  p.x AS x,\n"
                + "  p.y AS y,\n"
                + "  p.crs AS crs,\n"
                + "  p.srid AS srid" );

          containsRows( res, true, false,
                Row.of( TestLiteral.from( 3.0 ), TestLiteral.from( 4.0 ),
                        TestLiteral.from( "cartesian" ), TestLiteral.from( 7203 ) ) );
    }


    @Test
    public void WGS_843DPointFunTest() {
        GraphResult res = execute( "WITH point({latitude: 3, longitude: 4, height: 4321}) AS p\n"
                + "RETURN\n"
                + "  p.latitude AS latitude,\n"
                + "  p.longitude AS longitude,\n"
                + "  p.height AS height,\n"
                + "  p.x AS x,\n"
                + "  p.y AS y,\n"
                + "  p.z AS z,\n"
                + "  p.crs AS crs,\n"
                + "  p.srid AS srid" );

          containsRows( res, true, false,
                Row.of( TestLiteral.from( 3.0 ),
                        TestLiteral.from( 4.0 ),
                        TestLiteral.from( 4321.0 ),
                        TestLiteral.from( 4.0 ),
                        TestLiteral.from( 3.0 ),
                        TestLiteral.from( 4321.0 ),
                        TestLiteral.from( "wgs-84-3d" ),
                        TestLiteral.from( 4979 ) ) );
    }

}
