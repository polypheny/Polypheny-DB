/*
 * Copyright 2019-2026 The Polypheny Project
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
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.cypher.helper.TestLiteral;
import org.polypheny.db.webui.models.results.GraphResult;


public class CypherVectorDistanceTest extends CypherTestTemplate {

    private static final String NODE_A = "CREATE (:Item {name: 'a', embedding: [1.0,1.0]})";
    private static final String NODE_B = "CREATE (:Item {name: 'b', embedding: [2.0,2.0]})";
    private static final String NODE_C = "CREATE (:Item {name: 'c', embedding: [0.0,3.0]})";

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void l2DistanceReturnsDouble() {
        execute( NODE_A );
        GraphResult res = execute(
                "MATCH (n:Item) " +
                        "RETURN vector_distance(n.embedding, [1.0, 1.0], 'L2') AS dist " +
                        "LIMIT 1" );
        assert res.getData().length == 1;
    }


    @Test
    public void l2DistanceCorrectValues() {
        execute( NODE_A );
        execute( NODE_B );
        execute( NODE_C );
        GraphResult res = execute( "MATCH (n:Item) RETURN n.name, vector_distance(n.embedding, [1.0, 1.0], 'L2') AS dist LIMIT 3" );
        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "a" ), TestLiteral.from( "0.0" ) ),
                Row.of( TestLiteral.from( "b" ), TestLiteral.from( "1.4142135623730951" ) ),
                Row.of( TestLiteral.from( "c" ), TestLiteral.from( "2.23606797749979" ) )
        );
    }


    @Test
    public void l1DistanceCorrectValues() {
        execute( NODE_A );
        execute( NODE_B );
        execute( NODE_C );
        GraphResult res = execute( "MATCH (n:Item) RETURN n.name, vector_distance(n.embedding, [1.0, 1.0], 'L1') AS dist LIMIT 3" );
        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "a" ), TestLiteral.from( "0.0" ) ),
                Row.of( TestLiteral.from( "b" ), TestLiteral.from( "2.0" ) ),
                Row.of( TestLiteral.from( "c" ), TestLiteral.from( "3.0" ) )
        );
    }


    @Test
    public void cosineDistanceCorrectValues() {
        execute( NODE_A );
        execute( NODE_B );
        execute( NODE_C );
        GraphResult res = execute( "MATCH (n:Item) WHERE vector_distance(n.embedding, [1.0, 1.0], 'COSINE') < 1e-10 RETURN n.name LIMIT 3" );
        assert res.getData().length == 2;
    }


    @Test
    public void l2DistanceAsFilter() {
        execute( NODE_A );
        execute( NODE_B );
        execute( NODE_C );
        GraphResult res = execute( "MATCH (n:Item) WHERE vector_distance(n.embedding, [1.0, 1.0], 'L2') < 2.0 RETURN n.name LIMIT 3" );
        assert res.getData().length == 2;
    }


    @Test
    public void l2DistanceOrderByLimit() {
        execute( NODE_A );
        execute( NODE_B );
        execute( NODE_C );
        GraphResult res = execute(
                "MATCH (n:Item) " +
                        "RETURN n.name, vector_distance(n.embedding, [1.0, 1.0], 'L2') AS dist " +
                        "ORDER BY dist " +
                        "LIMIT 2" );
        assert res.getData().length == 2;
        assert containsRows( res, true, true,
                Row.of( TestLiteral.from( "a" ), TestLiteral.from( "0.0" ) ),
                Row.of( TestLiteral.from( "b" ), TestLiteral.from( "1.4142135623730951" ) )
        );
    }


    @Test
    public void unknownMetricThrows() {
        execute( NODE_A );
        GraphResult res = CypherConnection.executeGetResponse(
                "MATCH (n:Item) " +
                        "RETURN vector_distance(n.embedding, [1.0, 1.0], 'UNKNOWN') AS dist" );
        assert res.getError() != null;
    }


    @Test
    public void l2SquaredMetric() {
        execute( NODE_A );
        execute( NODE_B );
        execute( NODE_C );
        GraphResult res = execute( "MATCH (n:Item) RETURN n.name, vector_distance(n.embedding, [1.0, 1.0], 'L2SQUARED') AS dist LIMIT 3" );
        assert containsRows( res, true, false,
                Row.of( TestLiteral.from( "a" ), TestLiteral.from( "0.0" ) ),
                Row.of( TestLiteral.from( "b" ), TestLiteral.from( "2.0" ) ),
                Row.of( TestLiteral.from( "c" ), TestLiteral.from( "5.0" ) )
        );
    }

}
