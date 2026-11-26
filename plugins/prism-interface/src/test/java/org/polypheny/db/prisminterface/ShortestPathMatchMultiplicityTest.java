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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.Frame;

@Tag("adapter")
class ShortestPathMatchMultiplicityTest {

    private static final String NAMESPACE = "cyphershortestmultiplicity";


    @Test
    void preservesAnonymousSegmentSplitsWithoutMultiplyingShortestPathTies() throws IOException {
        TestHelper.getInstance();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
            try {
                connection.execute( NAMESPACE, "cypher", """
                        CREATE (:SplitStart)-[:X]->()-[:X]->()-[:X]->(:SplitEnd),
                               (a:SoloStart)-[:S]->(b:SoloEnd), (a)-[:S]->(b)
                        """, 1 );
                assertEquals( 2, count( connection, """
                        MATCH (:SplitStart)-[:X*1..3]->()-[:X*1..3]->(:SplitEnd)
                        RETURN count(*) AS matches
                        """ ) );
                assertEquals( 2, count( connection, """
                        MATCH p=shortestPath((:SoloStart)-[:S*]->(:SoloEnd)),
                              (:SplitStart)-[:X*1..3]->()-[:X*1..3]->(:SplitEnd)
                        RETURN count(p) AS matches
                        """ ) );
                assertEquals( 8, count( connection, """
                        UNWIND [1, 1] AS input
                        OPTIONAL MATCH (:SplitStart)-[:X*0..3]->()-[:X*0..3]->(:SplitEnd),
                                       p=shortestPath((:SoloStart)-[:S*]->(:SoloEnd))
                        RETURN count(p) AS matches
                        """ ) );
                connection.checkTransactions( 0 );
            } finally {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + NAMESPACE, 1 );
            }
        }
    }


    private static long count( PrismConnection connection, String query ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return integerValue( singleRow( relationalFrame( frame, true, List.of( "matches" ) ) ).getValues( 0 ) );
    }

}
