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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.Frame;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CypherMergeGraphExpressionsTest {

    private static final String NAMESPACE = "cyphermergegraphexpressions";
    private PrismConnection connection;


    @BeforeEach
    void resetGraph() throws IOException {
        TestHelper.getInstance();
        connection = new PrismConnection( true, 3 );
        connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + NAMESPACE, 1 );
        connection.execute( NAMESPACE, "cypher", "CREATE (:Source)-[:ROUTE]->(:Target)", 1 );
    }


    @AfterEach
    void dropGraph() throws IOException {
        try ( PrismConnection current = connection ) {
            if ( current != null ) {
                current.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + NAMESPACE, 1 );
            }
        }
    }


    @Test
    void evaluatesGraphExpressionsInPatternPropertiesAgainstEarlierRows() throws IOException {
        List<Row> rows = evaluate( """
                MATCH (a:Source), (b:Target)
                UNWIND [1,2] AS ordinal
                WITH a, b, ordinal ORDER BY ordinal
                MERGE (a)-[r:VISIT {ordinal:ordinal,
                    present:exists((a)-[:VISIT]->()),
                    hops:length(shortestPath((a)-[:ROUTE*]->(b)))}]->
                    (n:Visit {present:exists((a)-[:ROUTE]->(b))})
                RETURN r.present AS previous, r.hops AS hops, n.present AS present ORDER BY r.ordinal
                """, "previous", "hops", "present" ).getRowsList();

        assertEquals( 2, rows.size() );
        assertTrue( rows.get( 0 ).getValues( 0 ).hasBoolean() );
        assertFalse( rows.get( 0 ).getValues( 0 ).getBoolean().getBoolean() );
        assertTrue( rows.get( 1 ).getValues( 0 ).getBoolean().getBoolean() );
        for ( Row row : rows ) {
            assertEquals( 1, integerValue( row.getValues( 1 ) ) );
            assertTrue( row.getValues( 2 ).getBoolean().getBoolean() );
        }
        connection.checkTransactions( 0 );
    }


    @Test
    void onCreateReadsTheNewPatternAndEarlierSetItems() throws IOException {
        connection.execute( NAMESPACE, "cypher", """
                MATCH (a:Source)
                MERGE (a)-[r:VISIT]->(n:Visit)
                  ON CREATE SET r.ready = true,
                    n.present = exists((a)-[:VISIT {ready:true}]->(n)),
                    n.hops = length(shortestPath((a)-[:VISIT* {ready:true}]->(n)))
                """, 1 );

        assertVisitProperties();
        connection.checkTransactions( 0 );
    }


    @Test
    void onMatchReadsThePatternCreatedByAnEarlierRow() throws IOException {
        Row row = singleRow( evaluate( """
                MATCH (a:Source)
                UNWIND [1,1] AS key
                MERGE (a)-[r:VISIT {key:key}]->(n:Visit)
                  ON CREATE SET r.ready = true
                  ON MATCH SET n.present = exists((a)-[:VISIT {ready:true}]->(n)),
                    n.hops = length(shortestPath((a)-[:VISIT* {ready:true}]->(n)))
                RETURN count(*) AS matches
                """, "matches" ) );

        assertEquals( 2, integerValue( row.getValues( 0 ) ) );
        assertVisitProperties();
        connection.checkTransactions( 0 );
    }


    private void assertVisitProperties() throws IOException {
        Row row = singleRow( evaluate( "MATCH (:Source)-[:VISIT]->(n:Visit) RETURN n.present AS present, n.hops AS hops", "present", "hops" ) );
        assertTrue( row.getValues( 0 ).getBoolean().getBoolean() );
        assertEquals( 1, integerValue( row.getValues( 1 ) ) );
    }


    private RelationalFrame evaluate( String query, String... columns ) throws IOException {
        Frame frame = connection.execute( NAMESPACE, "cypher", query, -1 ).getResult().getFrame();
        return relationalFrame( frame, true, List.of( columns ) );
    }

}
