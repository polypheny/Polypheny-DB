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

package org.polypheny.db.processing.caching;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;

@Tag("adapter")
class CypherPlanCacheTest {

    private static final String NAMESPACE = "cypher_plan_cache_test";


    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void reusesLabeledNodeAndPathPlansAcrossConnections() throws SQLException {
        boolean previousCaching = RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean();
        RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( true );
        try ( JdbcConnection fixture = new JdbcConnection( true ); Statement setup = fixture.getConnection().createStatement() ) {
            setup.executeUpdate( "CREATE GRAPH NAMESPACE " + NAMESPACE );
            try ( JdbcConnection second = new JdbcConnection( true ) ) {
                PolyStatement firstQuery = graphStatement( fixture.getConnection() );
                PolyStatement secondQuery = graphStatement( second.getConnection() );
                firstQuery.execute( NAMESPACE, "cypher", "CREATE (:CacheProbe {label: 'left'})-[:CACHE_EDGE]->(:CacheTarget {label: 'right'})" );

                assertReused( firstQuery, secondQuery, "MATCH (n:CacheProbe) RETURN n.label AS name", "left" );
                assertEquals( List.of( "right" ), query( secondQuery, "MATCH (n:CacheTarget) RETURN n.label AS name" ) );
                assertEquals( List.of(), query( secondQuery, "MATCH (n:CacheProbe {label: 'other'}) RETURN n.label AS name" ) );

                assertReused( firstQuery, secondQuery,
                        "MATCH (a:CacheProbe)-[r:CACHE_EDGE]->(b:CacheTarget) RETURN b.label AS name", "right" );
                assertEquals( List.of(), query( secondQuery,
                        "MATCH (a:CacheProbe)<-[r:CACHE_EDGE]-(b:CacheTarget) RETURN b.label AS name" ) );
                assertEquals( List.of(), query( secondQuery,
                        "MATCH (a:CacheProbe)-[r:OTHER_EDGE]->(b:CacheTarget) RETURN b.label AS name" ) );
            } finally {
                setup.executeUpdate( "DROP NAMESPACE " + NAMESPACE );
            }
        } finally {
            RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( previousCaching );
        }
    }


    private static void assertReused( PolyStatement first, PolyStatement second, String cypher, String expected ) throws SQLException {
        assertEquals( List.of( expected ), query( first, cypher ), cypher );
        long hits = ImplementationCache.INSTANCE.getHitCount();
        assertEquals( List.of( expected ), query( second, cypher ), cypher );
        assertEquals( hits + 1, ImplementationCache.INSTANCE.getHitCount(), cypher );
    }


    private static List<String> query( PolyStatement statement, String cypher ) throws SQLException {
        RelationalResult result = statement.execute( NAMESPACE, "cypher", cypher ).unwrap( RelationalResult.class );
        List<String> values = new ArrayList<>();
        for ( PolyRow row : result ) {
            values.add( row.getValue( 0 ).asString() );
        }
        return values;
    }


    private static PolyStatement graphStatement( Connection connection ) throws SQLException {
        return connection.unwrap( PolyConnection.class ).createPolyStatement();
    }

}
