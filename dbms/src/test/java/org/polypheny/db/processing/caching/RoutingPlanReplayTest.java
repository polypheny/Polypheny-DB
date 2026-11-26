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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.config.RuntimeConfig;

@Tag("adapter")
class RoutingPlanReplayTest {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void replaysAReadInAnotherTransactionAndBypassesJoinsAndStaleSnapshots() throws SQLException {
        boolean routingCaching = RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean();
        boolean planCaching = RuntimeConfig.QUERY_PLAN_CACHING.getBoolean();
        boolean implementationCaching = RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean();
        RuntimeConfig.ROUTING_PLAN_CACHING.setBoolean( true );
        RuntimeConfig.QUERY_PLAN_CACHING.setBoolean( false );
        RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( false );
        try ( JdbcConnection fixture = new JdbcConnection( true ); Statement setup = fixture.getConnection().createStatement() ) {
            setup.execute( "CREATE TABLE routing_replay_left (id INTEGER PRIMARY KEY, label VARCHAR(20))" );
            setup.execute( "CREATE TABLE routing_replay_right (id INTEGER PRIMARY KEY, label VARCHAR(20))" );
            try {
                setup.executeUpdate( "INSERT INTO routing_replay_left VALUES (1, 'Aa'), (2, 'BB')" );
                setup.executeUpdate( "INSERT INTO routing_replay_right VALUES (1, 'right'), (2, 'other')" );
                RoutingPlanCache cache = RoutingPlanCache.INSTANCE;
                long hits = cache.getHitCount();
                long misses = cache.getMissCount();

                assertEquals( "Aa", findLabel( fixture.getConnection(), 1 ) );
                assertEquals( hits, cache.getHitCount() );
                assertEquals( misses + 1, cache.getMissCount() );

                try ( JdbcConnection second = new JdbcConnection( true ) ) {
                    assertEquals( "BB", findLabel( second.getConnection(), 2 ) );
                }
                assertEquals( hits + 1, cache.getHitCount() );
                assertEquals( misses + 1, cache.getMissCount() );

                for ( int i = 0; i < 2; i++ ) {
                    try ( ResultSet rows = setup.executeQuery( """
                            SELECT l.label, r.label FROM routing_replay_left l
                            JOIN routing_replay_right r ON l.id = r.id WHERE l.id = 1
                            """ ) ) {
                        assertTrue( rows.next() );
                        assertEquals( "Aa", rows.getString( 1 ) );
                        assertEquals( "right", rows.getString( 2 ) );
                        assertFalse( rows.next() );
                    }
                }
                assertEquals( hits + 1, cache.getHitCount() );
                assertEquals( misses + 1, cache.getMissCount() );

                setup.execute( "ALTER TABLE routing_replay_left ADD COLUMN extra INTEGER" );
                long hitsAfterDdl = cache.getHitCount();
                long missesAfterDdl = cache.getMissCount();
                assertEquals( "BB", findLabel( fixture.getConnection(), 2 ) );
                assertEquals( hitsAfterDdl, cache.getHitCount() );
                assertEquals( missesAfterDdl + 1, cache.getMissCount() );
            } finally {
                setup.execute( "DROP TABLE routing_replay_right" );
                setup.execute( "DROP TABLE routing_replay_left" );
            }
        } finally {
            RuntimeConfig.ROUTING_PLAN_CACHING.setBoolean( routingCaching );
            RuntimeConfig.QUERY_PLAN_CACHING.setBoolean( planCaching );
            RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( implementationCaching );
        }
    }


    private static String findLabel( Connection connection, int id ) throws SQLException {
        try ( PreparedStatement query = connection.prepareStatement( "SELECT label FROM routing_replay_left WHERE id = ?" ) ) {
            query.setInt( 1, id );
            try ( ResultSet rows = query.executeQuery() ) {
                assertTrue( rows.next() );
                String label = rows.getString( 1 );
                assertFalse( rows.next() );
                return label;
            }
        }
    }

}
