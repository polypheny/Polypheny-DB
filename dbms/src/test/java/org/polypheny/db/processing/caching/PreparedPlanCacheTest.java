/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.config.RuntimeConfig;

@Tag("adapter")
class PreparedPlanCacheTest {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @ParameterizedTest(name = "plan cache={0}, implementation cache={1}")
    @CsvSource({ "true,false", "false,true", "true,true" })
    void preparedQueriesReusePlansForDifferentRuntimeValues( boolean planCaching, boolean implementationCaching ) throws SQLException {
        boolean priorPlanCaching = RuntimeConfig.QUERY_PLAN_CACHING.getBoolean();
        boolean priorImplementationCaching = RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean();
        RuntimeConfig.QUERY_PLAN_CACHING.setBoolean( planCaching );
        RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( implementationCaching );
        try ( JdbcConnection fixture = new JdbcConnection( true ); Statement setup = fixture.getConnection().createStatement() ) {
            setup.execute( "CREATE TABLE prepared_plan_cache_test (id INTEGER PRIMARY KEY, label VARCHAR(20))" );
            try {
                setup.executeUpdate( "INSERT INTO prepared_plan_cache_test VALUES (1, 'Aa'), (2, 'BB')" );
                long plansBefore = QueryPlanCache.INSTANCE.getSize();
                long implementationsBefore = ImplementationCache.INSTANCE.getCacheSize();

                assertEquals( 1, findId( fixture.getConnection(), "Aa" ) );
                long plansAfterFirst = QueryPlanCache.INSTANCE.getSize();
                long implementationsAfterFirst = ImplementationCache.INSTANCE.getCacheSize();
                assertEquals( planCaching, plansAfterFirst > plansBefore, "Prepared SELECT must populate the plan cache when enabled" );
                assertEquals( implementationCaching, implementationsAfterFirst > implementationsBefore, "Prepared SELECT must populate the implementation cache when enabled" );
                long planHitsAfterFirst = QueryPlanCache.INSTANCE.getHitCount();
                long implementationHitsAfterFirst = ImplementationCache.INSTANCE.getHitCount();

                try ( JdbcConnection secondConnection = new JdbcConnection( true ) ) {
                    assertEquals( 2, findId( secondConnection.getConnection(), "BB" ) );
                }
                assertEquals( plansAfterFirst, QueryPlanCache.INSTANCE.getSize() );
                assertEquals( implementationsAfterFirst, ImplementationCache.INSTANCE.getCacheSize() );
                assertEquals( planCaching && !implementationCaching, QueryPlanCache.INSTANCE.getHitCount() > planHitsAfterFirst );
                assertEquals( implementationCaching, ImplementationCache.INSTANCE.getHitCount() > implementationHitsAfterFirst );
            } finally {
                setup.execute( "DROP TABLE prepared_plan_cache_test" );
            }
        } finally {
            RuntimeConfig.QUERY_PLAN_CACHING.setBoolean( priorPlanCaching );
            RuntimeConfig.IMPLEMENTATION_CACHING.setBoolean( priorImplementationCaching );
        }
    }


    private static int findId( Connection connection, String label ) throws SQLException {
        try ( PreparedStatement select = connection.prepareStatement( "SELECT id FROM prepared_plan_cache_test WHERE label = ?" ) ) {
            select.setString( 1, label );
            try ( ResultSet rows = select.executeQuery() ) {
                assertTrue( rows.next() );
                int id = rows.getInt( 1 );
                assertFalse( rows.next() );
                return id;
            }
        }
    }

}
