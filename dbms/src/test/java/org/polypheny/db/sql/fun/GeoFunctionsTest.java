/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.fun;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.excluded.CassandraExcluded;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class GeoFunctionsTest {

    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }

    @Test
    public void geoFromText() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // ST_GeoFromText with only 1 parameter (WKT)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeoFromText('POINT (0 1)')" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (0 1)" }
                        ) );
                // ST_GeoFromText with 2 parameters (WKT, SRID)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeoFromText('POINT (0 1)', 1)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=1;POINT (0 1)" }
                        ) );
            }
        }
    }

    @Test
    public void pointFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // get X coordinate of the point
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_X(ST_GeoFromText('POINT (0 1)'))" ),
                        ImmutableList.of(
                                new Object[]{ 0.0 }
                        ) );
            }
        }
    }

    @Test
    public void lineStringsFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // test if line is closed
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsClosed(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // test if line is a ring
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsRing(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // test if point is a coordinate of a line
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsCoordinate(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'), ST_GeoFromText('POINT (-1 -1)'))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check the starting point
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_StartPoint(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (-1 -1)" }
                        ) );
                // check the ending point
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_EndPoint(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (6 7)" }
                        ) );
            }
        }
    }

}
