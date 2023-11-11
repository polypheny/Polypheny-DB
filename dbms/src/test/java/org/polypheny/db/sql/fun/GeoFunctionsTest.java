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
    public void commonPropertiesFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // check that the geometry is simple
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsSimple(ST_GeoFromText('POINT (0 1)'))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that the geometry is empty
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsEmpty(ST_GeoFromText('POINT (0 1)'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // get the number of points in the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_NumPoints(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ 4 }
                        ) );
                // get the dimension of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Dimension(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ 1 }
                        ) );
                // get the length of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Length(ST_GeoFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ 10.67662 }
                        ) );
                // get the area of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Area(ST_GeoFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ 4.5 }
                        ) );
                // get the minimum bounding box of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Envelope(ST_GeoFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))" }
                        ) );
                // get the convex hull of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_ConvexHull(ST_GeoFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POLYGON ((-1 -1, -1 2, 2 2, -1 -1))" }
                        ) );
                // get the centroid of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Centroid(ST_GeoFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (-0 1)" }
                        ) );
            }
        }
    }

    @Test
    public void distanceFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // calculate the distance between two points
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Distance(ST_GeoFromText('POINT (7.852923 47.998949)', 4326), ST_GeoFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 134.45105 }
                        ) );
                // calculate the distance between point and linestring
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Distance(ST_GeoFromText('POINT (7.852923 47.998949)', 4326), ST_GeoFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 134.45105 } // still the same closest point
                        ) );
                // calculate the distance between point and polygon
                TestHelper.checkResultSet(                                                                                                                        //  -1 -1, 2 2, -1 2, -1 -1
                        statement.executeQuery( "SELECT ST_Distance(ST_GeoFromText('POINT (7.852923 47.998949)', 4326), ST_GeoFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 106.87882 }
                        ) );
            }
        }
    }

    @Test
    public void setOperationsFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // calculate the intersection of two points
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Intersection(ST_GeoFromText('POINT (7.852923 47.998949)', 4326), ST_GeoFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;POINT EMPTY" } // empty
                        ) );
                // calculate the union of two points
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Union(ST_GeoFromText('POINT (7.852923 47.998949)', 4326), ST_GeoFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;MULTIPOINT ((7.852923 47.998949), (9.289382 48.741588))" }
                        ) );
                // calculate the difference of linestring and polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Difference(ST_GeoFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), ST_GeoFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;LINESTRING (10.289382 47.741588, 12.289382 45.741588)" }
                        ) );
                // calculate the symmetrical difference of linestring and polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_SymDifference(ST_GeoFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), ST_GeoFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;GEOMETRYCOLLECTION (LINESTRING (10.289382 47.741588, 12.289382 45.741588), POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588)))" }
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


    @Test
    public void polygonFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // test if polygon is rectangle
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsRectangle(ST_GeoFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // retrieve the exterior ring of the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_ExteriorRing(ST_GeoFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;LINEARRING (-1 -1, 2 2, -1 2, -1 -1)" }
                        ) );
                // retrieve the number of interior ring of the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_NumInteriorRing(ST_GeoFromText('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))'))" ),
                        ImmutableList.of(
                                new Object[]{ 1 }
                        ) );
                // retrieve the nth interior ring of the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_InteriorRingN(ST_GeoFromText('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))'), 0)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;LINEARRING (1.5 1, 4 3, 4 1, 1.5 1)" }
                        ) );
            }
        }
    }

    @Test
    public void geometryCollectionFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // get the number of geometries in the collection
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_NumGeometries(ST_GeoFromText('GEOMETRYCOLLECTION ( POINT (2 3), LINESTRING (2 3, 3 4) )'))" ),
                        ImmutableList.of(
                                new Object[]{ 2 }
                        ) );
                // retrieve the nth geometry in the collection
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeometryN(ST_GeoFromText('GEOMETRYCOLLECTION ( POINT (2 3), LINESTRING (2 3, 3 4) )'), 1)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;LINESTRING (2 3, 3 4)" }
                        ) );
            }
        }
    }

}
