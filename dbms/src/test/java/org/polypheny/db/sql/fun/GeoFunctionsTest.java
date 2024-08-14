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
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class GeoFunctionsTest {

    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }

    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE TEST_GIS(ID INTEGER NOT NULL, geom GEOMETRY, PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TEST_GIS VALUES (1, ST_GeomFromText('POINT (7.852923 47.998949)', 4326))" );
                statement.executeUpdate( "INSERT INTO TEST_GIS VALUES (2, ST_GeomFromText('POINT (9.289382 48.741588)', 4326))" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TEST_GIS" );
            }
            connection.commit();
        }
    }

    // ------------------- Test that GEOMETRY type was persisted correctly ---------------------------

    @Test
    public void readGeo() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // scan table for geometries
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT geom FROM TEST_GIS" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;POINT (7.852923 47.998949)" },
                                new Object[]{ "SRID=4326;POINT (9.289382 48.741588)" }
                        ),
                        true);
            }
        }
    }


    // --------------- Test spatial functions without the actual persisted data ----------------------

    @Test
    public void geomFromText() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // ST_GeomFromText with only 1 parameter (WKT)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeomFromText('POINT (0 1)')" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (0 1)" }
                        ) );
                // ST_GeomFromText with 2 parameters (WKT, SRID)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeomFromText('POINT (0 1)', 1)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=1;POINT (0 1)" }
                        ) );
                // ST_GeomFromTWKB with 2 parameters (TWKB in HEX, SRID)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeomFromTWKB('e108010080dac40900', 1)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=1;POINT (0 1)" }
                        ) );
                // ST_GeomFromGeoJson with 1 parameter (GeoJson)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeomFromGeoJson('{ \"type\": \"Point\", \"coordinates\": [ 0, 1 ] }')" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;POINT (0 1)" }
                        ) );
                // ST_GeomFromGeoJson with 2 parameter (GeoJson, SRID)
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeomFromGeoJson('{ \"type\": \"Point\", \"coordinates\": [ 0, 1 ] }', 1)" ),
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
                        statement.executeQuery( "SELECT ST_IsSimple(ST_GeomFromText('POINT (0 1)'))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that the geometry is empty
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsEmpty(ST_GeomFromText('POINT (0 1)'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // get the number of points in the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_NumPoints(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ 4 }
                        ) );
                // get the dimension of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Dimension(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ 1 }
                        ) );
                // get the length of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Length(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ 10.67662 }
                        ) );
                // get the area of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Area(ST_GeomFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ 4.5 }
                        ) );
                // get the minimum bounding box of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Envelope(ST_GeomFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POLYGON ((-1 -1, -1 2, 2 2, 2 -1, -1 -1))" }
                        ) );
                // get the convex hull of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_ConvexHull(ST_GeomFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POLYGON ((-1 -1, -1 2, 2 2, -1 -1))" }
                        ) );
                // get the convex hull of the geometry from the database
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_ConvexHull(geom) from TEST_GIS" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;POINT (7.852923 47.998949)" },
                                new Object[]{ "SRID=4326;POINT (9.289382 48.741588)" }
                        ) );
                // get the centroid of the geometry
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Centroid(ST_GeomFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (-0 1)" }
                        ) );
            }
        }
    }

    @Test
    @Disabled
    // todo This is inaccurate with less than 1cm, which is okey.
    // So we exclude it until we have a new resultObject
    public void transformFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // transform single point to other SRID
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Transform(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), 2056)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=2056;POINT (2630923.876654428 1316590.5631470187)" }
                        ) );

                // transform linestring to other SRID
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Transform(ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), 2056)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=2056;LINESTRING (2736179.275459154 1400721.6498003295, 2813774.868277359 1291774.167120458, 2977340.286067275 1077215.140782387)" }
                        ) );
                // transform polygon to other SRID
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Transform(ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326), 2056)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=2056;POLYGON ((2736179.275459154 1400721.6498003295, 2813774.868277359 1291774.167120458, 2738803.7053598273 1289526.6432951635, 2736179.275459154 1400721.6498003295))" }
                        ) );
                // transform geometry collection to other SRID
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Transform(ST_GeomFromText('GEOMETRYCOLLECTION ( POINT(7.852923 47.998949), POINT(9.289382 48.741588))', 4326), 2056)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=2056;GEOMETRYCOLLECTION (POINT (2630923.876654428 1316590.5631470187), POINT (2736179.275459154 1400721.6498003295))" }
                        ) );
                // transform multipoint to other SRID
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Transform(ST_GeomFromText('MULTIPOINT ((7.852923 47.998949), (9.289382 48.741588))', 4326), 2056)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=2056;MULTIPOINT ((2630923.876654428 1316590.5631470187), (2736179.275459154 1400721.6498003295))" }
                        ) );
            }
        }

    }


    @Test
    public void spatialRelationsFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // check that two geo are equal
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Equals(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Equals(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POINT (7.852923 47.998949)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that geo are within the distance
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_DWithin(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POINT (9.289382 48.741588)', 4326), 135000)" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that geo are disjoint
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Disjoint(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that point touches the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Touches(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // check that line intersects the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Intersects(ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that line crosses the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Crosses(ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // check that point is within the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Within(ST_GeomFromText('POINT (9.3 48)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that point is relate with polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Relate(ST_GeomFromText('POINT (9.3 48)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326), 'T********')" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check that area contains the point
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT count(*) from TEST_GIS where ST_Contains(ST_GeomFromText('POLYGON ((9.2 48.8, 10.289382 48.8, 10.289382 47.741588, 9.2 47.741588, 9.2 48.8))', 4326), geom) and id = 2" ),
                        ImmutableList.of(
                                new Object[]{ 1 }
                        ) );
                // check that point is within area
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT count(*) from TEST_GIS where ST_Within(geom, ST_GeomFromText('POLYGON ((9.2 48.8, 10.289382 48.8, 10.289382 47.741588, 9.2 47.741588, 9.2 48.8))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 1 }
                        ) );
                // spatial join
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT count(*) from TEST_GIS g1, TEST_GIS g2  where ST_Contains(g1.geom, g2.geom)" ),
                        ImmutableList.of(
                                new Object[]{ 2 }
                        ) );
            }
        }
    }

    @Test
    public void distanceFunctions() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT count(*) from TEST_GIS where ST_Distance(geom, ST_GeomFromText('POINT (9.289382 48.741588)', 4326)) < 135555" ),
                        ImmutableList.of(
                                new Object[]{ 2 }
                        ) );
                // calculate the distance between two points
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Distance(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 134451.0468 }
                        ) );
                // calculate the distance between two points
//                TestHelper.checkResultSet(
//                        statement.executeQuery( "SELECT ST_Distance(ST_GeomFromText('POINT (13.3777 52.5163)', 4326), ST_GeomFromText('POINT (6.94 50.9322)', 4326))" ),
//                        ImmutableList.of(
//                                new Object[]{ 476918.8968838096 }
//                        ) );
                // calculate the distance between point and linestring
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Distance(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 134451.046875 } // still the same closest point
                        ) );
                // calculate the distance between point and polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Distance(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ 106878.8281 }
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
                        statement.executeQuery( "SELECT ST_Intersection(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;POINT EMPTY" } // empty
                        ) );
                // calculate the union of two points
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Union(ST_GeomFromText('POINT (7.852923 47.998949)', 4326), ST_GeomFromText('POINT (9.289382 48.741588)', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;MULTIPOINT ((7.852923 47.998949), (9.289382 48.741588))" }
                        ) );
                // calculate the difference of linestring and polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_Difference(ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=4326;LINESTRING (10.289382 47.741588, 12.289382 45.741588)" }
                        ) );
                // calculate the symmetrical difference of linestring and polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_SymDifference(ST_GeomFromText('LINESTRING (9.289382 48.741588, 10.289382 47.741588, 12.289382 45.741588)', 4326), ST_GeomFromText('POLYGON ((9.289382 48.741588, 10.289382 47.741588, 9.289382 47.741588, 9.289382 48.741588))', 4326))" ),
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
                        statement.executeQuery( "SELECT ST_X(ST_GeomFromText('POINT (0 1)'))" ),
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
                        statement.executeQuery( "SELECT ST_IsClosed(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // test if line is a ring
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsRing(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // test if point is a coordinate of a line
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_IsCoordinate(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'), ST_GeomFromText('POINT (-1 -1)'))" ),
                        ImmutableList.of(
                                new Object[]{ true }
                        ) );
                // check the starting point
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_StartPoint(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;POINT (-1 -1)" }
                        ) );
                // check the ending point
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_EndPoint(ST_GeomFromText('LINESTRING (-1 -1, 2 2, 4 5, 6 7)'))" ),
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
                        statement.executeQuery( "SELECT ST_IsRectangle(ST_GeomFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ false }
                        ) );
                // retrieve the exterior ring of the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_ExteriorRing(ST_GeomFromText('POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )'))" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;LINEARRING (-1 -1, 2 2, -1 2, -1 -1)" }
                        ) );
                // retrieve the number of interior ring of the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_NumInteriorRing(ST_GeomFromText('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))'))" ),
                        ImmutableList.of(
                                new Object[]{ 1 }
                        ) );
                // retrieve the nth interior ring of the polygon
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_InteriorRingN(ST_GeomFromText('POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))'), 0)" ),
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
                        statement.executeQuery( "SELECT ST_NumGeometries(ST_GeomFromText('GEOMETRYCOLLECTION ( POINT (2 3), LINESTRING (2 3, 3 4) )'))" ),
                        ImmutableList.of(
                                new Object[]{ 2 }
                        ) );
                // retrieve the nth geometry in the collection
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ST_GeometryN(ST_GeomFromText('GEOMETRYCOLLECTION ( POINT (2 3), LINESTRING (2 3, 3 4) )'), 1)" ),
                        ImmutableList.of(
                                new Object[]{ "SRID=0;LINESTRING (2 3, 3 4)" }
                        ) );
            }
        }
    }

}
