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

package org.polypheny.db.type.spatial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyGeometryCollection;
import org.polypheny.db.type.entity.spatial.PolyGeometryType;
import org.polypheny.db.type.entity.spatial.PolyLineString;
import org.polypheny.db.type.entity.spatial.PolyLinearRing;
import org.polypheny.db.type.entity.spatial.PolyMultiLineString;
import org.polypheny.db.type.entity.spatial.PolyMultiPoint;
import org.polypheny.db.type.entity.spatial.PolyMultiPolygon;
import org.polypheny.db.type.entity.spatial.PolyPoint;
import org.polypheny.db.type.entity.spatial.PolyPolygon;

public class GeometryTest {

    private PolyGeometry point2d;
    private PolyGeometry point3d;
    private PolyGeometry lineString;
    private PolyGeometry linearRing;
    private PolyGeometry polygon;
    private PolyGeometry geometryCollection;
    private PolyGeometry multiPoint;
    private PolyGeometry multiLineString;
    private PolyGeometry multiPolygon;


    @BeforeClass
    public static void start() {
        TestHelper.getInstance();
    }

    @Before
    public void prepareGeometries() throws InvalidGeometryException {
        point2d = PolyGeometry.of( GeometryConstants.POINT_EWKT );
        point3d = PolyGeometry.of( GeometryConstants.POINT_WKT );
        lineString = PolyGeometry.of( GeometryConstants.LINESTRING_WKT );
        linearRing = PolyGeometry.of( GeometryConstants.LINEAR_RING_WKT );
        polygon = PolyGeometry.of( GeometryConstants.POLYGON_WKT );
        geometryCollection = PolyGeometry.of( GeometryConstants.GEOMETRYCOLLECTION_WKT );
        multiPoint = PolyGeometry.of( GeometryConstants.MULTIPOINT_WKT );
        multiLineString = PolyGeometry.of( GeometryConstants.MULTILINESTRING_WKT );
        multiPolygon = PolyGeometry.of( GeometryConstants.MULTIPOLYGON_WKT );
    }


    @Test
    public void testPointValidity() {
        assertAll( "Group assertions of valid Point in EWKT",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.POINT_EWKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.POINT, point2d.getGeometryType() );
                    assertEquals( 4326, (long) point2d.getSRID() );
                    PolyPoint point = point2d.asPoint();
                    assertEquals( 13.4050, point.getX(), GeometryConstants.DELTA );
                    assertEquals( 52.5200, point.getY(), GeometryConstants.DELTA );
                    assertFalse( point.hasZ() );
                    assertFalse( point.hasM() );
        } );

        assertAll( "Group assertions of valid Point in WKT",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.POINT_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.POINT, point3d.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) point3d.getSRID() );
                    PolyPoint point = point3d.asPoint();
                    assertEquals( 13.4050, point.getX(), GeometryConstants.DELTA );
                    assertEquals( 52.5200, point.getY(), GeometryConstants.DELTA );
                    assertTrue( point.hasZ() );
                    assertEquals( 36.754, point.getZ(), GeometryConstants.DELTA );
                    assertFalse( point.hasM() );
        } );

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "POINT (13.4050)" ) );
        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "POINT (13.4050 13.4050 13.4050 13.4050 13.4050)" ) );
    }


    @Test
    public void testLineStringValidity() {
        assertAll( "Group assertions of valid LineString",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.LINESTRING_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.LINESTRING, lineString.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) lineString.getSRID() );
                    PolyLineString line = lineString.asLineString();
                    assertEquals( 10.6766191, line.getLength(), GeometryConstants.DELTA );
                    assertEquals( 4, line.getNumPoints() );
                    assertEquals( PolyPoint.of( "POINT(6 7)" ), line.getEndPoint() );
                    assertFalse( line.isEmpty() );
                    assertTrue( line.isSimple() );
                    assertFalse( line.isClosed() );
        } );

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "LINESTRING (0 0)" ) );
    }

    @Test
    public void testLinearRingValidity() {
        assertAll( "Group assertions of valid LinearRing",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.LINEAR_RING_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.LINEARRING, linearRing.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) linearRing.getSRID() );
                    PolyLinearRing ring = linearRing.asLinearRing();
                    assertTrue( ring.isRing() );
                    assertEquals( 40, ring.getLength(), GeometryConstants.DELTA );
                    assertEquals( 5, ring.getNumPoints() );
                } );

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "LINEARRING (0 0, 0 10, 10 10, 10 5, 5 5)" ) );
    }

    @Test
    public void testPolygonValidity() {
        assertAll( "Group assertions of valid Polygon",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.POLYGON_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.POLYGON, polygon.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) polygon.getSRID() );
                    PolyPolygon poly = polygon.asPolygon();
                    assertEquals( PolyPoint.of( "POINT (-0 1)" ), poly.getCentroid() );
                    assertEquals( PolyLinearRing.of( "LINEARRING (-1 -1, 2 2, -1 2, -1 -1)" ), poly.getBoundary() );
                    assertEquals( poly.getExteriorRing(), poly.getBoundary() );
                    assertEquals( 10.2426406, poly.getLength(), GeometryConstants.DELTA );
                    assertEquals( 4.5, poly.getArea(), GeometryConstants.DELTA );
                    assertEquals( 4, poly.getNumPoints() );
                } );

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "POLYGON ((-1 -1, 2 2, -1 1, -1 2, 2 2, -1 -1))" ) );
    }

    @Test
    public void testGeometryCollectionValidity() {
        assertAll( "Group assertions of valid GeometryCollection",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.GEOMETRYCOLLECTION_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.GEOMETRYCOLLECTION, geometryCollection.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) geometryCollection.getSRID() );
                    PolyGeometryCollection collection = geometryCollection.asGeometryCollection();
                    assertEquals( 2, collection.getNumGeometries() );
                    assertEquals( PolyLinearRing.of( "POINT(2 3)" ), collection.getGeometryN( 0 ) );
                    assertEquals( 3, collection.getNumPoints() );
                } );
        // GEOMETRYCOLLECTION do not have any extra validity rules, geometries may overlap
        assertDoesNotThrow( () -> PolyGeometry.of( "GEOMETRYCOLLECTION ( POINT (2 3), POINT (2 3) )" ) );
        assertDoesNotThrow( () -> PolyGeometry.of( "GEOMETRYCOLLECTION ( POLYGON ((-1 -1, 2 2, -1 2, -1 -1 )), POLYGON ((-1 -1, 2 2, -1 2, -1 -1 )) )" ) );
    }

    @Test
    public void testMultiPointValidity() {
        assertAll( "Group assertions of valid MultiPoint",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.MULTIPOINT_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.MULTIPOINT, multiPoint.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) multiPoint.getSRID() );
                    PolyMultiPoint multi = multiPoint.asGeometryCollection().asMultiPoint();
                    assertEquals( 2, multi.getNumGeometries() );
                    assertEquals( 2, multi.getNumPoints() );
                } );
        // points may overlap
        assertDoesNotThrow( () -> PolyGeometry.of( "MULTIPOINT ( (2 3), (2 3) )" ) );
        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "MULTIPOINT( (2 3), LINEARRING (-1 -1, 2 2, -1 2, -1 -1) )" ) );
    }

    @Test
    public void testMultiLineStringValidity() {
        assertAll( "Group assertions of valid MultiLineString",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.MULTILINESTRING_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.MULTILINESTRING, multiLineString.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) multiLineString.getSRID() );
                    PolyMultiLineString multi = multiLineString.asGeometryCollection().asMultiLineString();
                    assertEquals( 2, multi.getNumGeometries() );
                    assertEquals( 6, multi.getNumPoints() );
                } );
        // line strings may overlap
        assertDoesNotThrow( () -> PolyGeometry.of( "MULTILINESTRING ( (0 0, 1 1, 1 2), (0 0, 1 1, 1 2) )" ) );
        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "MULTILINESTRING ( (2 3), (2 3) )" ) );
    }

    @Test
    public void testMultiPolygonValidity() {
        assertAll( "Group assertions of valid MultiPolygon",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.MULTIPOLYGON_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.MULTIPOLYGON, multiPolygon.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) multiPolygon.getSRID() );
                    PolyMultiPolygon multi = multiPolygon.asGeometryCollection().asMultiPolygon();
                    assertEquals( 2, multi.getNumGeometries() );
                    assertEquals( 9, multi.getNumPoints() );
                } );
        // Polygons are not allowed to overlap
        assertThrows( InvalidGeometryException.class,
                () -> PolyGeometry.of(
                        "MULTIPOLYGON (( (1 5, 5 5, 5 1, 1 1, 1 5) ), (-1 -1, 2 2, -1 2, -1 -1 ))" )
        );
    }


    @Test
    public void testPointsEquality() throws InvalidGeometryException {
        PolyGeometry point2 = PolyGeometry.of( GeometryConstants.POINT_EWKT );
        assertEquals( point2d, point2 );
        assertNotEquals( point2d, point3d );
    }

    @Test
    public void testLineStringEquality() throws InvalidGeometryException {
        PolyGeometry line = PolyGeometry.of( GeometryConstants.LINESTRING_WKT );
        assertEquals( lineString, line );
        assertNotEquals( lineString, linearRing );
    }

    @Test
    public void testLinearRingEquality() throws InvalidGeometryException {
        PolyGeometry ring = PolyGeometry.of( GeometryConstants.LINEAR_RING_WKT );
        assertEquals( linearRing, ring );
        assertNotEquals( linearRing, lineString );
    }


    @Test
    public void testSerializability() {
        String serialized = point2d.serialize();
        PolyValue polyValue = PolyGeometry.deserialize( serialized );
        PolyGeometry deserializedGeometry = polyValue.asGeometry();
        assertEquals( point2d, deserializedGeometry );
    }

}