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
import org.polypheny.db.type.entity.spatial.PolyGeometryType;
import org.polypheny.db.type.entity.spatial.PolyLineString;
import org.polypheny.db.type.entity.spatial.PolyLinearRing;
import org.polypheny.db.type.entity.spatial.PolyPoint;

public class GeometryTest {

    private PolyGeometry point2d;
    private PolyGeometry point3d;
    private PolyGeometry lineString;
    private PolyGeometry linearRing;


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

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "LINESTRING(0 0)" ) );
    }

    @Test
    public void testLinearRingValidity() {
        assertAll( "Group assertions of valid LinearRing",
                () -> assertDoesNotThrow( () -> PolyGeometry.of( GeometryConstants.LINEAR_RING_WKT ) ),
                () -> {
                    assertEquals( PolyGeometryType.LINEAR_RING, linearRing.getGeometryType() );
                    assertEquals( GeometryConstants.NO_SRID, (long) linearRing.getSRID() );
                    PolyLinearRing ring = linearRing.asLinearRing();
                    assertTrue( ring.isRing() );
                    assertEquals( 40, ring.getLength(), GeometryConstants.DELTA );
                    assertEquals( 5, ring.getNumPoints() );
                } );

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "LINEARRING(0 0, 0 10, 10 10, 10 5, 5 5)" ) );
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