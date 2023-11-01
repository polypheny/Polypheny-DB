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

import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyPoint;

public class GeometryTest {

    private static final String VALID_POINT_EWKT = "SRID=4326;POINT (13.4050 52.5200)";
    private static final String VALID_POINT_WKT = "POINT (13.4050 52.5200 36.754)";
    private static final double DELTA = 1e-15;
    private static final int NO_SRID = 0;

    @BeforeClass
    public static void start() {
        TestHelper.getInstance();
    }

    @Test
    public void testGeometryValidity() {
        // Point
        assertAll(
            "Group assertions of valid Point in EWKT",
            () -> assertDoesNotThrow( () -> PolyGeometry.of( VALID_POINT_EWKT ) ),
            () -> {
                PolyGeometry geometry = PolyGeometry.of( VALID_POINT_EWKT );
                assertEquals( "Point", geometry.getGeometryType() );
                assertEquals( 4326, (long) geometry.getSRID() );
                PolyPoint point = geometry.asPoint();
                assertEquals( 13.4050, point.getX(), DELTA );
                assertEquals( 52.5200, point.getY(), DELTA );
                assertFalse( point.hasZ() );
                assertFalse( point.hasM() );
            });

        assertAll(
            "Group assertions of valid Point in WKT",
            () -> assertDoesNotThrow(() -> PolyGeometry.of( VALID_POINT_WKT )),
            () -> {
                PolyGeometry geometry = PolyGeometry.of( VALID_POINT_WKT );
                assertEquals( "Point", geometry.getGeometryType() );
                assertEquals( NO_SRID, (long) geometry.getSRID() );
                PolyPoint point = geometry.asPoint();
                assertEquals( 13.4050, point.getX(), DELTA );
                assertEquals( 52.5200, point.getY(), DELTA );
                assertTrue( point.hasZ() );
                assertEquals( 36.754, point.getZ(), DELTA );
                assertFalse( point.hasM() );
        });

        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "POINT (13.4050)" ) );
        assertThrows( InvalidGeometryException.class, () -> PolyGeometry.of( "POINT (13.4050 13.4050 13.4050 13.4050 13.4050)" ) );
    }

    @Test
    public void testPointsEquality() throws InvalidGeometryException {
        PolyGeometry point1 = PolyGeometry.of( VALID_POINT_EWKT );
        PolyGeometry point2 = PolyGeometry.of( VALID_POINT_EWKT );
        PolyGeometry point3 = PolyGeometry.of( VALID_POINT_WKT );
        assertEquals( point1, point2 );
        assertNotEquals( point1, point3 );
    }

}