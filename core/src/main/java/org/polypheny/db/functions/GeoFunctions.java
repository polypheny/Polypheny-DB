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

package org.polypheny.db.functions;

import static org.polypheny.db.functions.Functions.toUnchecked;

import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyFloat;
import org.polypheny.db.type.entity.PolyInteger;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyGeometryType.BufferCapStyle;

/**
 * Implementations of Geo functions
 */
public class GeoFunctions {

    private static final String POINT_RESTRICTION = "This function could be applied only to points";
    private static final String LINE_STRING_RESTRICTION = "This function could be applied only to line strings";
    private static final String POLYGON_RESTRICTION = "This function could be applied only to polygons";
    private static final String GEOMETRY_COLLECTION_RESTRICTION = "This function could be applied only to geometry collections";


    private GeoFunctions() {
        // empty on purpose
    }

    /*
     * Create Geometry
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeoFromText( PolyString wkt ) {
        try {
            return PolyGeometry.of( wkt.value );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeoFromText( PolyString wkt, PolyNumber srid ) {
        try {
            return PolyGeometry.of( wkt.value, srid.intValue() );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }

    /*
     * Common properties
     */
    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIsSimple( PolyGeometry geometry ) {
        return PolyBoolean.of( geometry.isSimple() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIsEmpty( PolyGeometry geometry ) {
        return PolyBoolean.of( geometry.isEmpty() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyInteger stNumPoints( PolyGeometry geometry ) {
        return PolyInteger.of( geometry.getNumPoints() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyInteger stDimension( PolyGeometry geometry ) {
        return PolyInteger.of( geometry.getDimension() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyFloat stLength( PolyGeometry geometry ) {
        return PolyFloat.of( geometry.getLength() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyFloat stArea( PolyGeometry geometry ) {
        return PolyFloat.of( geometry.getArea() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stEnvelope( PolyGeometry geometry ) {
        return geometry.getEnvelope();
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stBoundary( PolyGeometry geometry ) {
        return geometry.getBoundary();
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyInteger stBoundaryDimension( PolyGeometry geometry ) {
        return PolyInteger.of (geometry.getBoundaryDimension() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stConvexHull( PolyGeometry geometry ) {
        return geometry.convexHull();
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stCentroid( PolyGeometry geometry ) {
        return geometry.getCentroid();
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stReverse( PolyGeometry geometry ) {
        return geometry.reverse();
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stBuffer( PolyGeometry geometry, PolyNumber distance ) {
        return geometry.buffer( distance.doubleValue() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stBuffer( PolyGeometry geometry, PolyNumber distance, PolyNumber quadrantSegments ) {
        return geometry.buffer( distance.doubleValue(), quadrantSegments.intValue() );
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stBuffer( PolyGeometry geometry, PolyNumber distance, PolyNumber quadrantSegments, PolyString endCapStyle ) {
        return geometry.buffer( distance.doubleValue(), quadrantSegments.intValue(), BufferCapStyle.of( endCapStyle.value ) );
    }


    /*
     * Geometry Specific Functions
     */

    /*
     * on Points
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyFloat stX( PolyGeometry geometry ) {
        restrictToPoints( geometry );
        return PolyFloat.of( geometry.asPoint().getX() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyFloat stY( PolyGeometry geometry ) {
        restrictToPoints( geometry );
        return PolyFloat.of( geometry.asPoint().getY() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyFloat stZ( PolyGeometry geometry ) {
        restrictToPoints( geometry );
        return PolyFloat.of( geometry.asPoint().getZ() );
    }

    /*
     * on LineStrings
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIsClosed( PolyGeometry geometry ) {
        restrictToLineStrings( geometry );
        return PolyBoolean.of( geometry.asLineString().isClosed() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIsRing( PolyGeometry geometry ) {
        restrictToLineStrings( geometry );
        return PolyBoolean.of( geometry.asLineString().isRing() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIsCoordinate( PolyGeometry geometry, PolyGeometry point ) {
        restrictToLineStrings( geometry );
        restrictToPoints( point );
        return PolyBoolean.of( geometry.asLineString().isCoordinate( point.asPoint() ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stStartPoint( PolyGeometry geometry ) {
        restrictToLineStrings( geometry );
        return PolyGeometry.of( geometry.asLineString().getStartPoint().getJtsGeometry() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stEndPoint( PolyGeometry geometry ) {
        restrictToLineStrings( geometry );
        return PolyGeometry.of( geometry.asLineString().getEndPoint().getJtsGeometry() );
    }

    /*
     * on Polygons
     */

    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIsRectangle( PolyGeometry geometry ) {
        restrictToPolygons( geometry );
        return PolyBoolean.of( geometry.asPolygon().isRectangle() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stExteriorRing( PolyGeometry geometry ) {
        restrictToPolygons( geometry );
        return geometry.asPolygon().getExteriorRing();
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyInteger stNumInteriorRing( PolyGeometry geometry ) {
        restrictToPolygons( geometry );
        return PolyInteger.of( geometry.asPolygon().getNumInteriorRing() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stInteriorRingN( PolyGeometry geometry, PolyNumber n ) {
        restrictToPolygons( geometry );
        return PolyGeometry.of( geometry.asPolygon().getInteriorRingN( n.intValue() ).getJtsGeometry() );
    }

    /*
     * on GeometryCollection
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyInteger stNumGeometries( PolyGeometry geometry ) {
        restrictToGeometryCollection( geometry );
        return PolyInteger.of( geometry.asGeometryCollection().getNumGeometries() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeometryN( PolyGeometry geometry, PolyNumber n ) {
        restrictToGeometryCollection( geometry );
        return geometry.asGeometryCollection().getGeometryN( n.intValue() );
    }

    /*
     * Helpers
     */

    private static void restrictToPoints( PolyGeometry geometry ) {
        if ( !geometry.isPoint() ) {
            throw toUnchecked( new InvalidGeometryException( POINT_RESTRICTION ) );
        }
    }


    private static void restrictToLineStrings( PolyGeometry geometry ) {
        if ( !geometry.isLineString() ) {
            throw toUnchecked( new InvalidGeometryException( LINE_STRING_RESTRICTION ) );
        }
    }


    private static void restrictToPolygons( PolyGeometry geometry ) {
        if ( !geometry.isPolygon() ) {
            throw toUnchecked( new InvalidGeometryException( POLYGON_RESTRICTION ) );
        }
    }


    private static void restrictToGeometryCollection( PolyGeometry geometry ) {
        if ( !geometry.isGeometryCollection() ) {
            throw toUnchecked( new InvalidGeometryException( GEOMETRY_COLLECTION_RESTRICTION ) );
        }
    }

}
