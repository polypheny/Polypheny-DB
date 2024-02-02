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

import java.util.Objects;
import org.polypheny.db.functions.spatial.GeoTransformFunctions;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.spatial.GeometryTopologicalException;
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
    private static final String SRID_RESTRICTION = "Geometries of the same SRID should be used";


    private GeoFunctions() {
        // empty on purpose
    }

    /*
     * Create Geometry
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeomFromText( PolyString wkt ) {
        try {
            return PolyGeometry.fromWKT( wkt.value );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeomFromText( PolyString wkt, PolyNumber srid ) {
        try {
            return PolyGeometry.fromWKT( wkt.value, srid.intValue() );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeomFromTWKB( PolyString twkb ) {
        try {
            return PolyGeometry.fromTWKB( twkb.value );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeomFromTWKB( PolyString twkb, PolyNumber srid ) {
        try {
            return PolyGeometry.fromTWKB( twkb.value, srid.intValue() );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeomFromGeoJson( PolyString geoJson ) {
        try {
            return PolyGeometry.fromGeoJson( geoJson.value );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeomFromGeoJson( PolyString geoJson, PolyNumber srid ) {
        try {
            return PolyGeometry.fromGeoJson( geoJson.value, srid.intValue() );
        } catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyString stAsText( PolyGeometry geometry ) {
        return PolyString.of( geometry.toString() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyString stAsTWKB( PolyGeometry geometry ) {
        return PolyString.of( geometry.toBinary() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyString stAsGeoJson( PolyGeometry geometry ) {
        return PolyString.of( geometry.toJson() );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stTransform( PolyGeometry geometry, PolyNumber srid ) {
        try {
            return GeoTransformFunctions.transform( geometry, srid.intValue() );
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
        return PolyInteger.of( geometry.getBoundaryDimension() );
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
     * Spatial relationships
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stEquals( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.equals( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stDWithin( PolyGeometry g1, PolyGeometry g2, PolyNumber distance ) {
        restrictToSrid( g1, g2 );
        try {
            return PolyBoolean.of( g1.isWithinDistance( g2, distance.doubleValue() ) );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stDisjoint( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.disjoint( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stTouches( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.touches( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stIntersects( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.intersects( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stCrosses( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.crosses( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stWithin( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.within( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stContains( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.contains( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stOverlaps( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.overlaps( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stCovers( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.covers( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stCoveredBy( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        return PolyBoolean.of( g1.coveredBy( g2 ) );
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyBoolean stRelate( PolyGeometry g1, PolyGeometry g2, PolyString pattern ) {
        restrictToSrid( g1, g2 );
        try {
            return PolyBoolean.of( g1.relate( g2, pattern.value ) );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
    }



    /*
     * Yield metric values
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyNumber stDistance( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        try {
            return PolyFloat.of( g1.distance( g2 ) );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
    }

    /*
     * Set operations
     */


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stIntersection( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        try {
            return g1.intersection( g2 );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stUnion( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        try {
            return g1.union( g2 );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stDifference( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        try {
            return g1.difference( g2 );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stSymDifference( PolyGeometry g1, PolyGeometry g2 ) {
        restrictToSrid( g1, g2 );
        try {
            return g1.symDifference( g2 );
        } catch ( GeometryTopologicalException e ) {
            throw toUnchecked( e );
        }
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
        restrictToSrid( geometry, point );
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


    private static void restrictToSrid( PolyGeometry g1, PolyGeometry g2 ) {
        if ( !Objects.equals( g1.getSRID(), g2.getSRID() ) ) {
            throw toUnchecked( new GeometryTopologicalException( SRID_RESTRICTION ) );
        }
    }

}
