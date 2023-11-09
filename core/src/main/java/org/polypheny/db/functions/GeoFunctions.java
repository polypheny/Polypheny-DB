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

/**
 * Implementations of Geo functions
 */
public class GeoFunctions {

    private static final String POINT_RESTRICTION = "This function could be applied only to points";
    private static final String LINE_STRING_RESTRICTION = "This function could be applied only to line strings";
    private static final String POLYGON_RESTRICTION = "This function could be applied only to polygons";


    private GeoFunctions() {
        // empty on purpose
    }


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

}
