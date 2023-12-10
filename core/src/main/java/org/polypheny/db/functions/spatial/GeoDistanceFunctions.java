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

package org.polypheny.db.functions.spatial;

import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.polypheny.db.type.entity.spatial.GeometryTopologicalException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyLineString;
import org.polypheny.db.type.entity.spatial.PolyPoint;

/**
 * Calculate the spherical distances between various geometries on the <strong>PERFECT SPHERE</strong>
 */
public class GeoDistanceFunctions {

    // Define the radius of the Earth's sphere (in meters)
    private static final double EARTH_RADIUS_KM = 6371.0 * 1000;


    private GeoDistanceFunctions() {
        // empty on purpose
    }


    /**
     * Check that the distance between two geometries is within the given threshold
     *
     * @param g1 one geometry
     * @param g2 another one
     * @param distanceThreshold limit the distance between geometries
     * @return <code>TRUE</code> if two geometries are within the given distance
     * @throws GeometryTopologicalException if distance cannot be calculated
     */
    public static boolean isWithinSphericalDistance( @NotNull PolyGeometry g1, @NotNull PolyGeometry g2, double distanceThreshold ) throws GeometryTopologicalException {
        return sphericalDistance( g1, g2 ) <= distanceThreshold;
    }


    /**
     * Calculate the spherical distance between 2 geometries
     *
     * @param g1 one geometry
     * @param g2 another
     * @return the distance between geometries
     * @throws GeometryTopologicalException if distance cannot be calculated
     */
    public static double sphericalDistance( @NotNull PolyGeometry g1, @NotNull PolyGeometry g2 ) throws GeometryTopologicalException {
        // distance calculation are dependent on the type of the geometry
        if ( g1.isPoint() && g2.isPoint() ) {
            return calculateSphericalDistance( g1.asPoint(), g2.asPoint() );
        } else if ( g1.isLineString() && g2.isLineString() ) {
            return calculateSphericalDistance( g1.asLineString(), g2.asLineString() );
        } else if ( g1.isLineString() && g2.isPoint() ) {
            return calculateSphericalDistance( g2.asPoint(), g1.asLineString() );
        } else if ( g1.isPoint() && g2.isLineString() ) {
            return calculateSphericalDistance( g1.asPoint(), g2.asLineString() );
        } else if ( g1.isPoint() && g2.isPolygon() ) {
            // we could use exterior ring (linestring)
            return calculateSphericalDistance( g1.asPoint(), g2.asPolygon().getExteriorRing() );
        } else if ( g1.isPolygon() && g2.isPoint() ) {
            return calculateSphericalDistance( g2.asPoint(), g1.asPolygon().getExteriorRing() );
        } else if ( g1.isPolygon() && g2.isPolygon() ) {
            return calculateSphericalDistance( g1.asPolygon().getExteriorRing(), g2.asPolygon().getExteriorRing() );
        }
        throw new GeometryTopologicalException( "Distance calculation is not supported for this data type." );
    }


    private static double calculateSphericalDistance( @NotNull PolyPoint polyPoint, @NotNull PolyLineString polyLine ) {
        LineString lineString = (LineString) polyLine.getJtsGeometry();
        Point point = (Point) polyPoint.getJtsGeometry();

        // Get coordinates from the LineString
        Coordinate[] coordinates = lineString.getCoordinates();
        // Initialize variables for closest distance and closest point
        double closestDistance = Double.MAX_VALUE;
        // Iterate over line segments and find the closest point
        for ( int i = 0; i < coordinates.length - 1; i++ ) {
            Coordinate start = coordinates[i];
            Coordinate end = coordinates[i + 1];

            LineSegment lineSegment = new LineSegment( start, end );
            Coordinate closestPointOnSegment = lineSegment.closestPoint( point.getCoordinate() );

            double distance = calculateSphericalDistance( PolyPoint.of( point ), PolyPoint.of( point.getFactory().createPoint( closestPointOnSegment ) ) );

            // Update the closest distance and point if the new distance is smaller
            if ( distance < closestDistance ) {
                closestDistance = distance;
            }
        }

        return closestDistance;
    }


    private static double calculateSphericalDistance( @NotNull PolyLineString polyLine1, @NotNull PolyLineString polyLine2 ) {
        LineString lineString1 = (LineString) polyLine1.getJtsGeometry();
        LineString lineString2 = (LineString) polyLine2.getJtsGeometry();
        // Get coordinates from LineStrings
        Coordinate[] coords1 = lineString1.getCoordinates();
        Coordinate[] coords2 = lineString2.getCoordinates();

        // Initialize cumulative distance
        double cumulativeDistance = 0.0;

        // Iterate over coordinates and calculate cumulative spherical distance
        cumulativeDistance = getCumulativeDistance( lineString1, coords1, cumulativeDistance );
        cumulativeDistance = getCumulativeDistance( lineString2, coords2, cumulativeDistance );

        return cumulativeDistance;
    }


    /**
     * Iterate over coordinates and calculate cumulative spherical distance
     */
    private static double getCumulativeDistance( LineString lineString, Coordinate[] coords, double cumulativeDistance ) {
        for ( int i = 0; i < coords.length - 1; i++ ) {
            Point point1 = lineString.getFactory().createPoint( coords[i] );
            Point point2 = lineString.getFactory().createPoint( coords[i + 1] );
            cumulativeDistance += calculateSphericalDistance( PolyPoint.of( point1 ), PolyPoint.of( point2 ) );
        }
        return cumulativeDistance;
    }


    private static double calculateSphericalDistance( @NotNull PolyPoint point1, @NotNull PolyPoint point2 ) {
        // Convert latitude and longitude from degrees to radians
        double lat1 = Math.toRadians( point1.getY() );
        double lon1 = Math.toRadians( point1.getX() );
        double lat2 = Math.toRadians( point2.getY() );
        double lon2 = Math.toRadians( point2.getX() );

        // Haversine formula for spherical distance
        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;
        double a = Math.sin( dLat / 2 ) * Math.sin( dLat / 2 ) + Math.cos( lat1 ) * Math.cos( lat2 ) * Math.sin( dLon / 2 ) * Math.sin( dLon / 2 );
        double c = 2 * Math.atan2( Math.sqrt( a ), Math.sqrt( 1 - a ) );
        double distance = EARTH_RADIUS_KM * c;

        if ( point1.hasZ() ) {
            double dAlt = point2.getZ() - point1.getZ();
            // Incorporate altitude difference
            distance = Math.sqrt( Math.pow( distance, 2 ) + Math.pow( dAlt, 2 ) );
        }

        return distance;
    }

}
