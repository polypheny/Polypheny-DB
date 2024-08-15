/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.locationtech.jts.operation.distance.DistanceOp;
import org.polypheny.db.type.entity.spatial.PolyGeometry;

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
     */
    public static boolean isWithinSphericalDistance( @NotNull PolyGeometry g1, @NotNull PolyGeometry g2, double distanceThreshold ) {
        return sphericalDistance( g1, g2 ) <= distanceThreshold;
    }


    /**
     * Calculate the spherical distance between 2 geometries
     *
     * @param g1 one geometry
     * @param g2 another
     * @return the distance between geometries
     */
    public static double sphericalDistance( @NotNull PolyGeometry g1, @NotNull PolyGeometry g2 ) {
        Coordinate[] closestPoints = new DistanceOp( g1.getJtsGeometry(), g2.getJtsGeometry() ).nearestPoints();
        return calculateSphericalDistance( closestPoints[0], closestPoints[1] );
    }


    private static double calculateSphericalDistance( @NotNull Coordinate point1, @NotNull Coordinate point2 ) {
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

        if ( !Double.isNaN( point1.getZ() ) ) {
            double dAlt = point2.getZ() - point1.getZ();
            // Incorporate altitude difference
            distance = Math.sqrt( Math.pow( distance, 2 ) + Math.pow( dAlt, 2 ) );
        }

        return distance;
    }

}
