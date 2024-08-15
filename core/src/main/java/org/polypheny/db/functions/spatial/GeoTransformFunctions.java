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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransform;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.Proj4jException;
import org.locationtech.proj4j.ProjCoordinate;
import org.locationtech.proj4j.proj.Projection;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyGeometryCollection;

/**
 * Geo functions to:
 * Transform coordinates to various SRID
 * Project SRID coordinates on the plane
 */
public class GeoTransformFunctions {

    private static final String SRID_PREFIX = "EPSG:";
    private static final PrecisionModel PRECISION_MODEL = new PrecisionModel( PrecisionModel.FLOATING );
    // Create a CRSFactory to manage coordinate reference systems
    private static final CRSFactory crsFactory = new CRSFactory();


    private GeoTransformFunctions() {
        // empty on purpose
    }


    /**
     * Transform coordinates {@link PolyGeometry} into the given SRID.
     *
     * @param geometry to transform
     * @param srid new spatial reference
     * @return {@link PolyGeometry} with coordinates in given SRID
     * @throws InvalidGeometryException if coordinates cannot be transformed
     */
    public static PolyGeometry transform( PolyGeometry geometry, int srid ) throws InvalidGeometryException {
        // Create the original and target coordinate reference systems
        CoordinateReferenceSystem sourceCrs = crsFactory.createFromName( SRID_PREFIX + geometry.getSRID() );
        CoordinateReferenceSystem targetCrs = crsFactory.createFromName( SRID_PREFIX + srid );
        // create Geometry factory with new srid
        GeometryFactory geometryFactory = new GeometryFactory( PRECISION_MODEL, srid );
        // transform coordinates
        return modifyCoordinates( geometry, geometryFactory, (coordinates -> transformCoordinates( coordinates, sourceCrs, targetCrs )) );
    }


    /**
     * Project coordinates of {@link PolyGeometry} with SRID on the plane.
     *
     * @param geometry to transform
     * @return {@link PolyGeometry} with coordinates projected on the plane(<code>SRID=0</code>)
     * @throws InvalidGeometryException if coordinates cannot be projected
     */
    public static PolyGeometry project( PolyGeometry geometry ) throws InvalidGeometryException {
        // Create the coordinate reference systems
        CoordinateReferenceSystem crs = crsFactory.createFromName( SRID_PREFIX + geometry.getSRID() );
        // create Geometry factory with srid of the plane
        GeometryFactory geometryFactory = new GeometryFactory( PRECISION_MODEL, PolyGeometry.NO_SRID );
        // project coordinates
        return modifyCoordinates( geometry, geometryFactory, (coordinates -> projectCoordinates( coordinates, crs )) );
    }


    /**
     * Modify the coordinates using the modification function.
     *
     * @param geometry {@link PolyGeometry} original geometry
     * @param geometryFactory {@link GeometryFactory} with SRID that will create correct geometries
     * @param func to apply to coordinates
     * @return {@link PolyGeometry} with modified coordinates
     * @throws InvalidGeometryException in case modification of coordinates failed
     */
    private static PolyGeometry modifyCoordinates( PolyGeometry geometry, GeometryFactory geometryFactory, CoordinatesModificationFunction func ) throws InvalidGeometryException {
        if ( geometry.isGeometryCollection() ) {
            // for GeometryCollection every geometry is needed to be modified
            // and then the new GeometryCollection of modified geometries is created
            PolyGeometryCollection geometryCollection = geometry.asGeometryCollection();
            Geometry[] geometries = new Geometry[geometryCollection.getNumGeometries()];
            for ( int i = 0; i < geometryCollection.getNumGeometries(); i++ ) {
                Geometry geom = geometryCollection.getGeometryN( i ).getJtsGeometry();
                // Convert the Geometry to Proj4J coordinates
                ProjCoordinate[] coordinates = convertToProj4JCoordinates( geom );
                // Perform the SRID modification of coordinates
                ProjCoordinate[] modifiedCoordinates = func.apply( coordinates );
                // Convert Proj4J coordinates back to Geometry
                geometries[i] = convertToGeometry( modifiedCoordinates, geom, geometryFactory );
            }
            return createCorrectGeometryCollection( geometry, geometries, geometryFactory );
        } else {
            // coordinates of a single Geometry are modified
            // and Geometry of the same type with coordinates is returned
            ProjCoordinate[] coordinates = convertToProj4JCoordinates( geometry.getJtsGeometry() );
            ProjCoordinate[] modifiedCoordinates = func.apply( coordinates );
            return PolyGeometry.of( convertToGeometry( modifiedCoordinates, geometry.getJtsGeometry(), geometryFactory ) );
        }
    }


    /**
     * Convert Geometry coordinates to Proj4J coordinates
     *
     * @param geometry original {@link Geometry}
     * @return array of {@link ProjCoordinate} objects with coordinates of original {@link Geometry}
     */
    private static ProjCoordinate[] convertToProj4JCoordinates( Geometry geometry ) {
        ProjCoordinate[] projCoords = new ProjCoordinate[geometry.getNumPoints()];
        for ( int i = 0; i < geometry.getNumPoints(); i++ ) {
            projCoords[i] = new ProjCoordinate( geometry.getCoordinates()[i].getX(), geometry.getCoordinates()[i].getY() );
        }
        return projCoords;
    }


    /**
     * Convert Proj4J coordinates back to Geometry of correct type.
     *
     * @param projCoords coordinates in {@link ProjCoordinate}
     * @param geometry original {@link Geometry} for the reference
     * @param geometryFactory {@link GeometryFactory} with SRID that will create correct geometries
     * @return {@link Geometry} of correct type with new coordinates
     * @throws InvalidGeometryException if geometry cannot be created from provided coordinates
     */
    private static Geometry convertToGeometry( ProjCoordinate[] projCoords, Geometry geometry, GeometryFactory geometryFactory ) throws InvalidGeometryException {
        Coordinate[] originalCoordinates = geometry.getCoordinates();
        Coordinate[] coordinates = new Coordinate[projCoords.length];

        for ( int i = 0; i < projCoords.length; i++ ) {
            // X and Y are converted, Z is original
            coordinates[i] = new Coordinate( projCoords[i].x, projCoords[i].y, originalCoordinates[i].getZ() );
        }
        switch ( geometry.getGeometryType() ) {
            case Geometry.TYPENAME_POINT:
                return geometryFactory.createPoint( coordinates[0] );
            case Geometry.TYPENAME_LINESTRING:
                return geometryFactory.createLineString( coordinates );
            case Geometry.TYPENAME_LINEARRING:
                return geometryFactory.createLinearRing( coordinates );
            case Geometry.TYPENAME_POLYGON:
                return geometryFactory.createPolygon( coordinates );
            default:
                throw new InvalidGeometryException( "Cannot convert back to Geometry" );
        }

    }


    /**
     * Create the geometry collection of correct types
     *
     * @param geometry original of {@link PolyGeometry}
     * @param geometries array of {@link Geometry}
     * @param geometryFactory {@link GeometryFactory} with SRID that will create correct geometries
     * @return {@link PolyGeometry} that incorporates the correct {@link org.locationtech.jts.geom.GeometryCollection}
     */
    private static PolyGeometry createCorrectGeometryCollection( PolyGeometry geometry, Geometry[] geometries, GeometryFactory geometryFactory ) throws InvalidGeometryException {
        switch ( geometry.getGeometryType() ) {
            case GEOMETRYCOLLECTION:
                return PolyGeometry.of( geometryFactory.createGeometryCollection( geometries ) );
            case MULTIPOINT:
                Point[] points = new Point[geometries.length];
                for ( int i = 0; i < geometries.length; i++ ) {
                    points[i] = geometryFactory.createPoint( geometries[i].getCoordinate() );
                }
                return PolyGeometry.of( geometryFactory.createMultiPoint( points ) );
            case MULTILINESTRING:
                LineString[] lineStrings = new LineString[geometries.length];
                for ( int i = 0; i < geometries.length; i++ ) {
                    lineStrings[i] = geometryFactory.createLineString( geometries[i].getCoordinates() );
                }
                return PolyGeometry.of( geometryFactory.createMultiLineString( lineStrings ) );
            case MULTIPOLYGON:
                Polygon[] polygons = new Polygon[geometries.length];
                for ( int i = 0; i < geometries.length; i++ ) {
                    polygons[i] = geometryFactory.createPolygon( geometries[i].getCoordinates() );
                }
                return PolyGeometry.of( geometryFactory.createMultiPolygon( polygons ) );
            default:
                throw new InvalidGeometryException( "Cannot convert back to GeometryCollection" );
        }
    }


    /**
     * Perform the SRID transformation
     *
     * @param originalCoords in the original SRID
     * @param sourceCrs original reference system
     * @param targetCrs target reference system with new SRID
     * @return coordinates in target SRID
     */
    private static ProjCoordinate[] transformCoordinates(
            ProjCoordinate[] originalCoords, CoordinateReferenceSystem sourceCrs, CoordinateReferenceSystem targetCrs ) {
        try {
            CoordinateTransform trans = new CoordinateTransformFactory().createTransform( sourceCrs, targetCrs );
            ProjCoordinate[] targetCoords = new ProjCoordinate[originalCoords.length];
            for ( int i = 0; i < originalCoords.length; i++ ) {
                targetCoords[i] = new ProjCoordinate();
                trans.transform( originalCoords[i], targetCoords[i] );
            }
            return targetCoords;
        } catch ( Proj4jException e ) {
            throw new RuntimeException( "Error in coordinate transformation.", e );
        }
    }


    /**
     * Perform the SRID projection
     *
     * @param originalCoords in the original SRID
     * @param crs spatial reference system
     * @return projected coordinates on the plane
     */
    private static ProjCoordinate[] projectCoordinates(
            ProjCoordinate[] originalCoords, CoordinateReferenceSystem crs ) {
        try {
            Projection projection = crs.getProjection();
            ProjCoordinate[] targetCoords = new ProjCoordinate[originalCoords.length];
            for ( int i = 0; i < originalCoords.length; i++ ) {
                targetCoords[i] = new ProjCoordinate();
                // project radians on the plane
                projection.projectRadians( originalCoords[i], targetCoords[i] );
            }
            return targetCoords;
        } catch ( Proj4jException e ) {
            throw new RuntimeException( "Error in coordinate projection.", e );
        }
    }


    @FunctionalInterface
    interface CoordinatesModificationFunction {

        ProjCoordinate[] apply( ProjCoordinate[] coordinates );

    }

}
