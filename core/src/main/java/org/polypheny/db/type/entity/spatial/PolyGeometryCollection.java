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

package org.polypheny.db.type.entity.spatial;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.annotations.Deserialize;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.polypheny.db.type.PolyType;

/**
 * {@link PolyGeometryCollection} is collection that holds {@link PolyGeometry}
 * and it is a base class for all <strong>Multi</strong> spatial data types.
 * <p>
 * Collection of {@link PolyGeometry} of any {@link PolyGeometryType} and dimension.
 */
public class PolyGeometryCollection extends PolyGeometry {

    private GeometryCollection jtsGeometryCollection;


    public PolyGeometryCollection( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = getPolyGeometryType();
        this.jtsGeometryCollection = (GeometryCollection) jtsGeometry;
    }


    public PolyGeometryCollection( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int srid ) throws InvalidGeometryException {
        super( wkt, srid );
        this.geometryType = getPolyGeometryType();
        this.jtsGeometryCollection = (GeometryCollection) jtsGeometry;
    }


    public PolyGeometryCollection( Geometry geometry ) {
        super( geometry );
        this.geometryType = getPolyGeometryType();
        this.jtsGeometry = geometry;
        this.jtsGeometryCollection = (GeometryCollection) jtsGeometry;
        this.SRID = geometry.getSRID();
    }


    public PolyGeometryCollection( Geometry geometry, int srid ) {
        super( geometry );
        this.geometryType = getPolyGeometryType();
        this.jtsGeometry = geometry;
        this.jtsGeometryCollection = (GeometryCollection) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyGeometryCollection( PolyType type ) {
        super( type );
        this.geometryType = getPolyGeometryType();
    }


    public static PolyGeometryCollection of( Geometry geometry ) {
        return new PolyGeometryCollection( geometry );
    }


    public static PolyGeometryCollection of( Geometry geometry, int srid ) {
        return new PolyGeometryCollection( geometry, srid );
    }


    public boolean isMultiPoint() {
        return geometryType.equals( PolyGeometryType.MULTIPOINT );
    }


    @NotNull
    public PolyMultiPoint asMultiPoint() {
        if ( isMultiPoint() ) {
            return PolyMultiPoint.of( jtsGeometry, getSRID() );
        }
        throw cannotParse( this, PolyMultiPoint.class );
    }


    public boolean isMultiLineString() {
        return geometryType.equals( PolyGeometryType.MULTILINESTRING );
    }


    @NotNull
    public PolyMultiLineString asMultiLineString() {
        if ( isMultiLineString() ) {
            return PolyMultiLineString.of( jtsGeometry, getSRID() );
        }
        throw cannotParse( this, PolyMultiLineString.class );
    }


    public boolean isMultiPolygon() {
        return geometryType.equals( PolyGeometryType.MULTIPOLYGON );
    }


    @NotNull
    public PolyMultiPolygon asMultiPolygon() {
        if ( isMultiPolygon() ) {
            return PolyMultiPolygon.of( jtsGeometry, getSRID() );
        }
        throw cannotParse( this, PolyMultiPolygon.class );
    }


    /**
     * @return the number of {@link PolyGeometry} geometries in the {@link PolyGeometryCollection}.
     */
    public int getNumGeometries() {
        return jtsGeometryCollection.getNumGeometries();
    }


    /**
     * Get the nth {@link PolyGeometry} in the collection.
     *
     * @param n number of the {@link PolyGeometry} in the collection
     * @return the nth {@link PolyGeometry} in the collection.
     */
    public PolyGeometry getGeometryN( int n ) {
        return PolyGeometry.of( jtsGeometryCollection.getGeometryN( n ), getSRID() );
    }


    /**
     * Compute the union set of all {@link PolyGeometry} in the collection.
     *
     * @return the union set of all {@link PolyGeometry} in the collection.
     */
    public PolyGeometry union() {
        return PolyGeometry.of( jtsGeometryCollection.union(), getSRID() );
    }

}
