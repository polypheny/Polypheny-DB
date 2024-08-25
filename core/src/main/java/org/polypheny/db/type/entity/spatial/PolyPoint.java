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

package org.polypheny.db.type.entity.spatial;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.annotations.Deserialize;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.polypheny.db.type.PolyType;


/**
 * Represent a single point in the space.
 * <p>
 * The {@link PolyPoint} is valid if
 * <strong>X</strong> and <strong>Y</strong> coordinates are provided.
 * The {@link PolyPoint} could store up to 4 dimensions
 */
public class PolyPoint extends PolyGeometry {

    private Point jtsPoint;


    public PolyPoint( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.POINT;
        this.jtsPoint = (Point) jtsGeometry;
    }


    public PolyPoint( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int SRID ) throws InvalidGeometryException {
        super( wkt, SRID );
        this.geometryType = PolyGeometryType.POINT;
        this.jtsPoint = (Point) jtsGeometry;
    }


    protected PolyPoint( Geometry geometry ) {
        super( PolyType.GEOMETRY );
        this.geometryType = PolyGeometryType.POINT;
        this.jtsGeometry = geometry;
        this.jtsPoint = (Point) jtsGeometry;
        this.SRID = geometry.getSRID();
    }


    protected PolyPoint( Geometry geometry, int srid ) {
        super( PolyType.GEOMETRY );
        this.geometryType = PolyGeometryType.POINT;
        this.jtsGeometry = geometry;
        this.jtsPoint = (Point) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyPoint( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.POINT;
    }


    public static PolyPoint of( Geometry geometry ) {
        return new PolyPoint( geometry );
    }


    public static PolyPoint of( Geometry geometry, int srid ) {
        return new PolyPoint( geometry, srid );
    }


    public double getX() {
        return jtsPoint.getX();
    }


    public double getY() {
        return jtsPoint.getY();
    }


    public boolean hasZ() {
        return !Double.isNaN( jtsPoint.getCoordinate().getZ() );
    }


    public double getZ() {
        return jtsPoint.getCoordinate().getZ();
    }


    public boolean hasM() {
        return !Double.isNaN( jtsPoint.getCoordinate().getM() );
    }


    public double getM() {
        return jtsPoint.getCoordinate().getM();
    }

}
