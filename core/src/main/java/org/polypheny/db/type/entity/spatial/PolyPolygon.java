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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.polypheny.db.type.PolyType;

/**
 * Represent a polygon with linear edges, may contain holes.
 * The outer boundary (shell) and inner boundaries (holes) are represented by {@link PolyLinearRing}.
 * It means {@link PolyPolygon} is simple and closed.
 * Interior holes should not split the {@link PolyPolygon} into more than one part.
 * <p>
 * The {@link PolyPolygon} is valid if
 * outer and inner boundaries are valid ({@link PolyLinearRing})
 * holes touch the shell or another hole at most one point
 * Interior holes should not make the interior of the polygon disconnected
 */
public class PolyPolygon extends PolyGeometry {

    private Polygon jtsPolygon;


    public PolyPolygon( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.POLYGON;
        this.jtsPolygon = (Polygon) jtsGeometry;
    }


    public PolyPolygon( String wkt, int SRID ) throws InvalidGeometryException {
        super( wkt, SRID );
        this.geometryType = PolyGeometryType.POLYGON;
        this.jtsPolygon = (Polygon) jtsGeometry;
    }


    public PolyPolygon( Geometry geometry ) {
        super( geometry );
        this.geometryType = PolyGeometryType.POLYGON;
        this.jtsGeometry = geometry;
        this.jtsPolygon = (Polygon) jtsGeometry;
        this.SRID = geometry.getSRID();
    }


    protected PolyPolygon( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.POLYGON;
    }


    public static PolyPolygon of( Geometry geometry ) {
        return new PolyPolygon( geometry );
    }


    public PolyLinearRing getExteriorRing() {
        return PolyLinearRing.of( jtsPolygon.getExteriorRing() );
    }


    public int getNumInteriorRing() {
        return jtsPolygon.getNumInteriorRing();
    }


    public PolyLinearRing getInteriorRingN( int n ) {
        return PolyLinearRing.of( jtsPolygon.getInteriorRingN( n ) );
    }

}
