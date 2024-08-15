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
import org.locationtech.jts.geom.LinearRing;
import org.polypheny.db.type.PolyType;

/**
 * {@link PolyLinearRing} is {@link PolyLineString} that is simple and closed.
 * The {@link PolyLinearRing} is valid if it has
 * either <strong>0</strong> or <strong>3</strong> or <strong>more</strong> points.
 */
public class PolyLinearRing extends PolyLineString {

    private LinearRing jtsLinearRing;


    public PolyLinearRing( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.LINEARRING;
        this.jtsLinearRing = (LinearRing) jtsGeometry;
    }


    public PolyLinearRing( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int SRID ) throws InvalidGeometryException {
        super( wkt, SRID );
        this.geometryType = PolyGeometryType.LINEARRING;
        this.jtsLinearRing = (LinearRing) jtsGeometry;
    }


    protected PolyLinearRing( Geometry geometry ) {
        super( geometry );
        this.geometryType = PolyGeometryType.LINEARRING;
        this.jtsGeometry = geometry;
        this.jtsLinearRing = (LinearRing) jtsGeometry;
        this.SRID = geometry.getSRID();
    }


    protected PolyLinearRing( Geometry geometry, int srid ) {
        super( geometry );
        this.geometryType = PolyGeometryType.LINEARRING;
        this.jtsGeometry = geometry;
        this.jtsLinearRing = (LinearRing) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyLinearRing( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.LINEARRING;
    }


    public static PolyLinearRing of( Geometry geometry ) {
        return new PolyLinearRing( geometry );
    }


    public static PolyLinearRing of( Geometry geometry, int srid ) {
        return new PolyLinearRing( geometry, srid );
    }

}
