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
import org.locationtech.jts.geom.MultiPoint;
import org.polypheny.db.type.PolyType;


/**
 * A collection of valid {@link PolyPoint}.
 * {@link PolyMultiPoint} does not introduce any validity rules for the collection of {@link PolyPoint}:
 * {@link PolyPoint}s may overlap.
 * {@link PolyMultiPoint} does not introduce any new methods.
 */
public class PolyMultiPoint extends PolyGeometryCollection {

    private MultiPoint jtsMultiPoint;


    public PolyMultiPoint( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.MULTIPOINT;
        this.jtsMultiPoint = (MultiPoint) jtsGeometry;
    }


    public PolyMultiPoint( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int srid ) throws InvalidGeometryException {
        super( wkt, srid );
        this.geometryType = PolyGeometryType.MULTIPOINT;
        this.jtsMultiPoint = (MultiPoint) jtsGeometry;
    }


    public PolyMultiPoint( Geometry geometry ) {
        super( geometry );
        this.geometryType = PolyGeometryType.MULTIPOINT;
        this.jtsGeometry = geometry;
        this.jtsMultiPoint = (MultiPoint) jtsGeometry;
        this.SRID = geometry.getSRID();
    }


    public PolyMultiPoint( Geometry geometry, int srid ) {
        super( geometry );
        this.geometryType = PolyGeometryType.MULTIPOINT;
        this.jtsGeometry = geometry;
        this.jtsMultiPoint = (MultiPoint) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyMultiPoint( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.MULTIPOINT;
    }


    public static PolyMultiPoint of( Geometry geometry ) {
        return new PolyMultiPoint( geometry );
    }


    public static PolyMultiPoint of( Geometry geometry, int srid ) {
        return new PolyMultiPoint( geometry, srid );
    }

}
