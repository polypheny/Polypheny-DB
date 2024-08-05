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
import org.locationtech.jts.geom.MultiPolygon;
import org.polypheny.db.type.PolyType;

/**
 * {@link PolyMultiPolygon} is a collection of valid {@link PolyPolygon}s: non-overlapping and non-adjacent.
 * {@link PolyPolygon}s in the {@link PolyMultiPolygon} may touch only at a finite number of points.
 * The reason is -  in case of overlapping {@link PolyPolygon}s,
 * it is possible then to represent overlapping {@link PolyPolygon}s as one bigger {@link PolyPolygon} with an increased area.
 * {@link PolyMultiPolygon} does not introduce any new methods.
 */
public class PolyMultiPolygon extends PolyGeometryCollection {

    private MultiPolygon jtsMultiPolygon;


    public PolyMultiPolygon( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.MULTIPOLYGON;
        this.jtsMultiPolygon = (MultiPolygon) jtsGeometry;
    }


    public PolyMultiPolygon( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int srid ) throws InvalidGeometryException {
        super( wkt, srid );
        this.geometryType = PolyGeometryType.MULTIPOLYGON;
        this.jtsMultiPolygon = (MultiPolygon) jtsGeometry;
    }


    public PolyMultiPolygon( Geometry geometry ) {
        super( geometry );
        this.geometryType = PolyGeometryType.MULTIPOLYGON;
        this.jtsGeometry = geometry;
        this.jtsMultiPolygon = (MultiPolygon) jtsGeometry;
        this.SRID = jtsGeometry.getSRID();
    }


    public PolyMultiPolygon( Geometry geometry, int srid ) {
        super( geometry );
        this.geometryType = PolyGeometryType.MULTIPOLYGON;
        this.jtsGeometry = geometry;
        this.jtsMultiPolygon = (MultiPolygon) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyMultiPolygon( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.MULTIPOLYGON;
    }


    public static PolyMultiPolygon of( Geometry geometry ) {
        return new PolyMultiPolygon( geometry );
    }


    public static PolyMultiPolygon of( Geometry geometry, int srid ) {
        return new PolyMultiPolygon( geometry, srid );
    }

}
