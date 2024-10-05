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
import org.locationtech.jts.geom.MultiLineString;
import org.polypheny.db.type.PolyType;


/**
 * {@link PolyMultiLineString} is a collection of any valid {@link PolyLineString}.
 * {@link PolyLineString}s may overlap.
 */
public class PolyMultiLineString extends PolyGeometryCollection {

    private MultiLineString jtMultiLineString;


    public PolyMultiLineString( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.MULTILINESTRING;
        this.jtMultiLineString = (MultiLineString) jtsGeometry;
    }


    public PolyMultiLineString( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int srid ) throws InvalidGeometryException {
        super( wkt, srid );
        this.geometryType = PolyGeometryType.MULTILINESTRING;
        this.jtMultiLineString = (MultiLineString) jtsGeometry;
    }


    public PolyMultiLineString( Geometry geometry ) {
        super( geometry );
        this.geometryType = PolyGeometryType.MULTILINESTRING;
        this.jtsGeometry = geometry;
        this.jtMultiLineString = (MultiLineString) jtsGeometry;
        this.SRID = jtsGeometry.getSRID();
    }


    public PolyMultiLineString( Geometry geometry, int srid ) {
        super( geometry );
        this.geometryType = PolyGeometryType.MULTILINESTRING;
        this.jtsGeometry = geometry;
        this.jtMultiLineString = (MultiLineString) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyMultiLineString( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.MULTILINESTRING;
    }


    public static PolyMultiLineString of( Geometry geometry ) {
        return new PolyMultiLineString( geometry );
    }


    public static PolyMultiLineString of( Geometry geometry, int srid ) {
        return new PolyMultiLineString( geometry, srid );
    }


    /**
     * Test whether {@link PolyMultiLineString} is closed: line starts and ends at the same point.
     *
     * @return <code>true</code> if {@link PolyMultiLineString} is closed.
     */
    public boolean isClosed() {
        return jtMultiLineString.isClosed();
    }

}
