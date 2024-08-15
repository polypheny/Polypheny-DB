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
import org.locationtech.jts.geom.LineString;
import org.polypheny.db.type.PolyType;

/**
 * Represent a connected sequence of points (line segments) in the space.
 * The line segments may intersect other segments.
 * If line segments do not intersect each other, the line is simple
 * The line may start and end at the same point. In this case, it is called closed.
 * <p>
 * The {@link PolyLineString} is valid if it has
 * either <strong>0</strong> or <strong>2</strong> or <strong>more</strong> points.
 * The {@link PolyLineString} could store up to 4 dimensions.
 * {@link PolyLineString} is a base class for various types of lines.
 */
public class PolyLineString extends PolyGeometry {

    private LineString jtsLineString;


    public PolyLineString( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        super( wkt );
        this.geometryType = PolyGeometryType.LINESTRING;
        this.jtsLineString = (LineString) jtsGeometry;
    }


    public PolyLineString( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int SRID ) throws InvalidGeometryException {
        super( wkt, SRID );
        this.geometryType = PolyGeometryType.LINESTRING;
        this.jtsLineString = (LineString) jtsGeometry;
    }


    protected PolyLineString( Geometry geometry ) {
        super( PolyType.GEOMETRY );
        this.geometryType = PolyGeometryType.LINESTRING;
        this.jtsGeometry = geometry;
        this.jtsLineString = (LineString) jtsGeometry;
        this.SRID = geometry.getSRID();
    }


    protected PolyLineString( Geometry geometry, int srid ) {
        super( PolyType.GEOMETRY );
        this.geometryType = PolyGeometryType.LINESTRING;
        this.jtsGeometry = geometry;
        this.jtsLineString = (LineString) jtsGeometry;
        this.SRID = srid;
    }


    protected PolyLineString( PolyType type ) {
        super( type );
        this.geometryType = PolyGeometryType.LINESTRING;
    }


    public static PolyLineString of( Geometry geometry ) {
        return new PolyLineString( geometry );
    }


    public static PolyLineString of( Geometry geometry, int srid ) {
        return new PolyLineString( geometry, srid );
    }


    /**
     * Test whether {@link PolyLineString} is closed: line starts and ends at the same point.
     *
     * @return <code>true</code> if {@link PolyLineString} is closed.
     */
    public boolean isClosed() {
        return jtsLineString.isClosed();
    }


    /**
     * Test whether {@link PolyLineString} is a ring: simple and closed at the same time.
     *
     * @return <code>true</code> if {@link PolyLineString} is ring.
     */
    public boolean isRing() {
        return jtsLineString.isRing();
    }


    /**
     * Test whether {@link PolyPoint} is a vertex of this {@link PolyLineString}.
     *
     * @param point coordinate to test
     * @return <code>true</code> if {@link PolyPoint} is part of this {@link PolyLineString}.
     */
    public boolean isCoordinate( PolyPoint point ) {
        return jtsLineString.isCoordinate( point.jtsGeometry.getCoordinate() );
    }


    /**
     * Get the nth point in the sequence.
     *
     * @param n number of the point in the sequence
     * @return the nth point in the sequence.
     */
    public PolyPoint getPoint( int n ) {
        return PolyPoint.of( jtsLineString.getPointN( n ), getSRID() );
    }


    /**
     * @return the first (start) point in the sequence
     */
    public PolyPoint getStartPoint() {
        return PolyPoint.of( jtsLineString.getStartPoint(), getSRID() );
    }


    /**
     * @return the last (end) point in the sequence
     */
    public PolyPoint getEndPoint() {
        return PolyPoint.of( jtsLineString.getEndPoint(), getSRID() );
    }

}
