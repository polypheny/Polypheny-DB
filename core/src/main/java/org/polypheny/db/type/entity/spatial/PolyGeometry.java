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
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import io.activej.serializer.annotations.Deserialize;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.spatial.PolyGeometryType.BufferCapStyle;

/**
 * {@link PolyGeometry} is an abstraction for all spatial data types.
 * <p>
 * {@link PolyGeometry} wraps the {@link Geometry} of the JTS library
 * and the functionality of {@link PolyGeometry} is provided my means of JTS.
 */
@Slf4j
public class PolyGeometry extends PolyValue {

    // default plane
    private static final int NO_SRID = 0;
    // World Geodetic System 1984
    private static final int WGS_84 = 4326;

    /**
     * Wrap the JTS {@link Geometry} class.
     */
    @Getter
    protected Geometry jtsGeometry;
    // Spatial Reference System ID
    @Getter
    protected Integer SRID;

    @Getter
    protected PolyGeometryType geometryType;


    /**
     * Constructor creates the {@link PolyGeometry} from the WKT text.
     * <p>
     * The WKT may contain SRID in the text, e.g. <code>"SRID=4326;POINT (13.4050 52.5200)"</code>.
     * In this case SRID from the WKT would be used, otherwise the default one is selected.
     * <p>
     *
     * @param wkt Well Know Text representation of the geometry
     * @throws InvalidGeometryException if {@link PolyGeometry} is invalid or provided WKT is invalid.
     */
    public PolyGeometry( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        this( PolyType.GEOMETRY );
        int srid = NO_SRID;
        try {
            wkt = wkt.trim();
            // WKT is actually an extended EWKT with SRID before the WKT
            if ( wkt.startsWith( "SRID" ) ) {
                // in WKT semicolon is invalid character, so we could safely split by it
                String[] ewktParts = wkt.split( ";" );
                srid = Integer.parseInt( ewktParts[0].replace( "SRID=", "" ) );
                wkt = ewktParts[1];
            }
        } catch ( NumberFormatException e ) {
            throw new InvalidGeometryException( e.getMessage() );
        }
        init( wkt, srid );
    }


    /**
     * Constructor creates the {@link PolyGeometry} from the WKT text using the provided SRID.
     *
     * @param wkt Well Know Text representation of the geometry
     * @param srid Spatial reference system of the geometry
     * @throws InvalidGeometryException if {@link PolyGeometry} is invalid or provided WKT is invalid.
     */
    public PolyGeometry( @JsonProperty("wkt") @Deserialize("wkt") String wkt, int srid ) throws InvalidGeometryException {
        this( PolyType.GEOMETRY );
        init( wkt, srid );
    }


    public PolyGeometry( Geometry geometry ) {
        super( PolyType.GEOMETRY );
        this.jtsGeometry = geometry;
        this.SRID = geometry.getSRID();
        this.geometryType = getPolyGeometryType();
    }


    protected PolyGeometry( PolyType type ) {
        super( type );
    }


    public static PolyGeometry of( String wkt ) throws InvalidGeometryException {
        return new PolyGeometry( wkt );
    }


    public static PolyGeometry of( String wkt, int srid ) throws InvalidGeometryException {
        return new PolyGeometry( wkt, srid );
    }


    public static PolyGeometry of( Geometry geometry ) {
        return new PolyGeometry( geometry );
    }


    private void init( String wkt, int srid ) throws InvalidGeometryException {
        this.SRID = srid;
        WKTReader reader = new WKTReader();
        try {
            this.jtsGeometry = reader.read( wkt );
            if ( !jtsGeometry.isValid() ) {
                throw new ParseException( "Provided geometry is not valid." );
            }
            this.jtsGeometry.setSRID( this.SRID );
        } catch ( ParseException | IllegalArgumentException e ) {
            // IllegalArgumentException is thrown in case geometry conditions are not met
            throw new InvalidGeometryException( e.getMessage() );
        }
        this.geometryType = getPolyGeometryType();
    }


    protected PolyGeometryType getPolyGeometryType() {
        switch ( jtsGeometry.getGeometryType() ) {
            case "Point":
                return PolyGeometryType.POINT;
            case "LineString":
                return PolyGeometryType.LINESTRING;
            case "LinearRing":
                return PolyGeometryType.LINEARRING;
            case "Polygon":
                return PolyGeometryType.POLYGON;
            case "GeometryCollection":
                return PolyGeometryType.GEOMETRYCOLLECTION;
            case "MultiPoint":
                return PolyGeometryType.MULTIPOINT;
            case "MultiLineString":
                return PolyGeometryType.MULTILINESTRING;
            case "MultiPolygon":
                return PolyGeometryType.MULTIPOLYGON;
            default:
                throw new NotImplementedException( "value" );
        }
    }


    public boolean isPoint() {
        return geometryType.equals( PolyGeometryType.POINT );
    }


    @NotNull
    public PolyPoint asPoint() {
        if ( isPoint() ) {
            return PolyPoint.of( jtsGeometry );
        }
        throw cannotParse( this, PolyPoint.class );
    }


    public boolean isLineString() {
        return geometryType.equals( PolyGeometryType.LINESTRING );
    }


    @NotNull
    public PolyLineString asLineString() {
        if ( isLineString() ) {
            return PolyLineString.of( jtsGeometry );
        }
        throw cannotParse( this, PolyLineString.class );
    }


    public boolean isLinearRing() {
        return geometryType.equals( PolyGeometryType.LINEARRING );
    }


    @NotNull
    public PolyLinearRing asLinearRing() {
        if ( isLinearRing() ) {
            return PolyLinearRing.of( jtsGeometry );
        }
        throw cannotParse( this, PolyLinearRing.class );
    }


    public boolean isPolygon() {
        return geometryType.equals( PolyGeometryType.POLYGON );
    }


    @NotNull
    public PolyPolygon asPolygon() {
        if ( isPolygon() ) {
            return PolyPolygon.of( jtsGeometry );
        }
        throw cannotParse( this, PolyPolygon.class );
    }


    public boolean isGeometryCollection() {
        return List.of( PolyGeometryType.GEOMETRYCOLLECTION, PolyGeometryType.MULTIPOINT, PolyGeometryType.MULTILINESTRING, PolyGeometryType.MULTIPOLYGON ).contains( geometryType );
    }


    @NotNull
    public PolyGeometryCollection asGeometryCollection() {
        if ( isGeometryCollection() ) {
            return PolyGeometryCollection.of( jtsGeometry );
        }
        throw cannotParse( this, PolyGeometryCollection.class );
    }


    /**
     * Tests whether this {@link PolyGeometry} is a simple geometry: does not intersect itself.
     * May touch its own boundary at any point.
     *
     * @return <code>true</code> if {@link PolyGeometry} is simple.
     */
    public boolean isSimple() {
        return jtsGeometry.isSimple();
    }


    /**
     * @return <code>true</code> if the set of points covered by this {@link PolyGeometry} is empty.
     */
    public boolean isEmpty() {
        return jtsGeometry.isEmpty();
    }


    /**
     * @return the count of this {@link PolyGeometry} vertices.
     */
    public int getNumPoints() {
        return jtsGeometry.getNumPoints();
    }


    /**
     * @return the dimension of this {@link PolyGeometry}.
     */
    public int getDimension() {
        return jtsGeometry.getDimension();
    }


    /**
     * Linear geometries return their length.
     * Areal geometries return their perimeter.
     * The length of a {@link PolyPoint} or {@link PolyMultiPoint} is 0.
     * The length of a {@link PolyLineString} or {@link PolyMultiLineString} is the sum of the lengths of each line segment: distance from the start point to the end point
     * The length of a {@link PolyPolygon} or {@link PolyMultiPolygon} is the sum of the lengths of the exterior boundary and any interior boundaries.
     * The length of any {@link PolyGeometryCollection} is the sum of the lengths of all {@link PolyGeometry} it contains.
     *
     * @return the length of this {@link PolyGeometry}
     */
    public double getLength() {
        return jtsGeometry.getLength();
    }


    /**
     * Areal Geometries have a non-zero area.
     *
     * @return the area of this {@link PolyGeometry}
     */
    public double getArea() {
        return jtsGeometry.getArea();
    }


    /**
     * Bound the {@link PolyGeometry} by the minimum box that could fit this geometry.
     *
     * @return {@link PolyGeometry} with minimum bounding box
     */
    public PolyGeometry getEnvelope() {
        return PolyGeometry.of( jtsGeometry.getEnvelope() );
    }


    /**
     * The boundary of a {@link PolyGeometry} is a set of {@link PolyGeometry}s of the next lower dimension
     * that define the limit of this {@link PolyGeometry}:
     * For {@link PolyLineString} the boundary is start and end {@link PolyPoint}: {@link PolyMultiPoint}.
     * For {@link PolyPolygon} the boundary is an exterior shell {@link PolyLinearRing}.
     *
     * @return the set of the combinatorial boundary {@link PolyGeometry} that define the limit of this {@link PolyGeometry}
     */
    public PolyGeometry getBoundary() {
        return PolyGeometry.of( jtsGeometry.getBoundary() );
    }


    /**
     * @return the dimension of the boundary of this {@link PolyGeometry}.
     */
    public int getBoundaryDimension() {
        return jtsGeometry.getBoundaryDimension();
    }


    /**
     * Calculate the smallest convex {@link PolyGeometry} that contains this {@link PolyGeometry}.
     *
     * @return the minimum area {@link PolyGeometry} containing all points in this {@link PolyGeometry}
     */
    public PolyGeometry convexHull() {
        return PolyGeometry.of( jtsGeometry.convexHull() );
    }


    /**
     * Computes the geometric center - centroid - of this {@link PolyGeometry}.
     * <p>
     * When a geometry contains a combination of {@link PolyPoint}s, {@link PolyLineString}s, and {@link PolyPolygon}s,
     * the centroid calculation is influenced solely by the {@link PolyPolygon}s within the {@link PolyGeometry}.
     * <p>
     * Likewise, in the presence of both {@link PolyPoint}s and {@link PolyLineString}s within a {@link PolyGeometry},
     * the contribution of {@link PolyPoint}s to the centroid calculation is disregarded.
     *
     * @return {@link PolyPoint} that is the centroid of this {@link PolyGeometry}
     */
    public PolyPoint getCentroid() {
        return PolyPoint.of( jtsGeometry.getCentroid() );
    }


    /**
     * Compute a buffer area around this {@link PolyGeometry} within the given distance.
     * The buffer maybe empty.
     * The negative or zero-distance buffer of {@link PolyLineString}s and {@link PolyPoint}s
     * is always an {@link PolyPolygon}.
     *
     * @param distance width of the buffer
     * @return a {@link PolyPolygon} that represent buffer region around this {@link PolyGeometry}
     */
    public PolyGeometry buffer( double distance ) {
        return PolyPoint.of( jtsGeometry.buffer( distance ) );
    }


    /**
     * Compute a buffer area around this {@link PolyGeometry} within the given distance
     * and accuracy of approximation for circular arcs.
     * The buffer maybe empty.
     * The negative or zero-distance buffer of {@link PolyLineString}s and {@link PolyPoint}s
     * is always an {@link PolyPolygon}.
     *
     * @param distance width of the buffer
     * @param quadrantSegments number of line segments to represent a quadrant of a circle
     * @return a {@link PolyPolygon} that represent buffer region around this {@link PolyGeometry}
     */
    public PolyGeometry buffer( double distance, int quadrantSegments ) {
        return PolyPoint.of( jtsGeometry.buffer( distance, quadrantSegments ) );
    }


    /**
     * Compute a buffer area around this {@link PolyGeometry} within the given distance
     * and accuracy of approximation for circular arcs, and use provided buffer end cap style.
     * The buffer maybe empty.
     * The negative or zero-distance buffer of {@link PolyLineString}s and {@link PolyPoint}s
     * is an empty {@link PolyPolygon}.
     *
     * @param distance width of the buffer
     * @param quadrantSegments number of line segments to represent a quadrant of a circle
     * @param endCapStyle specifies how the buffer command terminates the end of a line.
     * @return a {@link PolyPolygon} that represent buffer region around this {@link PolyGeometry}
     */
    public PolyGeometry buffer( double distance, int quadrantSegments, BufferCapStyle endCapStyle ) {
        return PolyPoint.of( jtsGeometry.buffer( distance, quadrantSegments, endCapStyle.code ) );
    }


    /**
     * @return new {@link PolyGeometry} with coordinates in a reverse order.
     */
    public PolyGeometry reverse() {
        return PolyGeometry.of( jtsGeometry.reverse() );
    }


    /**
     * {@link #equals(Object) equals} ensures that the {@link PolyGeometry} types and coordinates are the same.
     * And {@link #SRID} used for representation of coordinates are also identical.
     */
    @Override
    public boolean equals( Object o ) {
        if ( !(o instanceof PolyGeometry) ) {
            return false;
        }
        PolyGeometry that = (PolyGeometry) o;
        return geometryType.equals( that.geometryType ) && jtsGeometry.equals( that.jtsGeometry ) && Objects.equals( SRID, that.SRID );
    }


    @Override
    public int hashCode() {
        return Objects.hash( super.hashCode(), jtsGeometry.hashCode(), SRID );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyGeometry.class );
    }


    @Override
    public Expression asExpression() {
        // TODO: dont know what is it
        return null;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        return jtsGeometry.compareTo( o );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        String wkt = toString();
        return (long) (wkt == null ? 1 : wkt.getBytes().length);
    }


    /**
     * Output the {@link PolyGeometry} in a WKT format with its SRID. So-called EWKT
     */
    @Override
    public String toString() {
        return String.format( "SRID=%d;%s", SRID, jtsGeometry.toString() );
    }


    public static class PolyGeometrySerializerDef extends SimpleSerializerDef<PolyGeometry> {

        @Override
        protected BinarySerializer<PolyGeometry> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyGeometry item ) {
                    out.writeUTF8( item.toString() );
                }


                @Override
                public PolyGeometry decode( BinaryInput in ) throws CorruptedDataException {
                    try {
                        return PolyGeometry.of( in.readUTF8() );
                    } catch ( InvalidGeometryException e ) {
                        throw new CorruptedDataException( e.getMessage() );
                    }
                }
            };
        }

    }

}
