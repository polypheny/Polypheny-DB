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
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

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
     * <p>
     * It is possible to extract useful information from the {@link Geometry}:
     * <code>jtsGeometry.getGeometryType()</code>
     * <code>jtsGeometry.getSRID()</code>
     * <code>jtsGeometry.getCoordinate()</code>
     */
    protected Geometry jtsGeometry;
    // Spatial Reference System ID
    @Getter
    protected Integer SRID;

    /**
     * Constructor creates the {@link Geometry} from the WKT text.
     * <p>
     * The WKT may contain SRID in the text, e.g. <code>"SRID=4326;POINT (13.4050 52.5200)"</code>.
     * In this case SRID from the WKT would be used, otherwise the default one is selected.
     * <p>
     * @param wkt Well Know Text representation of the geometry
     * @throws InvalidGeometryException if {@link Geometry} is invalid or provided WKT is invalid.
     */
    public PolyGeometry( @JsonProperty("wkt") @Deserialize("wkt") String wkt ) throws InvalidGeometryException {
        this( PolyType.GEOMETRY );
        this.SRID = NO_SRID;
        WKTReader reader = new WKTReader();
        try {
            // WKT is actually an extended EWKT with SRID before the WKT
            if (wkt.startsWith( "SRID" )) {
                // in WKT semicolon is invalid character, so we could safely split by it
                String[] ewktParts = wkt.split( ";" );
                this.SRID = Integer.valueOf( ewktParts[0].replace( "SRID=", "" ) );
                wkt = ewktParts[1];
            }
            this.jtsGeometry = reader.read( wkt );
            if (!jtsGeometry.isValid()) {
                throw new ParseException("Provided geometry is not valid.");
            }
            this.jtsGeometry.setSRID( this.SRID );
        } catch ( ParseException | NumberFormatException e) {
            throw new InvalidGeometryException(e.getMessage());
        }
    }

    public PolyGeometry( Geometry geometry ) {
        super( PolyType.GEOMETRY );
        this.jtsGeometry = geometry;
        this.SRID = geometry.getSRID();
    }

    protected PolyGeometry( PolyType type ) {
        super( type );
    }

    public static PolyGeometry of( String wkt ) throws InvalidGeometryException {
        return new PolyGeometry( wkt );
    }

    public static PolyGeometry of( Geometry geometry ) {
        return new PolyGeometry( geometry );
    }

    public String getGeometryType() {
        return jtsGeometry.getGeometryType();
    }

    public PolyPoint asPoint() {
        if (jtsGeometry.getGeometryType().equals( "Point" )) {
            return PolyPoint.of( jtsGeometry );
        }
        return null;
    }


    /**
     * Tests whether this {@link Geometry} is simple.
     * @return <code>true</code> if  {@link Geometry} is simple.
     */
    public boolean isSimple() {
        return jtsGeometry.isSimple();
    }


    /**
     * Bound the {@link Geometry} my the minimum box that could fit this geometry.
     * @return {@link PolyGeometry} with minimum bounding box
     */
    public PolyGeometry getMinimumBoundingBox() {
        return PolyGeometry.of( jtsGeometry.getEnvelope() );
    }


    /**
     * {@link #equals(Object) equals} ensures that the {@link Geometry} types and coordinates are the same.
     * And {@link #SRID} used for representation of coordinates are also identical.
     */
    @Override
    public boolean equals( Object o ) {
        if ( !( o instanceof PolyGeometry )) {
            return false;
        }
        PolyGeometry that = (PolyGeometry) o;
        return jtsGeometry.equals( that.jtsGeometry ) && Objects.equals( SRID, that.SRID );
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
     * Output the {@link Geometry} in a WKT format with its SRID. So-called EWKT
     */
    @Override
    public String toString() {
        return String.format( "SRID=%d;%s" , SRID, jtsGeometry.toString() );
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
                        return PolyGeometry.of( PolySerializable.deserialize( in.readUTF8(), serializer ).asString().value );
                    } catch ( InvalidGeometryException e ) {
                        throw new CorruptedDataException( e.getMessage() );
                    }
                }
            };
        }

    }

}
