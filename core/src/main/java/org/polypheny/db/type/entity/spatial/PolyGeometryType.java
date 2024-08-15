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

/**
 * Geometry types, with the names and codes assigned by OGC.
 */
public enum PolyGeometryType {
    GEOMETRY( 0 ),
    POINT( 1 ),
    LINESTRING( 2 ),
    // not actually a full type
    LINEARRING( 2 ),
    POLYGON( 3 ),
    MULTIPOINT( 4 ),
    MULTILINESTRING( 5 ),
    MULTIPOLYGON( 6 ),
    GEOMETRYCOLLECTION( 7 ),
    CURVE( 13 ),
    SURFACE( 14 ),
    POLYHEDRALSURFACE( 15 );

    final int code;


    PolyGeometryType( int code ) {
        this.code = code;
    }


    /**
     * How the "buffer" command terminates the end of a line.
     */
    public enum BufferCapStyle {
        ROUND( 1 ), FLAT( 2 ), SQUARE( 3 );

        final int code;


        BufferCapStyle( int code ) {
            this.code = code;
        }


        public static BufferCapStyle of( String value ) {
            switch ( value ) {
                case "round":
                    return ROUND;
                case "flat":
                    return FLAT;
                case "square":
                    return SQUARE;
                default:
                    throw new IllegalArgumentException( "unknown endcap value: " + value );
            }
        }
    }
}
