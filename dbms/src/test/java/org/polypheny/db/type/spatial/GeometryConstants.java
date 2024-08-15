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

package org.polypheny.db.type.spatial;

public class GeometryConstants {

    static final String POINT_EWKT = "SRID=4326;POINT (13.4050 52.5200)";
    static final String POINT_TWKB = "e10801a0c3eb7f80aaeff40300"; // in HEX
    static final String POINT_GEO_JSON = "{ \"type\": \"Point\", \"coordinates\": [ 13.4050, 52.5200 ] }";
    static final String POINT_WKT = "POINT (13.4050 52.5200 36.754)";
    static final String LINESTRING_WKT = "LINESTRING (-1 -1, 2 2, 4 5, 6 7)";
    static final String LINEAR_RING_WKT = "LINEARRING (0 0, 0 10, 10 10, 10 0, 0 0)";
    static final String POLYGON_WKT = "POLYGON ( (-1 -1, 2 2, -1 2, -1 -1 ) )";
    static final String GEOMETRYCOLLECTION_WKT = "GEOMETRYCOLLECTION ( POINT (2 3), LINESTRING (2 3, 3 4) )";
    static final String MULTIPOINT_WKT = "MULTIPOINT ( (2 3), (13.4050 52.5200) )";
    static final String MULTILINESTRING_WKT = "MULTILINESTRING ( (0 0, 1 1, 1 2), (2 3, 3 2, 5 4) )";
    static final String MULTIPOLYGON_WKT = "MULTIPOLYGON ( ( (1 5, 5 5, 5 1, 1 1, 1 5) ), ( (6 5, 9 1, 6 1, 6 5) ) )";
    static final double DELTA = 1e-5;
    static final int NO_SRID = 0;
    static final int WSG_84 = 4326;

}
