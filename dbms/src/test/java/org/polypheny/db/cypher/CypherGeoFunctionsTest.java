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

package org.polypheny.db.cypher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.webui.models.results.GraphResult;

public class CypherGeoFunctionsTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void createPointTest() {
        execute( "CREATE (bob:User)" );
        GraphResult res = execute( "MATCH (n) RETURN point({longitude: 56.7, latitude: 12}) AS point" );
        PolyGeometry geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == PolyGeometry.WGS_84;
        assert geometry.asPoint().getX() == 56.7;
        assert geometry.asPoint().getY() == 12.0;

        res = execute( "MATCH (n) RETURN point({x: 15, y: 5}) AS point" );
        geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == 0;
        assert geometry.asPoint().getX() == 15.0;
        assert geometry.asPoint().getY() == 5.0;

        res = execute( "MATCH (n) RETURN point({x: 1, y: 2, z: 3}) AS point" );
        geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == 0;
        assert geometry.asPoint().getX() == 1.0;
        assert geometry.asPoint().getY() == 2.0;
        // TODO: fix this
        // JTS Geometry WKT Representation is hardcoded to 2 dimensions
        // https://github.com/locationtech/jts/blob/6a9af07059671bdc6e62542c9739137ab53fd4d8/modules/core/src/main/java/org/locationtech/jts/io/WKTWriter.java#L139
        assert geometry.asPoint().getZ() == 3.0;

        res = execute( "MATCH (n) RETURN point({longitude: 55.5, latitude: 12.2, height: 100}) AS point" );
        geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == PolyGeometry.WGS_84_3D;
        assert geometry.asPoint().getX() == 55.5;
        assert geometry.asPoint().getY() == 12.2;
        // TODO: fix this
        assert geometry.asPoint().getZ() == 100.0;
    }


    @Test
    public void createPointFromNodeFields() {
        execute( "CREATE (c:Coordinate { lon: 56.7, lat: 12 })" );
        GraphResult res = execute( "MATCH (c:Coordinate) RETURN point({longitude: c.lon, latitude: c.lat}) AS point" );
        PolyGeometry geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == PolyGeometry.WGS_84;
        assert geometry.asPoint().getX() == 56.7;
        assert geometry.asPoint().getY() == 12.0;
    }


    @Test
    public void distanceTest() {
        // Compute distance in spherical coordinate system
        execute( """
                CREATE (basel:City {name: 'Basel', latitude: 47.5595, longitude: 7.5885}),
                       (zurich:City {name: 'Zürich', latitude: 47.3770, longitude: 8.5416});
                """ );
        GraphResult res = execute( """
                MATCH (basel:City {name: 'Basel'}), (zurich:City {name: 'Zürich'})
                WITH basel, zurich,
                     point({latitude: basel.latitude, longitude: basel.longitude}) AS point1,
                     point({latitude: zurich.latitude, longitude: zurich.longitude}) AS point2
                RETURN basel.name, zurich.name, point.distance(point1, point2) AS distance;
                """ );
        assert res.data[0].length == 3;
        assert Math.abs( PolyValue.fromJson( res.data[0][2] ).asDocument().get( new PolyString( "value" ) ).asDouble().doubleValue() - 74460.31287583392 ) < 1e-9;

        // Compute distance in euclidean coordinate system
        execute( """
                CREATE (a:Dot {x: 1, y: 1}),
                       (b:Dot {x: 2, y: 2}),
                       (a)-[:CONNECTED]->(b);
                """ );
        res = execute( """
                MATCH (a:Dot)-[:CONNECTED]->(b:Dot)
                WITH a, b,
                     point({x: a.x, y: a.y}) AS d1,
                     point({x: b.x, y: b.y}) AS d2
                RETURN point.distance(d1, d2) AS distance;
                """ );
        assert res.data[0].length == 1;
        assert Math.abs( PolyValue.fromJson( res.data[0][0] ).asDocument().get( new PolyString( "value" ) ).asDouble().doubleValue() - Math.sqrt( 2 )) < 1e-9;

        // Compute distance in euclidean coordinate system with 3 coordinates
        execute( """
                CREATE (a:Dot3D {x: 1, y: 1, z:1}),
                       (b:Dot3D {x: 2, y: 2, z:3}),
                       (a)-[:CONNECTED]->(b);
                """ );
        res = execute( """
                MATCH (a:Dot3D)-[:CONNECTED]->(b:Dot3D)
                WITH a, b,
                     point({x: a.x, y: a.y, z: a.z}) AS d1,
                     point({x: b.x, y: b.y, z: b.z}) AS d2
                RETURN point.distance(d1, d2) AS distance;
                """ );
        assert res.data[0].length == 1;
        assert Math.abs( PolyValue.fromJson( res.data[0][0] ).asDocument().get( new PolyString( "value" ) ).asDouble().doubleValue() - Math.sqrt( 2 )) < 1e-9;
    }


    private PolyGeometry convertJsonToPolyGeometry( String json ) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree( json );
            String wkt = jsonNode.get( "wkt" ).asText();
            return PolyGeometry.of( wkt );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }

}
