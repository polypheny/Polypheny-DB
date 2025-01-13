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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.webui.models.results.GraphResult;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class CypherGeoFunctionsTest extends CypherTestTemplate {

    final static String neo4jAdapterName = "neo4j";
    final static String neo4jDatabaseName = "neo4j_database";

    @BeforeAll
    public static void init() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( "ALTER ADAPTERS ADD \"" + neo4jAdapterName + "\" USING 'Neo4j' AS 'Store'"
                        + " WITH '{mode:docker,instanceId:\"0\"}'" );
            }
        } catch ( SQLException e ) {
            // If there is an error while adding the adapter, the most likely reason it does not work
            // is that docker is not running!
            throw new RuntimeException( e );
        }
    }

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void createPointTest() {
//        tearDown();
//        createGraph(neo4jDatabaseName, neo4jAdapterName);
        tearDown();
        createGraph();

        // TODO: Why are all my commands executed on Neo4j, only because I added the adapter?
        execute( format( "USE GRAPH %s", GRAPH_NAME ) );
        execute( "CREATE (bob:User)", GRAPH_NAME );
//        execute( "CREATE (bob:User)", neo4jDatabaseName );
        GraphResult res = execute( "MATCH (n) RETURN point({longitude: 56.7, latitude: 12}) AS point", GRAPH_NAME );
        PolyGeometry geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == PolyGeometry.WGS_84;
        assert geometry.asPoint().getX() == 56.7;
        assert geometry.asPoint().getY() == 12.0;

        res = execute( "MATCH (n) RETURN point({x: 15, y: 5}) AS point", neo4jDatabaseName );
        geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == 0;
        assert geometry.asPoint().getX() == 15.0;
        assert geometry.asPoint().getY() == 5.0;

        res = execute( "MATCH (n) RETURN point({x: 1, y: 2, z: 3}) AS point", neo4jDatabaseName );
        geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == 0;
        assert geometry.asPoint().getX() == 1.0;
        assert geometry.asPoint().getY() == 2.0;
        assert geometry.asPoint().getZ() == 3.0;

        res = execute( "MATCH (n) RETURN point({longitude: 55.5, latitude: 12.2, height: 100}) AS point", neo4jDatabaseName );
        geometry = convertJsonToPolyGeometry( res.data[0][0] );
        assert geometry.getSRID() == PolyGeometry.WGS_84_3D;
        assert geometry.asPoint().getX() == 55.5;
        assert geometry.asPoint().getY() == 12.2;
        assert geometry.asPoint().getZ() == 100.0;
    }

    @Test
    public void createNodeWithPointTest(){
        execute( "CREATE (z:Station {name: 'Zürich', location: point({latitude: 47.3769, longitude: 8.5417})})" );
        // Node should have the following properties (according to Neo4j)
        //"properties": {
        //		  "name": "Zürich",
        //		  "location": {
        //			"srid": {
        //			  "low": 4326,
        //			  "high": 0
        //			},
        //			"x": 8.5417,
        //			"y": 47.3769
        //		  }
        //		},
        GraphResult res = execute( "MATCH (n) RETURN n;" );
        // TODO: Validate object properties as well...
        assert res.data.length == 1;
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
        // Compute distance in spherical coordinate system (2 dimensions)
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

        // Compute distance in spherical coordinate system (3 dimensions)
        execute( """
                CREATE (basel:City {name: 'Basel', latitude: 47.5595, longitude: 7.5885}),
                       (zurich:City {name: 'Zürich', latitude: 47.3770, longitude: 8.5416});
                """ );
        res = execute( """
                MATCH (basel:City {name: 'Basel'}), (zurich:City {name: 'Zürich'})
                WITH basel, zurich,
                     point({latitude: basel.latitude, longitude: basel.longitude, height: 100}) AS point1,
                     point({latitude: zurich.latitude, longitude: zurich.longitude, height: 200}) AS point2
                RETURN basel.name, zurich.name, point.distance(point1, point2) AS distance;
                """ );
        assert res.data[0].length == 3;
        assert Math.abs( PolyValue.fromJson( res.data[0][2] ).asDocument().get( new PolyString( "value" ) ).asDouble().doubleValue() - 74462.13313143898 ) < 1e-9;

        // Compute distance in euclidean coordinate system (2 dimensions)
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
        assert Math.abs( PolyValue.fromJson( res.data[0][0] ).asDocument().get( new PolyString( "value" ) ).asDouble().doubleValue() - Math.sqrt( 2 ) ) < 1e-9;

        // Compute distance in euclidean coordinate system (3 dimensions)
        execute( """
                CREATE (a:Dot3D {x: 1, y: 1, z:1}),
                       (b:Dot3D {x: 2, y: 2, z:2}),
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
        assert Math.abs( PolyValue.fromJson( res.data[0][0] ).asDocument().get( new PolyString( "value" ) ).asDouble().doubleValue() - 1.7320508075688772 ) < 1e-9;
    }


    @Test
    public void withinBBoxTest() {
        // 2D, planar geometry
        execute( """
                CREATE (a:Dot {x: 1, y: 1, name: 'on edge'}),
                       (b:Dot {x: 1.5, y: 1.5, name: 'inside'}),
                       (c:Dot {x: 3, y: 3, name: 'outside'});
                """ );
        GraphResult res = execute( """
                MATCH (d:Dot {name: 'inside'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        assert res.data[0][0].contains( "\"value\":true" );

        res = execute( """
                MATCH (d:Dot {name: 'outside'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        assert res.data[0][0].contains( "\"value\":false" );

        res = execute( """
                MATCH (d:Dot {name: 'on edge'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        assert res.data[0][0].contains( "\"value\":true" );

        // 2D, spherical geometry
        execute( """
                CREATE (paris:Coordinate {longitude: 2.346956837011285, latitude: 48.85505503368006, name: 'Paris'}),
                       (brussels:Coordinate {longitude: 4.352467876598982, latitude: 50.846742342693915, name: 'Brussels'});
                """ );
        res = execute( """
                MATCH (c:Coordinate {name: 'Paris'})
                WITH point({longitude: c.longitude, latitude: c.latitude}) AS cPoint, c
                RETURN point.withinBBox(
                    cPoint,
                    point({longitude: 1.9987169362536548, latitude: 48.567460188915405}),
                    point({longitude: 4.6322913692799705, latitude: 50.68567402837961}))
                AS result, c.name
                """ );
        assert res.data[0][0].contains( "\"value\":true" );

        res = execute( """
                MATCH (c:Coordinate {name: 'Brussels'})
                WITH point({longitude: c.longitude, latitude: c.latitude}) AS cPoint, c
                RETURN point.withinBBox(
                    cPoint,
                    point({longitude: 1.9987169362536548, latitude: 48.567460188915405}),
                    point({longitude: 4.6322913692799705, latitude: 50.68567402837961}))
                AS result, c.name
                """ );
        assert res.data[0][0].contains( "\"value\":false" );

        // 3D, planar geometry
        // TODO

        // 3D, spherical geometry
        // TODO

        // Attempting to use POINT values with different Coordinate Reference Systems (such as WGS 84 2D and WGS 84 3D) will return null.
        res = execute( """
                MATCH (d:Dot {name: 'on edge'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2, z: 1})) AS result, d.name
                """ );
        assert res.data[0][0] == null;
        
        res = execute( """
                MATCH (c:Coordinate {name: 'Paris'})
                WITH point({longitude: c.longitude, latitude: c.latitude, height: 100}) AS cPoint, c
                RETURN point.withinBBox(
                    cPoint,
                    point({longitude: 4.6322913692799705, latitude: 50.68567402837961}),
                    point({longitude: 1.9987169362536548, latitude: 48.567460188915405}))
                AS result, c.name
                """ );
        assert res.data[0][0] == null;
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

    private List<List<JsonNode>> convertResultsToJsonList(String[][] data){
        List<List<JsonNode>> results = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (String[] d : data){
            List<JsonNode> nodes = new ArrayList<>();
            for(String result : d){
                try {
                    JsonNode jsonNode = objectMapper.readTree( result );
                    nodes.add( jsonNode );
                } catch ( JsonProcessingException e ) {
                    throw new RuntimeException( e );
                }
            }
            results.add( nodes );
        }
        return results;
    }

}
