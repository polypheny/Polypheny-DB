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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyGeometry.GeometryInputFormat;
import org.polypheny.db.webui.models.results.GraphResult;

@SuppressWarnings("SqlNoDataSourceInspection")
@Tag("adapter")
@Tag("docker")
public class CypherGeoFunctionsTest extends CypherTestTemplate {

    final static String neo4jAdapterName = "neo4j";
    final static String neo4jDatabaseName = "neo4j_database";


    @BeforeAll
    public static void init() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( """
                        ALTER ADAPTERS ADD "%s" USING 'Neo4j' AS 'Store' WITH '{mode:docker,instanceId:"%d"}'""".formatted( neo4jAdapterName, 0 ) );
            }
        } catch ( SQLException e ) {
            // If there is an error while adding the adapter, the most likely reason it does not work
            // is that docker is not running!
            throw new RuntimeException( e );
        }
    }


    @AfterAll
    public static void close() {
        tearDown();
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {

            if ( Catalog.getInstance().getAdapters().values().stream().noneMatch( a -> a.uniqueName.equals( neo4jAdapterName ) ) ) {
                System.out.println( "Already shutting down neo4j adapter" );
                return;
            }

            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( "ALTER ADAPTERS DROP \"" + neo4jAdapterName + "\"" );
            }
            AdapterManager adapterManager = AdapterManager.getInstance();
            if ( adapterManager.getAdapters().containsKey( neo4jAdapterName ) ) {
                fail();
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


    /**
     * Measures percentages change, relative to the larger number.
     */
    private static boolean isWithinPercentageChange( double a, double b, double percentage ) {
        double diff = Math.abs( a - b );
        double maxAllowedDiff = (percentage / 100) * Math.max( a, b );
        return diff <= maxAllowedDiff;
    }


    private List<GraphResult> runQueries( List<String> queries ) {
        return runQueries( queries, true, true );
    }


    private List<GraphResult> runQueries( List<String> queries, boolean onHsqlDb, boolean onNeo4j ) {
        try {
            List<GraphResult> results = new ArrayList<>();
            GraphResult finalResult = null;

            // 1. Run queries internally
            if ( onHsqlDb ) {
                tearDown();
                createGraph();
                for ( String query : queries ) {
                    finalResult = execute( query, GRAPH_NAME );
                }
                results.add( finalResult );
            }

            // 2. Run queries in Docker
            if ( onNeo4j ) {
                deleteData( neo4jDatabaseName );
                createGraph( neo4jDatabaseName, neo4jAdapterName );
                for ( String query : queries ) {
                    finalResult = execute( query, neo4jDatabaseName );
                }
                results.add( finalResult );
            }

            if ( onHsqlDb && onNeo4j ) {
                assertEquals( 2, results.size() );
            }
            return results;
        } finally {
            deleteData( neo4jDatabaseName );
        }
    }


    private Map<String, Object> assertResultsAreEqual( List<GraphResult> results ) {
        return assertResultsAreEqual( results.get( 0 ), results.get( 1 ) );
    }


    private Map<String, Object> assertResultsAreEqual( GraphResult hsqlResult, GraphResult neo4jResult ) {
        Map<String, Object> hsqlJson = Map.of();
        assertEquals( hsqlResult.data.length, neo4jResult.data.length );

        for ( int i = 0; i < neo4jResult.data.length; i++ ) {
            hsqlJson = convertResultToMap( hsqlResult ).get( 0 );
            Map<String, Object> neo4jJson = convertResultToMap( neo4jResult ).get( 0 );
            assertEquals( neo4jJson.keySet(), hsqlJson.keySet() );

            for ( Entry<String, Object> entry : hsqlJson.entrySet() ) {
                String key = entry.getKey();
                Object value = hsqlJson.get( key );
                Object neo4jValue = neo4jJson.get( key );
                assertEquals( neo4jValue, value );
            }
        }

        return hsqlJson;
    }


    private List<Map<String, Object>> convertResultToMap( GraphResult result ) {
        List<Map<String, Object>> results = new ArrayList<>();

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            for ( int i = 0; i < result.data.length; i++ ) {
                Map<String, Object> map = objectMapper.readValue( result.data[i][0], new TypeReference<>() {
                } );
                results.add( map );
            }
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }

        return results;
    }


    private Object extractValueAtPath( Map<String, Object> map, List<String> path ) {
        Object currentMap = map;
        for ( String key : path ) {
            if ( currentMap instanceof Map<?, ?> m ) {
                currentMap = m.get( key );
            } else if ( currentMap instanceof ArrayList<?> listOfLists ) {
                for ( Object o : listOfLists ) {
                    if ( o instanceof ArrayList<?> keyValueList ) {
                        // 0 -> Key
                        // 1 -> Value
                        if ( keyValueList.get( 0 ).equals( key ) ) {
                            currentMap = keyValueList.get( 1 );
                            break;
                        }
                    }

                }
            }
        }
        return currentMap;
    }


    @Test
    public void distanceNeo4jTest() {
        List<String> queries = new ArrayList<>();
        queries.add( "CREATE (berlin:Location {name: \"Berlin\", lat: 52.5200, lon: 13.4050})" );
        queries.add( "CREATE (paris:Location {name: \"Paris\", lat: 48.8566, lon: 2.3522})" );

        queries.add( """
                MATCH (a:Location {name: "Berlin"}), (b:Location {name: "Paris"})
                WITH
                    point({latitude: a.lat, longitude: a.lon}) AS pointBerlin,
                    point({latitude: b.lat, longitude: b.lon}) AS pointParis
                RETURN point.distance(pointBerlin, pointParis, 'neo4j') AS distance_meters;
                """ );
        List<GraphResult> results = runQueries( queries );
        Map<String, Object> hsqldbResult = convertResultToMap( results.get( 0 ) ).get( 0 );
        Map<String, Object> neo4jResult = convertResultToMap( results.get( 1 ) ).get( 0 );

        // Validate that the difference change between both numbers is smaller than a threshold, e.g. 0,2% (
        assert isWithinPercentageChange( ((Number) hsqldbResult.get( "value" )).doubleValue(), (Integer) neo4jResult.get( "value" ), 0.2 );
    }


    @Test
    public void createPointTest() {
        List<String> queries = new ArrayList<>();
        queries.add( "CREATE (bob:User)" );
        queries.add( "MATCH (n) RETURN point({longitude: 56.7, latitude: 12}) AS point" );

        List<GraphResult> results = runQueries( queries );
        Map<String, Object> res = assertResultsAreEqual( results );
        PolyGeometry geometry = PolyGeometry.ofOrThrow( (String) res.get( "wkt" ) );
        assert geometry.getSRID() == PolyGeometry.WGS_84;
        assert geometry.asPoint().getX() == 56.7;
        assert geometry.asPoint().getY() == 12.0;

        queries.remove( 1 );
        queries.add( "MATCH (n) RETURN point({x: 15, y: 5}) AS point" );
        results = runQueries( queries );
        res = assertResultsAreEqual( results );
        geometry = PolyGeometry.ofOrThrow( (String) res.get( "wkt" ) );
        assert geometry.getSRID() == 0;
        assert geometry.asPoint().getX() == 15.0;
        assert geometry.asPoint().getY() == 5.0;

        queries.remove( 1 );
        queries.add( "MATCH (n) RETURN point({x: 1, y: 2, z: 3}) AS point" );
        results = runQueries( queries );
        res = assertResultsAreEqual( results );
        geometry = PolyGeometry.ofOrThrow( (String) res.get( "wkt" ) );
        assert geometry.getSRID() == 0;
        assert geometry.asPoint().getX() == 1.0;
        assert geometry.asPoint().getY() == 2.0;
        assert geometry.asPoint().getZ() == 3.0;

        queries.remove( 1 );
        queries.add( "MATCH (n) RETURN point({longitude: 55.5, latitude: 12.2, height: 100}) AS point" );
        results = runQueries( queries );
        res = assertResultsAreEqual( results );
        geometry = PolyGeometry.ofOrThrow( (String) res.get( "wkt" ) );
        assert geometry.getSRID() == PolyGeometry.WGS_84_3D;
        assert geometry.asPoint().getX() == 55.5;
        assert geometry.asPoint().getY() == 12.2;
        assert geometry.asPoint().getZ() == 100.0;
    }


    @Test
    @Tag("fileExcluded")
    public void createNodeWithPointTest() throws InvalidGeometryException {
        List<GraphResult> results = runQueries( List.of(
                "CREATE (z:Station {name: 'Zürich', location: point({latitude: 47.3769, longitude: 8.5417})})",
                "MATCH (n) RETURN n;"
        ) );
        assert results.size() == 2;
        String name = "value";

        boolean usesNeo4j = PolyphenyDb.defaultStoreName.equals( "neo4j" );

        if ( usesNeo4j ) {
            name = "wkt";
        }

        Object hsqlValue = extractValueAtPath( convertResultToMap( results.get( 0 ) ).get( 0 ), List.of( "properties", "_ps", "location", name ) );
        Object neo4jValue = extractValueAtPath( convertResultToMap( results.get( 1 ) ).get( 0 ), List.of( "properties", "_ps", "location", "wkt" ) );
        PolyGeometry neo4jGeometry = PolyGeometry.of( neo4jValue.toString() );
        PolyGeometry hsqlGeometry = usesNeo4j ? PolyGeometry.of( hsqlValue.toString() ) : new PolyGeometry( hsqlValue.toString(), 4326, GeometryInputFormat.GEO_JSON );
        assertEquals( neo4jGeometry, hsqlGeometry );

        results = runQueries( List.of(
                "CREATE (z:Station {name: 'Zürich', location: point({x: 15, y: 30})})",
                "MATCH (n) RETURN n;"
        ) );
        assert results.size() == 2;
        hsqlValue = extractValueAtPath( convertResultToMap( results.get( 0 ) ).get( 0 ), List.of( "properties", "_ps", "location", name ) );
        neo4jValue = extractValueAtPath( convertResultToMap( results.get( 1 ) ).get( 0 ), List.of( "properties", "_ps", "location", "wkt" ) );
        neo4jGeometry = PolyGeometry.of( neo4jValue.toString() );
        hsqlGeometry = usesNeo4j ? PolyGeometry.of( hsqlValue.toString() ) : new PolyGeometry( hsqlValue.toString(), 0, GeometryInputFormat.GEO_JSON );
        assertEquals( neo4jGeometry, hsqlGeometry );
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
        List<String> queries = new ArrayList<>();

        queries.add( """
                CREATE (a:Dot {x: 1, y: 1, name: 'on edge'}),
                       (b:Dot {x: 1.5, y: 1.5, name: 'inside'}),
                       (c:Dot {x: 3, y: 3, name: 'outside'});
                """ );
        queries.add( """
                MATCH (d:Dot {name: 'inside'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        List<GraphResult> results = runQueries( queries );
        assert convertResultToMap( results.get( 0 ) ).get( 0 ).get( "value" ).equals( true );
        assert convertResultToMap( results.get( 1 ) ).get( 0 ).get( "value" ).equals( true );

        queries.remove( 1 );
        queries.add( """
                MATCH (d:Dot {name: 'outside'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        results = runQueries( queries );
        assert convertResultToMap( results.get( 0 ) ).get( 0 ).get( "value" ).equals( false );
        assert convertResultToMap( results.get( 1 ) ).get( 0 ).get( "value" ).equals( false );

        queries.remove( 1 );
        queries.add( """
                MATCH (d:Dot {name: 'on edge'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinBBox(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        results = runQueries( queries );
        assert convertResultToMap( results.get( 0 ) ).get( 0 ).get( "value" ).equals( true );
        assert convertResultToMap( results.get( 1 ) ).get( 0 ).get( "value" ).equals( true );
    }


    /**
     * The function call point.withinGeometry used in this test does not exist in Cypher / Neo4j.
     * It is possible to extend Neo4j with additional functions, however, this has not (yet) been done,
     * which is why it will only be executed internally for now.
     */
    @Test
    public void withinGeometryTest() {
        List<String> queries = new ArrayList<>();
        queries.add( """
                CREATE (a:Dot {x: 1, y: 1, name: 'on edge'}),
                       (b:Dot {x: 1.5, y: 1.5, name: 'inside'}),
                       (c:Dot {x: 3, y: 3, name: 'outside'});
                """ );
        queries.add( """
                MATCH (d:Dot {name: 'inside'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinGeometry(dPoint, 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))') AS result, d.name
                """ );
        List<GraphResult> results = runQueries( queries, true, false );
        assert convertResultToMap( results.get( 0 ) ).get( 0 ).get( "value" ).equals( true );

        queries.remove( 1 );
        queries.add( """
                MATCH (d:Dot {name: 'outside'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinGeometry(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        results = runQueries( queries, true, false );
        assert convertResultToMap( results.get( 0 ) ).get( 0 ).get( "value" ).equals( false );

        queries.remove( 1 );
        queries.add( """
                MATCH (d:Dot {name: 'on edge'})
                WITH point({x: d.x, y: d.y}) AS dPoint, d
                RETURN point.withinGeometry(dPoint, point({x: 1, y: 1}), point({x: 2, y: 2})) AS result, d.name
                """ );
        results = runQueries( queries, true, false );
        assert convertResultToMap( results.get( 0 ) ).get( 0 ).get( "value" ).equals( true );
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
