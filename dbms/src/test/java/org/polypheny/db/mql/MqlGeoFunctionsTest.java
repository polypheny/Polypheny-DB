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

package org.polypheny.db.mql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.webui.models.results.DocResult;

@SuppressWarnings("SqlNoDataSourceInspection")
@Tag("adapter")
@Slf4j
public class MqlGeoFunctionsTest extends MqlTestTemplate {

    final static String mongoAdapterName = "mongo";
    final static String mongoCollection = "mongo";
    final static String defaultCollection = "default";
    final static List<String> collections = List.of( defaultCollection
//            , mongoCollection
    );
    final static Map<String, String> collectionToStore = Map.of(
            mongoCollection, mongoAdapterName,
            defaultCollection, "hsqldb"
    );
    final static String clearCollection = """
            db.%s.deleteMany({})
            """;


    @BeforeAll
    public static void init() {
        addMongoDbAdapter();

        // Create collection and save it to either the internal store or MongoDB.
        // This way, we can compare if the implementations match.
        for ( String collection : collections ) {
            String createCollection = """
                    db.createCollection(%s).store(%s)
                    """.formatted( collection, collectionToStore.get( collection ) );
            execute( createCollection, namespace );
        }
    }


    @BeforeEach
    public void beforeEach() {
        // Make sure collections are emptied before each test.
//        clearCollections();
    }


    @Test
    public void docGeoIntersectsTest() {
        ArrayList<String> queries = new ArrayList<>();
        queries.add(
                """
                        db.%s.insertMany([
                            {
                              name: "Legacy [0,0]",
                              num: 1,
                              legacy: [0,0]
                            },
                            {
                              name: "Legacy [1,1]",
                              num: 2,
                              legacy: [1,1]
                            },
                            {
                              name: "Legacy [2,2]",
                              num: 3,
                              legacy: [2,2]
                            }
                        ])
                        """
        );
        queries.add(
                """
                        db.%s.find({
                            legacy: {
                               $geoIntersects: {
                                  $geometry: {
                                      type: "Polygon",
                                      coordinates: [[ [0,0], [0,1], [1,1], [1,0], [0,0] ]]
                                  }
                               }
                            }
                        })
                        """
        );
        List<DocResult> results = runQueries( queries );
        compareResults( results );
    }


    @Test
    public void docGeoWithinTest() {
        List<DocResult> results;
        String insertMany = """
                db.%s.insertMany([
                    {
                      name: "Legacy [0,0]",
                      num: 1,
                      legacy: [0,0]
                    },
                    {
                      name: "Legacy [1,1]",
                      num: 2,
                      legacy: [1,1]
                    },
                    {
                      name: "Legacy [2,2]",
                      num: 3,
                      legacy: [2,2]
                    }
                ])
                """;
        String geoWithinBox = """
                db.%s.find({
                    legacy: {
                       $geoWithin: {
                          $box: [
                            [0,0],
                            [1,1]
                          ]
                       }
                    }
                })
                """;
        results = runQueries( Arrays.asList( insertMany, geoWithinBox ) );
        compareResults( results );

        String geoWithinGeometry = """
                db.%s.find({
                    legacy: {
                        $geoWithin: {
                            $geometry: {
                                type: "Polygon",
                                coordinates: [[ [0,0], [0,1], [1,1], [1,0], [0,0] ]]
                            }
                        }
                    }
                })
                """;
        results = runQueries( Arrays.asList( clearCollection, insertMany, geoWithinGeometry ) );
        compareResults( results );

        String geoWithinPolygon = """
                db.%s.find({
                    legacy: {
                        $geoWithin: {
                            $polygon: [ [0,0], [0,1], [1,1], [1,0], [0,0] ]
                        }
                    }
                })
                """;
        results = runQueries( Arrays.asList( clearCollection, insertMany, geoWithinPolygon ) );
        compareResults( results );

        String geoWithinCenter = """
                db.%s.find({
                    legacy: {
                        $geoWithin: {
                             $center: [
                                [ 0, 0 ],
                                1.5
                             ]
                        }
                    }
                })
                """;
        results = runQueries( Arrays.asList( clearCollection, insertMany, geoWithinCenter ) );
        compareResults( results );

        String insertCoordinates = """
                db.%s.insertMany([
                    {
                      name: "Kirchgebäude Mittlere Brücke",
                      legacy: [7.5898043, 47.5600440]
                    },
                    {
                      name: "Mitte Rhein Johanniterbrücke",
                      legacy: [7.585512, 47.564843]
                    },
                ])
                """;

        String geoWithinCenterSphere = """
                db.%s.find({
                    legacy: {
                        $geoWithin: {
                             $centerSphere: [
                                [7.5872232, 47.5601937],
                                0.00004
                             ]
                        }
                    }
                })
                """;
        results = runQueries( Arrays.asList( clearCollection, insertCoordinates, geoWithinCenterSphere ) );
        compareResults( results );
    }


    @Test
    public void docGeoNearTest() {
        ArrayList<String> queries = new ArrayList<>();
        queries.add(
                """
                        db.%s.insertMany([
                            {
                              name: "Legacy [2,2]",
                              num: 3,
                              legacy: [2,2]
                            }
                            {
                              name: "Legacy [0,0]",
                              num: 1,
                              legacy: [0,0]
                            },
                            {
                              name: "Legacy [3,3]",
                              num: 4,
                              legacy: [3,3]
                            }
                            {
                              name: "Legacy [1,1]",
                              num: 2,
                              legacy: [1,1]
                            },
                        ])
                        """
        );

//        queries.add(
//                """
//                        db.%s.find({
//                            legacy: {
//                               $near: {
//                                  $geometry: {
//                                      type: "Point",
//                                      coordinates: [0,0]
//                                  },
//                                  $minDistance: 1
//                               }
//                            }
//                        })
//                        """
//        );

//        ,
//        $minDistance: 1,
//        $maxDistance: 100,

        queries.add(
                """
                        db.%s.find({
                            legacy: {
                               $near: [0,0],
                               $maxDistance: 1
                            }
                        })
                        """
        );

        // only filter.
//        queries.add(
//                """
//                        db.%s.find({ "num": { "$lte" : 2 } })
//                        """
//        );

        // Fails somewhere in RexProgramBuilder.mergePrograms
//        DocResult result = execute( """
//                db.%s.find({
//                    legacy: {
//                       $near: {
//                          $geometry: {
//                              type: "Point",
//                              coordinates: [0,0]
//                          },
//                          $minDistance: 1,
//                          $maxDistance: 100,
//                       }
//                    }
//                })
//                """.formatted( defaultCollection ), namespace );

//        queries.add(
//                """
//                        db.%s.find({
//                          num: {
//                            $gte: 1,
//                            $lte: 2
//                          }
//                        })
//                        """
//        );
//        execute( """
//                db.%s.find({
//                  num: {
//                    $gte: 1,
//                    $lte: 2
//                  }
//                })
//                """.formatted( defaultCollection ), namespace );
        execute( """
                db.%s.insertMany([
                    {
                      name: "Legacy [2,2]",
                      num: 3,
                      legacy: [2,2]
                    }
                    {
                      name: "Legacy [0,0]",
                      num: 1,
                      legacy: [0,0]
                    },
                    {
                      name: "Legacy [3,3]",
                      num: 4,
                      legacy: [3,3]
                    }
                    {
                      name: "Legacy [1,1]",
                      num: 2,
                      legacy: [1,1]
                    },
                ])
                """.formatted( defaultCollection ), namespace );
        DocResult result = execute( """
                db.%s.find({
                    legacy: {
                       $near: [0,0],
                       $maxDistance: 10
                    }
                })
                """.formatted( defaultCollection ), namespace );
        System.out.println( "Test" );
        // Find with a projection with a function and a new field.
//        queries.add(
//                """
//                        db.%s.find({},
//                        {
//                            name: 1,
//                            sum: { $add: [ "$num", "$num" ] }
//                        })
//                        """
//        );

//        queries.add(
//                """
//                        db.%s.find({},
//                        {
//                            name: 1,
//                            _distance: { $distance: [ "$legacy", "$legacy.$near.$geometry" ] }
//                        })
//                        """
//        );
//        DocResult result = execute( queries.get( 1 ).formatted( defaultCollection ), namespace );
//        System.out.println("Test");
//        List<DocResult> results = runQueries( queries );
//        compareResults( results );
    }


    @Test
    public void PipelineTest() {
        String pipeline = """
                db.%s.aggregate([
                  {
                    "$addFields": {
                      "sum": {
                        "$add": ["$num", "$num"]
                      }
                    }
                  },
                  {
                    "$match": {
                      "sum": {
                        "$gte": 1,
                        "$lte": 4
                      }
                    }
                  }
                ])
                """.formatted( defaultCollection );
        DocResult result = execute( pipeline, namespace );
        System.out.println( "Done" );
    }


    private void clearCollections() {
        for ( String collection : collections ) {
            execute( clearCollection.formatted( collection ), namespace );
        }
    }


    private static void addMongoDbAdapter() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.addMongodb( mongoAdapterName, statement );
                initDatabase( "test_mongo" );
            }
        } catch ( SQLException e ) {
            // If there is an error while adding the adapter, the most likely reason it does not work
            // is that docker is not running!
            throw new RuntimeException( e );
        }
    }


    /**
     * Runs the queries for each collection, and saves the result of the
     * final query to a list. Afterward, we assert if the result of
     * both systems match.
     *
     * @param queries Queries which are run for each system. Make sure
     * the query contains a placeholder %s for the collection.
     */
    private List<DocResult> runQueries( List<String> queries ) {
        ArrayList<DocResult> results = new ArrayList<>();

        for ( String collection : collections ) {
            DocResult finalResult = null;
            for ( String queryWithPlaceholder : queries ) {
                String query = queryWithPlaceholder.formatted( collection );
                finalResult = execute( query, namespace );
            }
            results.add( finalResult );
        }

        assertEquals( 2, results.size() );
        return results;
    }


    private void compareResults( List<DocResult> results ) {
        compareResults( results.get( 0 ), results.get( 1 ) );
    }


    private void compareResults( DocResult mongoResult, DocResult result ) {
        assertEquals( mongoResult.data.length, result.data.length );

        ObjectMapper objectMapper = new ObjectMapper();
        for ( int i = 0; i < result.data.length; i++ ) {
            String document = result.data[i];
            String mongoDocument = mongoResult.data[i];

            Map<String, Object> documentMap;
            Map<String, Object> mongoDocumentMap;
            try {
                documentMap = objectMapper.readValue( document, new TypeReference<Map<String, Object>>() {
                } );
                mongoDocumentMap = objectMapper.readValue( mongoDocument, new TypeReference<Map<String, Object>>() {
                } );
            } catch ( JsonProcessingException e ) {
                throw new RuntimeException( e );
            }
            assertEquals( mongoDocumentMap.keySet(), documentMap.keySet() );

            for ( Map.Entry<String, Object> entry : documentMap.entrySet() ) {
                String key = entry.getKey();
                if ( Objects.equals( key, "_id" ) ) {
                    // Do not compare the _id, as this will be different.
                    continue;
                }
                Object value = entry.getValue();
                Object mongoValue = entry.getValue();
                assertEquals( mongoValue, value );
            }
        }
    }

}
