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
public class MqlGeoFunctionsBenchmark extends MqlTestTemplate {

    final static String mongoAdapterName = "mongo";
    final static String mongoCollection = "mongo";
    final static String defaultCollection = "default";
    final static List<String> collections = List.of( defaultCollection, mongoCollection );
    final static Map<String, String> collectionToStore = Map.of( mongoCollection, mongoAdapterName, defaultCollection, "hsqldb" );
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
        clearCollections();
    }


//    default: 34.92757623762376ms
//    mongo: 11.864550495049505ms
    @Test
    public void docGeoWithinPerformanceTst() {
        List<DocResult> results;
        String insertMany = generateInsertTestDataQueries( 100 );
        String geoWithinBox = """
                db.%s.find({
                    location: {
                       $geoWithin: {
                          $box: [
                            [10,10],
                            [20,20]
                          ]
                       }
                    }
                })
                """;
        benchmarkQueries( Arrays.asList( insertMany, geoWithinBox ) );
    }


    private String generateInsertTestDataQueries( int itemCount ) {
        List<String> items = new ArrayList<>( itemCount );
        for ( int i = 0; i < itemCount; i++ ) {
            String itemTemplate = """
                    {
                      name: "GeoJSON [%s,%s]",
                      num: %s,
                      location: {
                        type: "Point",
                        coordinates: [%s,%s]
                      }
                    }
                    """.formatted( i, i, i, i, i );
            items.add( itemTemplate );
        }
        String query = "db.%s.insertMany([" + String.join( ",", items ) + "])";
        return query;
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
    private void benchmarkQueries( List<String> queries ) {
        int runCount = 110;
        int warmUpRuns = 10;

        for ( String collection : collections ) {
            List<Double> durations = new ArrayList<>();
            double durationMs = -1;
            for ( int i = 0; i < queries.size(); i++ ) {
                String queryWithPlaceholder = queries.get( i );
                String query = queryWithPlaceholder.formatted( collection );

                int runs = queries.size() - 1 == i ? runCount : 1;
                for ( int j = 0; j < runs; j++ ) {
                    long startTime = System.nanoTime();
                    // Use for verification
                    DocResult finalResult = execute( query, namespace );
                    if (runs == runCount){
//                        System.out.println( finalResult.toString() );
                    }

                    long endTime = System.nanoTime();
                    durationMs = (endTime - startTime) / 1_000_000.0;
                    if (j >= warmUpRuns){
                        durations.add( durationMs );
                    }
                }
            }
            assert durations.size() == runCount - warmUpRuns;
            double averageDurationMs = durations.stream()
                    .mapToDouble( Double::doubleValue )
                    .average()
                    .orElse( -1 );
            System.out.println( collection + ": " + averageDurationMs + "ms" );
        }
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
                documentMap = objectMapper.readValue( document, new TypeReference<>() {
                } );
                mongoDocumentMap = objectMapper.readValue( mongoDocument, new TypeReference<>() {
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
