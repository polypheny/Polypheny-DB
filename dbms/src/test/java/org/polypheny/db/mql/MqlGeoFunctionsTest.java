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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.webui.models.results.DocResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("SqlNoDataSourceInspection")
@Tag("adapter")
@Slf4j
public class MqlGeoFunctionsTest extends MqlTestTemplate {

    final static String namespaceMongo = "test_mongo";
    final static String collectionName = "doc";
    final static String mongoAdapterName = "mongo";
    final static ArrayList<String> namespaces = new ArrayList<>();


    @BeforeAll
    public static void init() {
        namespaces.add( namespace );
        namespaces.add( namespaceMongo );
        addMongoDbAdapter();
    }


    public static void addMongoDbAdapter() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.addMongodb( mongoAdapterName, statement );
                initDatabase( namespaceMongo );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    public static void removeMongoDbAdapter() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.executeSQL( statement, "DROP NAMESPACE \"%s\"".formatted( namespaceMongo ) );
                TestHelper.dropAdapter( mongoAdapterName, statement );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void docGeoIntersectsTest() {
        ArrayList<DocResult> results = new ArrayList<>();

        for ( String ns : namespaces ) {
            String createCollection = """
                    db.createCollection(%s)
                    """.formatted( ns );
            execute( createCollection, ns );

            if ( ns.equals( namespaceMongo ) ) {
                execute( String.format( "db.%s.addPlacement(\"%s\")", ns, mongoAdapterName ) );
                execute( String.format( "db.%s.deletePlacement(\"%s\")", ns, "hsqldb" ) );
            } else {
                execute( String.format( "db.%s.addPlacement(\"%s\")", ns, "hsqldb" ) );
                execute( String.format( "db.%s.deletePlacement(\"%s\")", ns, mongoAdapterName ) );
            }

            String insertDocuments = """
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
                    """.formatted( ns );
            execute( insertDocuments, ns );

            DocResult result;
            String geoIntersects = """
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
                    """.formatted( ns );
            result = execute( geoIntersects, ns );
            results.add( result );
        }

        compareResults( results.get( 0 ), results.get( 1 ) );
    }


    public static void compareResults( DocResult mongoResult, DocResult result ) {
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


    @Test
    public void docGeoWithinTest() {
        String insertDocuments = """
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
                """.formatted( collectionName );
        execute( insertDocuments );

        DocResult result;
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
                """.formatted( collectionName );
        result = execute( geoWithinBox );
        assertEquals( result.data.length, 2 );

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
                """.formatted( collectionName );
        result = execute( geoWithinGeometry );
        assertEquals( result.data.length, 2 );

        // TODO: This test does not make any sense, as 1.5 in radians is so big
        //       that it includes all three points.
        //       Create another test, with more sensible numbers...
        String geoWithinCenterSphere = """
                db.%s.find({
                    legacy: {
                        $geoWithin: {
                             $centerSphere: [
                                [ 0, 0 ],
                                1.5
                             ]
                        }
                    }
                })
                """.formatted( collectionName );
        result = execute( geoWithinCenterSphere );
        assertEquals( 3, result.data.length );
    }

}
