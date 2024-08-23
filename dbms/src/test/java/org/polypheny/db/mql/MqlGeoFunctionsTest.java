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

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("SqlNoDataSourceInspection")
@Tag("adapter")
public class MqlGeoFunctionsTest extends MqlTestTemplate {

    final static String collectionName = "doc";
    final static String mongoAdapterName = "mongo";

    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        TestHelper.getInstance();

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
//                TestHelper.addHsqldb(mongoAdapterName, statement);
                TestHelper.addMongodb( mongoAdapterName, statement );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @BeforeEach
    public void beforeEach() {
        // Clear both DBs before each test
        String deleteCollection = "db.%s.deleteMany({})".formatted( collectionName );
        execute(deleteCollection);
        execute(deleteCollection, mongoAdapterName);
    }


    @Test
    public void docGeoWithinTest() {
        // TODO: Compare values with MongoDB, instead of with the values that I expect.
        //       Somehow possible to execute the commands once on mongodb, and once on
        //       hsqldb?
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

        String geoWithin = """
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
        DocResult result = execute( geoWithin );
        assertEquals( result.data.length, 2 );
    }

}
