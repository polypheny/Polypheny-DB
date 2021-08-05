/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.mongoql.model.Result;


/**
 * Test template, which wraps often-used MongoQL commands
 * for easier execution
 */
public class MqlTestTemplate {

    public static String database = "test";


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        initDatabase();
    }


    @After
    public void cleanDocuments() {
        deleteMany( "{}" );
    }


    private static void initDatabase() {
        MongoConnection.executeGetResponse( "use " + database );
    }


    public static void insert( String json ) {
        insert( json, database );
    }


    public static void insert( String json, String db ) {
        MongoConnection.executeGetResponse( "db." + db + ".insert(" + json + ")" );
    }


    public static void insertMany( List<String> jsons ) {
        insertMany( jsons, database );
    }


    public static void insertMany( List<String> jsons, String db ) {
        MongoConnection.executeGetResponse( "db." + db + ".insertMany([" + String.join( ",", jsons ) + "])" );
    }


    protected Result find( String query, String project ) {
        return find( query, project, database );
    }


    protected Result find( String query, String project, String db ) {
        return MongoConnection.executeGetResponse( "db." + db + ".find(" + query + "," + project + ")" );
    }


    protected static void deleteMany( String query ) {
        deleteMany( query, database );
    }


    protected static void deleteMany( String query, String database ) {
        MongoConnection.executeGetResponse( "db." + database + ".deleteMany(" + query + ")" );
    }

}
