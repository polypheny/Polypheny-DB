/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.Result;


/**
 * Test template, which wraps often-used MongoQL commands
 * for easier execution
 */
public class MqlTestTemplate {

    public static String database = "test";


    @BeforeClass
    public static void start() {
        TestHelper.getInstance();
        initDatabase();
    }


    @AfterClass
    public static void tearDown() {
        dropDatabase();
    }


    public static void createCollection( String collection, String database ) {
        MongoConnection.executeGetResponse( String.format( "db.createCollection( %s )", collection ), database );
    }


    @After
    public void cleanDocuments() {
        deleteMany( "{}" );
    }


    protected static void dropDatabase() {
        dropDatabase( database );
    }


    public static Result execute( String doc ) {
        return MongoConnection.executeGetResponse( doc );
    }


    public static Result execute( String doc, String database ) {
        return MongoConnection.executeGetResponse( doc, database );
    }


    public static void initDatabase() {
        initDatabase( database );
    }


    public static void initDatabase( String database ) {
        MongoConnection.executeGetResponse( "use " + database );
    }


    protected String $addFields( String doc ) {
        return "{\"$addFields\":" + doc + "}";
    }


    protected String $count( String doc ) {
        return "{\"$count\":\"" + doc + "\"}";
    }


    protected String $group( String doc ) {
        return "{\"$group\":" + doc + "}";
    }


    protected String $limit( Integer limit ) {
        return "{\"$limit\":" + limit + "}";
    }


    protected String $match( String doc ) {
        return "{\"$match\":" + doc + "}";
    }


    protected String $project( String doc ) {
        return "{\"$project\":" + doc + "}";
    }


    protected String $replaceRoot( String doc ) {
        return "{\"$replaceRoot\":{\"newRoot\":\"" + doc + "\"}}";
    }


    protected String $replaceWith( String doc ) {
        return "{\"$replaceWith\":\"" + doc + "\"}";
    }


    protected String $set( String doc ) {
        return "{\"$set\":" + doc + "}";
    }


    protected String $skip( int amount ) {
        return "{\"$skip\":" + amount + "}";
    }


    protected String $sort( String doc ) {
        return "{\"$sort\":" + doc + "}";
    }


    protected String $unset( String doc ) {
        return "{\"$unset\":\"" + doc + "\"}";
    }


    protected String $unset( List<String> docs ) {
        return "{\"$unset\":[" + String.join( ",", docs ) + "]}";
    }


    protected String $unwind( String path ) {
        return "{\"$unwind\":\"" + path + "\"}";
    }


    public static void insert( String json ) {
        insert( json, database );
    }


    public static void insert( String json, String collection ) {
        insert( json, collection, database );
    }


    public static void insert( String json, String collection, String database ) {
        MongoConnection.executeGetResponse( "db." + collection + ".insert(" + json + ")", database );
    }


    public static void insertMany( List<String> jsons ) {
        insertMany( jsons, database );
    }


    public static void insertMany( List<String> jsons, String db ) {
        MongoConnection.executeGetResponse( "db." + db + ".insertMany([" + String.join( ",", jsons ) + "])" );
    }


    public static void update( String query, String update ) {
        update( query, update, database );
    }


    public static void update( String query, String update, String db ) {
        MongoConnection.executeGetResponse( "db." + db + ".update(" + query + ", " + update + ")" );
    }


    protected Result find( String query, String project ) {
        return find( query, project, database );
    }


    protected Result find( String query, String project, String db ) {
        return MongoConnection.executeGetResponse( "db." + db + ".find(" + query + "," + project + ")" );
    }


    protected Result aggregate( String... stages ) {
        return aggregate( database, Arrays.asList( stages ) );
    }


    protected Result aggregate( String db, List<String> stages ) {
        return MongoConnection.executeGetResponse( "db." + db + ".aggregate([" + String.join( ",", stages ) + "])" );
    }


    protected static void deleteMany( String query ) {
        deleteMany( query, database );
    }


    protected static void deleteMany( String query, String database ) {
        MongoConnection.executeGetResponse( "db." + database + ".deleteMany(" + query + ")" );
    }


    public static void dropDatabase( String database ) {
        MongoConnection.executeGetResponse( "use " + database );
        MongoConnection.executeGetResponse( "db.dropDatabase()", database );
    }

}
