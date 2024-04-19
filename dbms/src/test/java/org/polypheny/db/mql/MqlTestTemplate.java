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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.webui.models.results.DocResult;


/**
 * Test template, which wraps often-used MongoQL commands
 * for easier execution
 */
public class MqlTestTemplate {

    public static String namespace = "test";


    @BeforeAll
    public static void start() {
        TestHelper.getInstance();
        initDatabase();
    }


    @BeforeEach
    public void initCollection() {
        initCollection( namespace );
    }


    @AfterEach
    public void dropCollection() {
        dropCollection( namespace );
    }


    public static void dropCollection( String collection ) {
        MongoConnection.executeGetResponse( "db." + collection + ".drop()" );
    }


    public static void initCollection( String collection ) {
        MongoConnection.executeGetResponse( "db.createCollection(" + collection + ")" );
    }


    @AfterAll
    public static void tearDown() {
        dropDatabase();
    }


    public static void createCollection( String collection, String database ) {
        MongoConnection.executeGetResponse( String.format( "db.createCollection( %s )", collection ), database );
    }


    @AfterEach
    public void cleanDocuments() {
        deleteMany( "{}" );
    }


    protected static void dropDatabase() {
        dropDatabase( namespace );
    }


    public static DocResult execute( String doc ) {
        return MongoConnection.executeGetResponse( doc );
    }


    public static DocResult execute( String doc, String database ) {
        return MongoConnection.executeGetResponse( doc, database );
    }


    public static void initDatabase() {
        initDatabase( namespace );
    }


    public static void initDatabase( String database ) {
        MongoConnection.executeGetResponse( "use " + database );
    }


    public static String document( String... entries ) {
        return String.format( "{ %s }", String.join( ",", entries ) );
    }


    public static String string( String string ) {
        return String.format( "\"%s\"", string );
    }


    public static String kv( String key, Object value ) {
        return String.format( "%s : %s", key, value );
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
        insert( json, namespace );
    }


    public static void insert( String json, String collection ) {
        insert( json, collection, namespace );
    }


    public static void insert( String json, String collection, String database ) {
        MongoConnection.executeGetResponse( "db." + collection + ".insert(" + json + ")", database );
    }


    public static void insertMany( List<String> jsons ) {
        insertMany( jsons, namespace );
    }


    public static void insertMany( List<String> jsons, String db ) {
        MongoConnection.executeGetResponse( "db." + db + ".insertMany([" + String.join( ",", jsons ) + "])" );
    }


    public static void update( String query, String update ) {
        update( query, update, namespace );
    }


    public static void update( String query, String update, String db ) {
        MongoConnection.executeGetResponse( "db." + db + ".update(" + query + ", " + update + ")" );
    }


    protected DocResult find( String query, String project ) {
        return find( query, project, namespace );
    }


    protected DocResult find( String query, String project, String db ) {
        return MongoConnection.executeGetResponse( "db." + db + ".find(" + query + "," + project + ")" );
    }


    protected DocResult aggregate( String... stages ) {
        return aggregate( namespace, Arrays.asList( stages ) );
    }


    protected DocResult aggregate( String db, List<String> stages ) {
        return MongoConnection.executeGetResponse( "db." + db + ".aggregate([" + String.join( ",", stages ) + "])" );
    }


    protected static void deleteMany( String query ) {
        deleteMany( query, namespace );
    }


    protected static void deleteMany( String query, String database ) {
        MongoConnection.executeGetResponse( "db." + database + ".deleteMany(" + query + ")" );
    }


    public static void dropDatabase( String database ) {
        MongoConnection.executeGetResponse( "db.dropDatabase()", database );
    }

}
