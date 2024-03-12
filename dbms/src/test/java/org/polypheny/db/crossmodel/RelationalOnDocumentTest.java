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

package org.polypheny.db.crossmodel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.ResultSet;
import java.util.List;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.mql.MqlTestTemplate;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class RelationalOnDocumentTest extends CrossModelTestTemplate {

    private static final String DATABASE_NAME = "crossDocumentSchema";

    private static final String COLLECTION_NAME = "crossCollection";
    public static final String TEST_DATA = "{\"_id\":\"630103687f2e95058018fd9b\",\"test\":3}";
    public static final String TEST_DATA_REV = "{\"test\":3,\"_id\":\"630103687f2e95058018fd9b\"}";


    @BeforeAll
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( DATABASE_NAME );
        MqlTestTemplate.createCollection( COLLECTION_NAME, DATABASE_NAME );
        MqlTestTemplate.insert( TEST_DATA, COLLECTION_NAME, DATABASE_NAME );
    }


    @AfterAll
    public static void tearDown() {
        MqlTestTemplate.dropDatabase( DATABASE_NAME );
    }


    @Test
    public void simpleSelectTest() {

        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT * FROM %s.%s", DATABASE_NAME, COLLECTION_NAME ) );
            List<Object[]> doc = TestHelper.convertResultSetToList( result );
            // contents of documents are non-deterministic, and we cannot compare them as usual through TestHelper.checkResultSet
            assertEquals( BsonDocument.parse( TEST_DATA ), BsonDocument.parse( (String) doc.get( 0 )[0] ) );
        } );
    }



    @Test
    public void itemJsonSelectTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT JSON_VALUE(CAST(d AS VARCHAR(2050)), 'lax $.test') FROM %s.%s", DATABASE_NAME, COLLECTION_NAME ) );
            TestHelper.checkResultSet( result, List.of( new Object[][]{ new Object[]{ "3" } } ) );
        } );
    }


    @Test
    public void itemJsonSelectUnknownLaxTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT JSON_VALUE(CAST(d AS VARCHAR(2050)), 'lax $.other') FROM %s.%s", DATABASE_NAME, COLLECTION_NAME ) );
            TestHelper.checkResultSet( result, List.of( new Object[][]{ new Object[]{ null } } ) );
        } );
    }


}
