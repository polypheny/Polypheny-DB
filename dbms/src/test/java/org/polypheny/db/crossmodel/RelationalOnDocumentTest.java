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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.jdbc.types.PolyDocument;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class RelationalOnDocumentTest extends CrossModelTestTemplate {

    private static final String DATABASE_NAME = "crossDocumentSchema";

    private static final String COLLECTION_NAME = "crossCollection";
    public static final String TEST_DATA = "{\"_id\":\"630103687f2e95058018fd9b\",\"test\":3,\"name\":\"Max\"}";
    public static final String TEST_DATA_REV = "{\"test\":3,\"name\":\"Max\",\"_id\":\"630103687f2e95058018fd9b\"}";


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
            PolyDocument document = (PolyDocument) doc.get( 0 )[0];
            assertEquals( document.size(), 3 );
            assertEquals( document.get( "_id" ).asString(), "630103687f2e95058018fd9b" );
            assertEquals( document.get( "test" ).asInt(), 3 );
            assertEquals( document.get( "name" ).asString(), "Max" );
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
    public void itemJsonSelectStringTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT JSON_VALUE(CAST(d AS VARCHAR(2050)), 'lax $.name') FROM %s.%s", DATABASE_NAME, COLLECTION_NAME ) );
            TestHelper.checkResultSet( result, List.of( new Object[][]{ new Object[]{ "Max" } } ) );
        } );
    }


    @Test
    public void itemJsonSelectUnknownLaxTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT JSON_VALUE(CAST(d AS VARCHAR(2050)), 'lax $.other') FROM %s.%s", DATABASE_NAME, COLLECTION_NAME ) );
            TestHelper.checkResultSet( result, List.of( new Object[][]{ new Object[]{ null } } ) );
        } );
    }


    @Test
    public void materializedViewFromDocumentCollectionTest() {
        executeStatements( ( s, c ) -> {
            s.executeUpdate( String.format( "CREATE MATERIALIZED VIEW crossDocumentMaterialized AS SELECT * FROM %s.%s ON STORE hsqldb FRESHNESS MANUAL", DATABASE_NAME, COLLECTION_NAME ) );

            try {
                ResultSet result = s.executeQuery( "SELECT JSON_VALUE(CAST(d AS VARCHAR(2050)), 'lax $.test') FROM crossDocumentMaterialized" );
                TestHelper.checkResultSet( result, List.of( new Object[][]{ new Object[]{ "3" } } ) );
            } finally {
                s.executeUpdate( "DROP MATERIALIZED VIEW crossDocumentMaterialized" );
            }
        } );
    }


    @Test
    public void materializedViewWithJsonValueFromDocumentCollectionTest() {
        executeStatements( ( s, c ) -> {
            String collection = "crossBatchCollection";
            int batchSize = RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.getInteger();
            RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.setInteger( 1 );

            try {
                MqlTestTemplate.createCollection( collection, DATABASE_NAME );
                MqlTestTemplate.execute(
                        "db." + collection + ".insertMany(["
                                + "{\"_id\":\"batch_0\",\"name\":\"Patient0\",\"test\":0},"
                                + "{\"_id\":\"batch_1\",\"name\":\"Patient1\",\"test\":1}"
                                + "])",
                        DATABASE_NAME );

                s.executeUpdate( String.format(
                        "CREATE MATERIALIZED VIEW crossDocumentJsonMaterialized AS "
                                + "SELECT JSON_VALUE(d, 'lax $.name') AS patient_id, JSON_VALUE(d, 'lax $.test') AS viral_load_day1, * "
                                + "FROM %s.%s ON STORE hsqldb FRESHNESS MANUAL",
                        DATABASE_NAME,
                        collection ) );

                ResultSet result = s.executeQuery( "SELECT patient_id, viral_load_day1 FROM crossDocumentJsonMaterialized ORDER BY patient_id" );
                TestHelper.checkResultSet( result, List.of( new Object[][]{
                        new Object[]{ "Patient0", "0" },
                        new Object[]{ "Patient1", "1" }
                } ) );
            } finally {
                RuntimeConfig.DATA_MIGRATOR_BATCH_SIZE.setInteger( batchSize );
                try {
                    s.executeUpdate( "DROP MATERIALIZED VIEW crossDocumentJsonMaterialized" );
                } catch ( Exception ignored ) {
                    // The regression fails during creation, before the materialized view is available to drop.
                }
                MqlTestTemplate.execute( "db." + collection + ".drop()", DATABASE_NAME );
            }
        } );
    }


}
