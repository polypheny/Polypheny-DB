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
    private static final String EDGE_COLLECTION_NAME = "crossSqlEdgeCollection";
    public static final String TEST_DATA = "{\"_id\":\"630103687f2e95058018fd9b\",\"test\":3,\"name\":\"Max\"}";
    public static final String TEST_DATA_REV = "{\"test\":3,\"name\":\"Max\",\"_id\":\"630103687f2e95058018fd9b\"}";


    @BeforeAll
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( DATABASE_NAME );
        MqlTestTemplate.createCollection( COLLECTION_NAME, DATABASE_NAME );
        MqlTestTemplate.insert( TEST_DATA, COLLECTION_NAME, DATABASE_NAME );
        MqlTestTemplate.createCollection( EDGE_COLLECTION_NAME, DATABASE_NAME );
        MqlTestTemplate.execute(
                "db." + EDGE_COLLECTION_NAME + ".insertMany(["
                        + "{\"_id\":\"edge_1\",\"name\":\"Alice\",\"age\":30,\"score\":12.5,\"active\":true,\"group\":\"A\",\"tags\":[\"urgent\",\"blue\"],\"nested\":{\"level\":2,\"code\":\"x.y\"},\"visits\":[{\"day\":1,\"value\":10},{\"day\":2,\"value\":20}]},"
                        + "{\"_id\":\"edge_2\",\"name\":\"Bob\",\"age\":22,\"score\":7,\"active\":false,\"group\":\"B\",\"tags\":[\"green\"],\"nested\":{\"level\":1},\"visits\":[{\"day\":1,\"value\":5}],\"note\":\"contains spaces\"},"
                        + "{\"_id\":\"edge_3\",\"name\":\"Cara\",\"age\":35,\"score\":12.5,\"active\":true,\"group\":\"A\",\"tags\":[],\"nested\":{\"level\":3,\"code\":\"z\"},\"visits\":[]},"
                        + "{\"_id\":\"edge_4\",\"name\":\"Drew\",\"active\":true,\"group\":\"C\",\"tags\":[\"urgent\"],\"nested\":{},\"visits\":[]}"
                        + "])",
                DATABASE_NAME );
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
    public void jsonValueScalarNestedAndArrayIndexTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format(
                    "SELECT "
                            + "JSON_VALUE(d, 'lax $._id') AS doc_id, "
                            + "JSON_VALUE(d, 'lax $.name') AS doc_name, "
                            + "JSON_VALUE(d, 'lax $.age') AS age_value, "
                            + "JSON_VALUE(d, 'lax $.active') AS active_value, "
                            + "JSON_VALUE(d, 'lax $.nested.level') AS nested_level, "
                            + "JSON_VALUE(d, 'lax $.visits[1].value') AS second_visit "
                            + "FROM %s.%s "
                            + "ORDER BY doc_id",
                    DATABASE_NAME,
                    EDGE_COLLECTION_NAME ) );

            TestHelper.checkResultSet( result, List.of(
                    new Object[]{ "edge_1", "Alice", "30", "true", "2", "20" },
                    new Object[]{ "edge_2", "Bob", "22", "false", "1", null },
                    new Object[]{ "edge_3", "Cara", "35", "true", "3", null },
                    new Object[]{ "edge_4", "Drew", null, "true", null, null }
            ) );
        } );
    }


    @Test
    public void jsonValueMissingAndNonScalarPathsReturnNullTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format(
                    "SELECT "
                            + "JSON_VALUE(d, 'lax $.missing') AS missing_value, "
                            + "JSON_VALUE(d, 'lax $.tags') AS array_value, "
                            + "JSON_VALUE(d, 'lax $.nested') AS object_value, "
                            + "JSON_VALUE(d, 'lax $.visits') AS object_array_value "
                            + "FROM %s.%s "
                            + "WHERE JSON_VALUE(d, 'lax $._id') = 'edge_1'",
                    DATABASE_NAME,
                    EDGE_COLLECTION_NAME ) );

            TestHelper.checkResultSet( result, List.<Object[]>of( new Object[]{ null, null, null, null } ) );
        } );
    }


    @Test
    public void jsonExistsMissingArraysAndFilterSemanticsTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format(
                    "SELECT "
                            + "JSON_VALUE(d, 'lax $._id') AS doc_id, "
                            + "JSON_EXISTS(d, 'lax $.tags') AS tags_exist, "
                            + "JSON_EXISTS(d, 'lax $.tags[?(@ == \"urgent\")]') AS urgent_exists, "
                            + "JSON_EXISTS(d, 'lax $.tags[?(@ == \"missing\")]') AS missing_tag_exists, "
                            + "JSON_EXISTS(d, 'lax $.missing') AS missing_path_exists "
                            + "FROM %s.%s "
                            + "ORDER BY doc_id",
                    DATABASE_NAME,
                    EDGE_COLLECTION_NAME ) );

            TestHelper.checkResultSet( result, List.of(
                    new Object[]{ "edge_1", true, true, false, false },
                    new Object[]{ "edge_2", true, false, false, false },
                    new Object[]{ "edge_3", true, false, false, false },
                    new Object[]{ "edge_4", true, true, false, false }
            ) );
        } );
    }


    @Test
    public void sqlFilterAndOrderByJsonExpressionsTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format(
                    "SELECT JSON_VALUE(d, 'lax $.name') AS doc_name "
                            + "FROM %s.%s "
                            + "WHERE JSON_EXISTS(d, 'lax $.tags[?(@ == \"urgent\")]') "
                            + "ORDER BY doc_name",
                    DATABASE_NAME,
                    EDGE_COLLECTION_NAME ) );

            TestHelper.checkResultSet( result, List.of(
                    new Object[]{ "Alice" },
                    new Object[]{ "Drew" }
            ) );
        } );
    }


    @Test
    public void sqlAggregateOnJsonExtractedValuesTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format(
                    "SELECT JSON_VALUE(d, 'lax $.group') AS doc_group, COUNT(*) AS doc_count "
                            + "FROM %s.%s "
                            + "GROUP BY JSON_VALUE(d, 'lax $.group') "
                            + "ORDER BY doc_group",
                    DATABASE_NAME,
                    EDGE_COLLECTION_NAME ) );

            TestHelper.checkResultSet( result, List.of(
                    new Object[]{ "A", 2 },
                    new Object[]{ "B", 1 },
                    new Object[]{ "C", 1 }
            ) );
        } );
    }


    @Test
    public void sqlJoinRelationalTableWithDocumentJsonValuesTest() {
        executeStatements( ( s, c ) -> {
            s.executeUpdate( "DROP TABLE IF EXISTS cross_doc_groups" );
            try {
                s.executeUpdate( "CREATE TABLE cross_doc_groups( group_id VARCHAR(10) NOT NULL, label VARCHAR(20), PRIMARY KEY (group_id) )" );
                s.executeUpdate( "INSERT INTO cross_doc_groups VALUES ('A', 'Alpha')" );
                s.executeUpdate( "INSERT INTO cross_doc_groups VALUES ('B', 'Beta')" );

                ResultSet result = s.executeQuery( String.format(
                        "SELECT JSON_VALUE(d, 'lax $.name') AS doc_name, g.label "
                                + "FROM %s.%s c "
                                + "JOIN cross_doc_groups g ON JSON_VALUE(c.d, 'lax $.group') = g.group_id "
                                + "ORDER BY doc_name",
                        DATABASE_NAME,
                        EDGE_COLLECTION_NAME ) );

                TestHelper.checkResultSet( result, List.of(
                        new Object[]{ "Alice", "Alpha" },
                        new Object[]{ "Bob", "Beta" },
                        new Object[]{ "Cara", "Alpha" }
                ) );
            } finally {
                s.executeUpdate( "DROP TABLE IF EXISTS cross_doc_groups" );
            }
        } );
    }


    @Test
    public void jsonValueReturningTypeCastsCanBeUsedInSqlPredicatesTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format(
                    "SELECT JSON_VALUE(d, 'lax $.name') AS doc_name "
                            + "FROM %s.%s "
                            + "WHERE JSON_VALUE(d, 'lax $.age' RETURNING INTEGER) >= 30 "
                            + "ORDER BY doc_name",
                    DATABASE_NAME,
                    EDGE_COLLECTION_NAME ) );

            TestHelper.checkResultSet( result, List.of(
                    new Object[]{ "Alice" },
                    new Object[]{ "Cara" }
            ) );
        } );
    }


    @Test
    public void materializedViewWithJsonFilterAndExistsTest() {
        executeStatements( ( s, c ) -> {
            s.executeUpdate( "DROP MATERIALIZED VIEW IF EXISTS crossDocumentFilteredMaterialized" );
            try {
                s.executeUpdate( String.format(
                        "CREATE MATERIALIZED VIEW crossDocumentFilteredMaterialized AS "
                                + "SELECT "
                                + "JSON_VALUE(d, 'lax $._id') AS doc_id, "
                                + "JSON_VALUE(d, 'lax $.name') AS doc_name, "
                                + "JSON_EXISTS(d, 'lax $.tags[?(@ == \"urgent\")]') AS urgent, "
                                + "* "
                                + "FROM %s.%s "
                                + "WHERE JSON_EXISTS(d, 'lax $.tags[?(@ == \"urgent\")]') "
                                + "ON STORE hsqldb FRESHNESS MANUAL",
                        DATABASE_NAME,
                        EDGE_COLLECTION_NAME ) );

                ResultSet result = s.executeQuery( "SELECT doc_id, doc_name, urgent FROM crossDocumentFilteredMaterialized ORDER BY doc_id" );
                TestHelper.checkResultSet( result, List.of(
                        new Object[]{ "edge_1", "Alice", true },
                        new Object[]{ "edge_4", "Drew", true }
                ) );
            } finally {
                s.executeUpdate( "DROP MATERIALIZED VIEW IF EXISTS crossDocumentFilteredMaterialized" );
            }
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
