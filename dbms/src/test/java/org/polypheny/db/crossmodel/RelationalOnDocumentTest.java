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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.jdbc.types.PolyDocument;

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
            PolyDocument document = (PolyDocument) doc.get( 0 )[0];
            assertEquals( document.size(), 2 );
            assertEquals( document.get( "_id" ).asString(), "630103687f2e95058018fd9b" );
            assertEquals( document.get( "test" ).asInt(), 3 );
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


    @Test
    public void jsonValueConvertsBooleanStrings() {
        executeStatements( ( s, c ) -> {
            String query = """
                    SELECT JSON_VALUE('{"value":"true"}', 'lax $.value' RETURNING BOOLEAN),
                           JSON_VALUE('{"value":"false"}', 'lax $.value' RETURNING BOOLEAN),
                           JSON_VALUE('{"value":null}', 'lax $.value' RETURNING BOOLEAN),
                           JSON_VALUE('{"value":"true"}', 'lax $.value')
                    """;
            try ( ResultSet result = s.executeQuery( query ) ) {
                TestHelper.checkResultSet( result, List.<Object[]>of( new Object[]{ Boolean.TRUE, Boolean.FALSE, null, "true" } ) );
            }
        } );
    }


    @Test
    public void jsonValuePreservesDecimalsOutsideLongRange() {
        executeStatements( ( s, c ) -> {
            String query = """
                    SELECT JSON_VALUE('{"n":{"$numberDecimal":"9223372036854775808"}}', 'lax $.n'),
                           JSON_VALUE('{"n":{"$numberDecimal":"-9223372036854775809"}}', 'lax $.n')
                    """;
            try ( ResultSet result = s.executeQuery( query ) ) {
                TestHelper.checkResultSet( result, List.<Object[]>of( new Object[]{ "9223372036854775808", "-9223372036854775809" } ) );
            }
        } );
    }


    @Test
    public void joinOnJsonStringTest() {
        String joinCollection = "crossJoinProducts";
        List<String> names = List.of( "widget", "null", "12.00", "{quoted\"\\key" );
        MqlTestTemplate.createCollection( joinCollection, DATABASE_NAME );
        for ( String name : names ) {
            String document = String.format( "{\"name\":%s}", PolyString.of( name ).toQuotedJson() );
            MqlTestTemplate.insert( document, joinCollection, DATABASE_NAME );
        }

        executeStatements( ( s, c ) -> {
            s.executeUpdate( "CREATE TABLE crossJoinOrders (id INTEGER PRIMARY KEY, product_name VARCHAR(64) NOT NULL)" );
            try {
                try ( PreparedStatement insert = c.prepareStatement( "INSERT INTO crossJoinOrders VALUES (?, ?)" ) ) {
                    for ( int i = 0; i < names.size(); i++ ) {
                        insert.setInt( 1, i );
                        insert.setString( 2, names.get( i ) );
                        insert.executeUpdate();
                    }
                }

                String query = String.format( """
                        SELECT o.id, JSON_VALUE(p.d, 'lax $.name')
                        FROM crossJoinOrders o
                        JOIN %s.%s p ON o.product_name = JSON_VALUE(p.d, 'lax $.name')
                        ORDER BY o.id
                        """, DATABASE_NAME, joinCollection );
                try ( ResultSet result = s.executeQuery( query ) ) {
                    TestHelper.checkResultSet( result, List.of(
                            new Object[]{ 0, names.get( 0 ) },
                            new Object[]{ 1, names.get( 1 ) },
                            new Object[]{ 2, names.get( 2 ) },
                            new Object[]{ 3, names.get( 3 ) } ) );
                }
            } finally {
                s.executeUpdate( "DROP TABLE crossJoinOrders" );
            }
        } );
    }


    @Test
    public void joinOnJsonPathSumPreservesInfinity() {
        String samplesCollection = "crossJoinSamples";
        MqlTestTemplate.createCollection( samplesCollection, DATABASE_NAME );
        MqlTestTemplate.insert( "{\"samples\":[1e308,1e308]}", samplesCollection, DATABASE_NAME );

        executeStatements( ( s, c ) -> {
            s.executeUpdate( "CREATE TABLE crossJoinTotals (id INTEGER PRIMARY KEY, total VARCHAR(32) NOT NULL)" );
            try {
                s.executeUpdate( "INSERT INTO crossJoinTotals VALUES (1, 'Infinity')" );
                s.executeUpdate( "INSERT INTO crossJoinTotals VALUES (2, '0')" );

                String query = String.format( """
                        SELECT t.id, JSON_VALUE(s.d, 'lax $.samples.sum()')
                        FROM crossJoinTotals t
                        JOIN %s.%s s ON t.total = JSON_VALUE(s.d, 'lax $.samples.sum()')
                        """, DATABASE_NAME, samplesCollection );
                try ( ResultSet result = s.executeQuery( query ) ) {
                    TestHelper.checkResultSet( result, List.<Object[]>of( new Object[]{ 1, "Infinity" } ) );
                }
            } finally {
                s.executeUpdate( "DROP TABLE crossJoinTotals" );
            }
        } );
    }


}
