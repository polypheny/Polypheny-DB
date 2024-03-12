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

package org.polypheny.db.restapi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.RequestBodyEntity;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class RestTest {

    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestSchema();
    }


    @AfterAll
    public static void stop() {
        deleteOldData();
    }


    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private static void addTestSchema() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE SCHEMA restschema" );
                statement.executeUpdate( "CREATE TABLE restschema.resttest( "
                        + "tbigint BIGINT NOT NULL, "
                        + "tboolean BOOLEAN NOT NULL, "
                        + "tdate DATE NOT NULL, "
                        + "tdecimal DECIMAL(5,2) NOT NULL, "
                        + "tdouble DOUBLE NOT NULL, "
                        + "tinteger INTEGER NOT NULL, "
                        + "treal REAL NOT NULL, "
                        + "tsmallint SMALLINT NOT NULL, "
                        + "ttime TIME NOT NULL, "
                        + "ttimestamp TIMESTAMP NOT NULL, "
                        + "ttinyint TINYINT NOT NULL, "
                        + "tvarchar VARCHAR(20) NOT NULL, "
                        + "PRIMARY KEY (tinteger) )" );
                statement.executeUpdate( "CREATE VIEW restschema.viewtest AS SELECT * FROM restschema.resttest" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW restschema.materializedtest AS SELECT restschema.resttest.tinteger FROM restschema.resttest FRESHNESS MANUAL " );
                connection.commit();
            }
        }
    }


    private static void deleteOldData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW restschema.materializedtest" );
                    statement.executeUpdate( "DROP VIEW restschema.viewtest" );
                    statement.executeUpdate( "DROP TABLE restschema.resttest" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                statement.executeUpdate( "DROP SCHEMA restschema" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting old data", e );
        }
    }


    private HttpResponse<String> executeRest( HttpRequest<?> request ) {
        request.basicAuth( "pa", "" );
        request.routeParam( "protocol", "http" );
        request.routeParam( "host", "127.0.0.1" );
        request.routeParam( "port", "8089" );
        if ( log.isDebugEnabled() ) {
            log.debug( request.getUrl() );
        }
        try {
            HttpResponse<String> result = request.asString();
            if ( !result.isSuccess() ) {
                throw new RuntimeException( "Error while executing REST query. Message: " + result.getStatusText() + "  |  URL: " + request.getUrl() );
            }
            return result;
        } catch ( UnirestException e ) {
            throw new RuntimeException( e );
        }
    }


    private static HttpRequest<?> buildRestInsert( String table, List<JsonObject> rows ) {
        JsonArray array = new JsonArray();
        rows.forEach( array::add );
        JsonObject data = new JsonObject();
        data.add( "data", array );

        return Unirest.post( "{protocol}://{host}:{port}/restapi/v1/res/" + table )
                .header( "Content-ExpressionType", "application/json" )
                .body( data );
    }


    private static HttpRequest<?> buildRestUpdate( String table, JsonObject set, Map<String, String> where ) {
        JsonArray array = new JsonArray();
        array.add( set );
        JsonObject data = new JsonObject();
        data.add( "data", array );

        RequestBodyEntity request = Unirest.patch( "{protocol}://{host}:{port}/restapi/v1/res/" + table )
                .header( "Content-ExpressionType", "application/json" )
                .body( data );

        for ( Map.Entry<String, String> entry : where.entrySet() ) {
            request.queryString( entry.getKey(), entry.getValue() );
        }

        return request;
    }


    private static HttpRequest<?> buildRestDelete( String table, Map<String, String> where ) {
        HttpRequest<?> request = Unirest.delete( "{protocol}://{host}:{port}/restapi/v1/res/" + table )
                .header( "Content-ExpressionType", "application/json" );

        for ( Map.Entry<String, String> entry : where.entrySet() ) {
            request.queryString( entry.getKey(), entry.getValue() );
        }

        return request;
    }

    // --------------- Tests ---------------


    @Test
    @Tag("monetdbExcluded")
    public void testOperations() {
        // Insert
        HttpRequest<?> request = buildRestInsert( "restschema.resttest", ImmutableList.of( getTestRow() ) );
        assertEquals(
                "{\"result\":[{\"ROWCOUNT\":1}],\"size\":1}",
                executeRest( request ).getBody() );

        // Update
        Map<String, String> where = new HashMap<>();
        where.put( "restschema.resttest.tsmallint", "=" + 45 );
        request = buildRestUpdate( "restschema.resttest", getTestRow( 1 ), where );
        assertEquals(
                "{\"result\":[{\"ROWCOUNT\":1}],\"size\":1}",
                executeRest( request ).getBody() );

        // Update
        Map<String, String> where2 = new HashMap<>();
        where2.put( "restschema.resttest.tsmallint", "=" + 46 );
        request = buildRestUpdate( "restschema.resttest", getTestRow( 0 ), where2 );
        assertEquals(
                "{\"result\":[{\"ROWCOUNT\":1}],\"size\":1}",
                executeRest( request ).getBody() );

        // Get
        request = Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/restschema.resttest" )
                .queryString( "restschema.resttest.ttinyint", "=" + 22 );

        String expected = "{\"result\":[{\"restschema.resttest.tsmallint\":45,\"restschema.resttest.tdecimal\":123.45,\"restschema.resttest.ttinyint\":22,\"restschema.resttest.treal\":0.3333,\"restschema.resttest.tinteger\":9876,\"restschema.resttest.ttime\":43505000,\"restschema.resttest.tbigint\":1234,\"restschema.resttest.tboolean\":true,\"restschema.resttest.tdate\":18466,\"restschema.resttest.tdouble\":1.999999,\"restschema.resttest.tvarchar\":\"hallo\",\"restschema.resttest.ttimestamp\":\"2020-07-23T12:05:05\"}],\"size\":1}";
        JsonElement jsonExpected = JsonParser.parseString( expected );
        JsonElement jsonResult = JsonParser.parseString( executeRest( request ).getBody() );
        assertEquals( jsonExpected, jsonResult );

        // Delete
        where = new HashMap<>();
        where.put( "restschema.resttest.tvarchar", "=" + "hallo" );
        request = buildRestDelete( "restschema.resttest", where );
        assertEquals(
                "{\"result\":[{\"ROWCOUNT\":1}],\"size\":1}",
                executeRest( request ).getBody() );

        // Select
        request = Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/restschema.resttest" )
                .queryString( "restschema.resttest.tinteger", "=" + 9876 );
        assertEquals(
                "{\"result\":[],\"size\":0}",
                executeRest( request ).getBody() );

        //Select View
        request = Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/restschema.viewtest" ).
                queryString( "restschema.viewtest.tinteger", "=" + 9876 );
        assertEquals( "{\"result\":[],\"size\":0}",
                executeRest( request ).getBody() );

        request = Unirest.get( "{protocol}://{host}:{port}/restapi/v1/res/restschema.materializedtest" ).queryString( "restschema.materializedtest.tinteger", "=" + 9876 );
        assertEquals( "{\"result\":[],\"size\":0}",
                executeRest( request ).getBody() );
    }


    private JsonObject getTestRow() {
        return getTestRow( 0 );
    }


    private JsonObject getTestRow( int change ) {
        JsonObject row = new JsonObject();
        row.add(
                "restschema.resttest.tbigint",
                new JsonPrimitive( 1234L + change ) );
        row.add(
                "restschema.resttest.tboolean",
                new JsonPrimitive( true ) );
        row.add(
                "restschema.resttest.tdate",
                new JsonPrimitive( LocalDate.of( 2020, 7, 23 ).format( DateTimeFormatter.ISO_LOCAL_DATE ) ) );
        row.add(
                "restschema.resttest.tdecimal",
                new JsonPrimitive( new BigDecimal( "123.45" ) ) );
        row.add(
                "restschema.resttest.tdouble",
                new JsonPrimitive( 1.999999 ) );
        row.add(
                "restschema.resttest.tinteger",
                new JsonPrimitive( 9876 ) );
        row.add(
                "restschema.resttest.treal",
                new JsonPrimitive( 0.3333 ) );
        row.add(
                "restschema.resttest.tsmallint",
                new JsonPrimitive( 45 + change ) );
        row.add(
                "restschema.resttest.ttime",
                new JsonPrimitive( LocalTime.of( 12, 5, 5 ).format( DateTimeFormatter.ISO_LOCAL_TIME ) ) );
        row.add(
                "restschema.resttest.ttimestamp",
                new JsonPrimitive( LocalDateTime.of( 2020, 7, 23, 12, 5, 5 ).format( DateTimeFormatter.ISO_LOCAL_DATE_TIME ) ) );
        row.add(
                "restschema.resttest.ttinyint",
                new JsonPrimitive( 22 ) );
        row.add(
                "restschema.resttest.tvarchar",
                new JsonPrimitive( "hallo" ) );
        return row;
    }

}
