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

package org.polypheny.db;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.runtime.functions.Functions;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.models.Result;


@Slf4j
public class TestHelper {

    private static final TestHelper INSTANCE = new TestHelper();
    private static final double EPSILON = 0.0001;

    private final PolyphenyDb polyphenyDb;

    @Getter
    private final TransactionManager transactionManager;


    public static TestHelper getInstance() {
        return INSTANCE;
    }


    private TestHelper() {
        polyphenyDb = new PolyphenyDb();
        log.info( "Starting Polypheny-DB..." );

        Runnable runnable = () -> {
            try {
                polyphenyDb.testMode = true;
                String defaultStoreName = System.getProperty( "store.default" );
                if ( defaultStoreName != null ) {
                    polyphenyDb.defaultStoreName = defaultStoreName;
                }
                // polyphenyDb.resetCatalog = true;
                polyphenyDb.runPolyphenyDb();
            } catch ( GenericCatalogException e ) {
                log.error( "Exception while starting Polypheny-DB", e );
            }
        };
        Thread thread = new Thread( runnable );
        thread.start();

        // Wait until Polypheny-DB is ready to process queries
        int i = 0;
        while ( !polyphenyDb.isReady() ) {
            try {
                TimeUnit.SECONDS.sleep( 1 );
                if ( i++ > 180 ) {
                    if ( thread.getStackTrace().length > 0 ) {
                        System.err.println( "Stacktrace of Polypheny-DB thread:" );
                        for ( int j = 0; j < thread.getStackTrace().length; j++ ) {
                            System.err.println( "\tat " + thread.getStackTrace()[j] );
                        }
                    }
                    throw new RuntimeException( "There seems to be an issue with Polypheny-DB. Waited 3 minutes for Polypheny-DB to get ready. Aborting tests." );
                }
            } catch ( InterruptedException e ) {
                log.error( "Interrupted exception", e );
            }
        }

        // Hack to get TransactionManager
        try {
            Field f = PolyphenyDb.class.getDeclaredField( "transactionManager" );
            f.setAccessible( true );
            transactionManager = (TransactionManager) f.get( polyphenyDb );
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new RuntimeException( e );
        }
    }


    public Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, true, "Test Helper" );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    @AfterClass
    public static void tearDown() {
        //LOG.info( "shutdown - closing DB connection" );
    }


    public static void checkResultSet( ResultSet resultSet, List<Object[]> expected ) throws SQLException {
        checkResultSet( resultSet, expected, false );
    }


    public static void checkResultSet( ResultSet resultSet, List<Object[]> expected, boolean ignoreOrderOfResultRows ) throws SQLException {
        checkResultSet( resultSet, expected, ignoreOrderOfResultRows, false );
    }


    // isConvertingDecimals should only(!) be set to true if a decimal value is the result of a type conversion (e.g., when change the type of column to decimal)
    public static void checkResultSet( ResultSet resultSet, List<Object[]> expected, boolean ignoreOrderOfResultRows, boolean isConvertingDecimals ) throws SQLException {
        List<Object[]> received = convertResultSetToList( resultSet );
        if ( ignoreOrderOfResultRows ) {
            expected = orderResultList( expected );
            received = orderResultList( received );
        }
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int i = 0;
        for ( Object[] row : received ) {
            Assert.assertTrue( "Result set has more rows than expected", i < expected.size() );
            Object[] expectedRow = expected.get( i++ );
            Assert.assertEquals( "Wrong number of columns", expectedRow.length, rsmd.getColumnCount() );
            int j = 0;
            while ( j < expectedRow.length ) {
                if ( expectedRow.length >= j + 1 ) {
                    int columnType = rsmd.getColumnType( j + 1 );
                    if ( columnType == Types.BINARY ) {
                        if ( expectedRow[j] == null ) {
                            Assert.assertNull( "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "': ", row[j] );
                        } else {
                            Assert.assertEquals(
                                    "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "'",
                                    new String( (byte[]) expectedRow[j] ),
                                    new String( (byte[]) row[j] ) );
                        }
                    } else if ( columnType != Types.ARRAY ) {
                        if ( expectedRow[j] != null ) {
                            if ( columnType == Types.FLOAT || columnType == Types.REAL ) {
                                float diff = Math.abs( (float) expectedRow[j] - (float) row[j] );
                                Assert.assertTrue(
                                        "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "': The difference between the expected float and the received float exceeds the epsilon. Difference: " + (diff - EPSILON),
                                        diff < EPSILON );
                            } else if ( columnType == Types.DOUBLE ) {
                                double diff = Math.abs( (double) expectedRow[j] - (double) row[j] );
                                Assert.assertTrue(
                                        "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "': The difference between the expected double and the received double exceeds the epsilon. Difference: " + (diff - EPSILON),
                                        diff < EPSILON );
                            } else if ( columnType == Types.DECIMAL ) { // Decimals are exact // but not for calculations?
                                BigDecimal expectedResult = (BigDecimal) expectedRow[j];
                                double diff = Math.abs( expectedResult.doubleValue() - ((BigDecimal) row[j]).doubleValue() );
                                if ( isConvertingDecimals ) {
                                    Assert.assertTrue(
                                            "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "': The difference between the expected decimal and the received decimal exceeds the epsilon. Difference: " + (diff - EPSILON),
                                            diff < EPSILON );
                                } else {
                                    Assert.assertEquals( "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "'", 0, expectedResult.doubleValue() - ((BigDecimal) row[j]).doubleValue(), 0.0 );
                                }
                            } else {
                                Assert.assertEquals(
                                        "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "'",
                                        expectedRow[j],
                                        row[j]
                                );
                            }
                        } else {
                            Assert.assertEquals(
                                    "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "'",
                                    expectedRow[j],
                                    row[j]
                            );
                        }

                    } else {
                        List resultList = (List) row[j];
                        Object[] expectedArray = (Object[]) expectedRow[j];
                        if ( expectedArray == null ) {
                            Assert.assertNull( "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "': ", resultList );
                        } else {
                            for ( int k = 0; k < expectedArray.length; k++ ) {
                                Assert.assertEquals(
                                        "Unexpected data in column '" + rsmd.getColumnName( j + 1 ) + "' at position: " + k + 1,
                                        expectedArray[k],
                                        resultList.get( k ) );
                            }
                        }
                    }
                    j++;
                } else {
                    fail( "More data available then expected." );
                }
            }
        }
        Assert.assertEquals( "Wrong number of rows in the result set", expected.size(), i );
    }


    public static List<Object[]> convertResultSetToList( ResultSet resultSet ) throws SQLException {
        ResultSetMetaData md = resultSet.getMetaData();
        int columns = md.getColumnCount();
        List<Object[]> list = new ArrayList<>();
        while ( resultSet.next() ) {
            Object[] row = new Object[columns];
            for ( int i = 1; i <= columns; ++i ) {
                int columnType = resultSet.getMetaData().getColumnType( i );
                if ( columnType == Types.BINARY ) {
                    row[i - 1] = resultSet.getBytes( i );
                } else if ( columnType != Types.ARRAY ) {
                    if ( resultSet.getObject( i ) != null ) {
                        if ( columnType == Types.FLOAT || columnType == Types.REAL ) {
                            row[i - 1] = resultSet.getFloat( i );
                        } else if ( columnType == Types.DOUBLE ) {
                            row[i - 1] = resultSet.getDouble( i );
                        } else if ( columnType == Types.DECIMAL ) {
                            row[i - 1] = resultSet.getBigDecimal( i );
                        } else {
                            row[i - 1] = resultSet.getObject( i );
                        }
                    } else {
                        row[i - 1] = resultSet.getObject( i );
                    }
                } else {
                    row[i - 1] = Functions.deepArrayToList( resultSet.getArray( i ) );
                }
            }
            list.add( row );
        }
        return list;
    }


    public static List<Object[]> orderResultList( List<Object[]> result ) {
        List<Object[]> list = new ArrayList<>( result );
        list.sort( ( lhs, rhs ) -> {
            String lhsStr = Arrays.toString( lhs );
            String rhsStr = Arrays.toString( rhs );
            // -1 - less than, 1 - greater than, 0 - equal, all inversed for descending
            return lhsStr.compareTo( rhsStr );
        } );
        return list;
    }


    public static abstract class HttpConnection {

        public static HttpRequest<?> buildQuery( String route, String query, String database ) {
            JsonObject data = new JsonObject();
            data.addProperty( "query", query );
            data.addProperty( "database", database );

            return Unirest.post( "{protocol}://{host}:{port}" + route )
                    .header( "Content-ExpressionType", "application/json" )
                    .body( data );

        }


        protected static HttpResponse<String> execute( String prefix, String query, String database ) {
            HttpRequest<?> request = buildQuery( prefix, query, database );
            request.basicAuth( "pa", "" );
            request.routeParam( "protocol", "http" );
            request.routeParam( "host", "127.0.0.1" );
            request.routeParam( "port", "13137" );
            return request.asString();
        }

    }


    public static class MongoConnection extends HttpConnection {

        public static final String MONGO_PREFIX = "/mongo";
        public static final String MONGO_DB = "test";
        static Gson gson = new Gson();


        private MongoConnection() {
        }


        public static boolean checkResultSet( Result result, List<Object[]> expected, boolean containsId ) {
            assertEquals( expected.size(), result.getData().length );

            int j = 0;
            for ( String[] data : result.getData() ) {
                int i = 0;
                for ( String entry : data ) {
                    if ( containsId && !entry.contains( "_id" ) ) {
                        return false;
                    }

                    if ( entry != null && expected.get( j )[i] != null ) {
                        if ( containsId && result.getHeader()[i].dataType.toLowerCase().contains( "document" ) ) {
                            BsonDocument doc = BsonDocument.parse( entry );
                            doc.remove( "_id" );

                            assertEquals( BsonDocument.parse( ((String) expected.get( j )[i]) ), doc );
                        } else {
                            assertEquals( ((String) expected.get( j )[i]).replace( " ", "" ), entry.replace( " ", "" ) );
                        }
                    } else {
                        assertEquals( expected.get( j )[i], entry );
                    }
                    i++;
                }
                j++;
            }

            return true;
        }


        public static Result executeGetResponse( String mongoQl ) {
            return executeGetResponse( mongoQl, MONGO_DB );
        }


        public static Result executeGetResponse( String mongoQl, String database ) {
            return getBody( execute( MONGO_PREFIX, mongoQl, database ) );
        }


        private static Result getBody( HttpResponse<String> res ) {
            try {
                Result[] result = gson.fromJson( res.getBody(), Result[].class );
                if ( result.length == 1 ) {
                    return gson.fromJson( res.getBody(), Result[].class )[0];
                } else if ( result.length == 0 ) {
                    return new Result();
                }
                fail( "There was more than one result in the response!" );
                throw new RuntimeException( "This cannot happen" );

            } catch ( JsonSyntaxException e ) {
                log.warn( "{}\nmessage: {}", res.getBody(), e.getMessage() );
                fail();
                throw new RuntimeException( "This cannot happen" );
            }
        }


        public static boolean checkUnorderedResultSet( Result result, List<String[]> expected, boolean excludeId ) {
            if ( result.getData() == null ) {
                fail( result.getError() );
            }
            assertEquals( expected.size(), result.getData().length );

            List<List<String>> parsedResults = new ArrayList<>();

            for ( String[] data : result.getData() ) {
                int i = 0;
                List<String> row = new ArrayList<>();
                for ( String entry : data ) {
                    if ( !result.getHeader()[i].name.equals( "_id" ) ) {
                        BsonDocument doc = tryGetBson( result, i, entry );
                        if ( doc != null ) {
                            if ( excludeId && result.getHeader()[i].name.equals( "d" ) ) {
                                doc.remove( "_id" );
                            }
                            row.add( doc.toJson().replace( " ", "" ) );
                            i++;
                            continue;
                        }

                        if ( entry != null ) {
                            row.add( entry.replace( " ", "" ) );
                        } else {
                            row.add( null );
                        }
                    }
                    i++;
                }
                parsedResults.add( row );
            }
            List<List<String>> parsedExpected = new ArrayList<>();

            expected.forEach( row -> parsedExpected.add( Arrays.asList( row ) ) );

            List<List<String>> finalExpected = parsedExpected
                    .stream()
                    .map(
                            list -> list
                                    .stream()
                                    .map( e -> {
                                        if ( e != null ) {
                                            return e.replace( " ", "" );
                                        } else {
                                            return null;
                                        }
                                    } )
                                    .collect( Collectors.toList() ) )
                    .collect( Collectors.toList() );

            assertTrue( "Expected result does not contain all actual results", finalExpected.containsAll( parsedResults ) );
            assertTrue( "Actual result does not contain all expected results", parsedResults.containsAll( finalExpected ) );
            return true;
        }


        private static BsonDocument tryGetBson( Result result, int i, String entry ) {
            BsonDocument doc = null;
            if ( result.getHeader()[i].dataType.toLowerCase().contains( "document" ) ) {
                doc = BsonDocument.parse( entry );
            } else if ( result.getHeader()[i].dataType.toLowerCase().contains( "any" ) ) {
                try {
                    doc = BsonDocument.parse( entry );
                } catch ( Exception e ) {
                    // empty on purpose
                }
            }

            return doc;
        }


        public static String toDoc( String key, Object value ) {
            return String.format( "{\"%s\": %s}", key, value );
        }

    }


    public static class CypherConnection extends HttpConnection {

        static Gson gson = new Gson();


        public static Result executeGetResponse( String query ) {
            return getBody( execute( "/cypher", query, "test" ) );
        }


        public static Result executeGetResponse( String query, String database ) {
            return getBody( execute( "/cypher", query, database ) );
        }


        private static Result getBody( HttpResponse<String> res ) {
            try {
                Result[] result = gson.fromJson( res.getBody(), Result[].class );
                if ( result.length == 1 ) {
                    return gson.fromJson( res.getBody(), Result[].class )[0];
                } else if ( result.length == 0 ) {
                    return new Result();
                }
                fail( "There was more than one result in the response!" );
                throw new RuntimeException( "This cannot happen" );

            } catch ( JsonSyntaxException e ) {
                log.warn( "{}\nmessage: {}", res.getBody(), e.getMessage() );
                fail();
                throw new RuntimeException( "This cannot happen" );
            }
        }

    }


    public static class JdbcConnection implements AutoCloseable {

        private final static String dbHost = "localhost";
        private final static int port = 20591;

        private final Connection conn;


        public JdbcConnection( boolean autoCommit ) throws SQLException {
            try {
                Class.forName( "org.polypheny.jdbc.Driver" );
            } catch ( ClassNotFoundException e ) {
                log.error( "Polypheny JDBC Driver not found", e );
            }
            final String url = "jdbc:polypheny:http://" + dbHost + ":" + port;
            log.debug( "Connecting to database @ {}", url );

            Properties props = new Properties();
            props.setProperty( "user", "pa" );
            props.setProperty( "serialization", "PROTOBUF" );

            conn = DriverManager.getConnection( url, props );
            conn.setAutoCommit( autoCommit );
        }


        public Connection getConnection() {
            return conn;
        }


        @Override
        public void close() throws SQLException {
            conn.commit();
            conn.close();
        }

    }

}
