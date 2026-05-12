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

package org.polypheny.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import kong.unirest.HttpRequest;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.jupiter.api.AfterAll;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.QueryPlanCache;
import org.polypheny.db.processing.caching.RoutingPlanCache;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.models.results.DocResult;
import org.polypheny.db.webui.models.results.GraphResult;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.requests.UIRequest;


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
            PolyphenyDb.mode = RunMode.TEST;
            String defaultStoreName = System.getProperty( "store.default" );
            if ( defaultStoreName != null ) {
                polyphenyDb.defaultStoreName = defaultStoreName;
            }
            // polyphenyDb.resetCatalog = true;
            polyphenyDb.runPolyphenyDb();
        };
        Thread thread = new Thread( runnable );
        thread.start();

        // Wait until Polypheny-DB is ready to process queries
        int i = 0;
        while ( !polyphenyDb.isReady() ) {
            try {
                TimeUnit.SECONDS.sleep( 1 );
                if ( i++ > 300 ) {
                    if ( thread.getStackTrace().length > 0 ) {
                        System.err.println( "Stacktrace of Polypheny-DB thread:" );
                        for ( int j = 0; j < thread.getStackTrace().length; j++ ) {
                            System.err.println( "\tat " + thread.getStackTrace()[j] );
                        }
                    }
                    throw new RuntimeException( "There seems to be an issue with Polypheny-DB. Waited 5 minutes for Polypheny-DB to get ready. Aborting tests." );
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


    public static PolyValue toPolyValue( Object value ) {

        if ( value instanceof Integer ) {
            return PolyInteger.of( (Integer) value );
        } else if ( value instanceof Long ) {
            return PolyLong.of( (Long) value );
        } else if ( value instanceof Float ) {
            return PolyFloat.of( (Float) value );
        } else if ( value instanceof Double ) {
            return PolyDouble.of( (Double) value );
        } else if ( value instanceof String ) {
            return PolyString.of( (String) value );
        } else if ( value instanceof Collection ) {
            return PolyList.of( ((List<?>) value).stream().map( TestHelper::toPolyValue ).toList() );
        }

        throw new NotImplementedException();
    }


    public static void checkResultSetWithDelay( int tries, int waitSeconds, DelayedSupplier<ResultSet> resultSet, ImmutableList<Object[]> expected ) {
        checkResultSetWithDelay( tries, waitSeconds, resultSet, expected, false );
    }


    public static void checkResultSetWithDelay( int tries, int waitSeconds, DelayedSupplier<ResultSet> resultSet, ImmutableList<Object[]> expected, boolean ignoreOrder ) {
        try {
            TimeUnit.SECONDS.sleep( waitSeconds );
            try {
                checkResultSet( resultSet.get(), expected, ignoreOrder );
            } catch ( Throwable e ) {
                if ( tries > 0 ) {
                    checkResultSetWithDelay( tries - 1, waitSeconds, resultSet, expected, ignoreOrder );
                } else {
                    throw new RuntimeException( e );
                }
            }
        } catch ( InterruptedException interruptedException ) {
            log.error( "Interrupted exception", interruptedException );
        }
    }


    public Transaction getTransaction() {
        return transactionManager.startTransaction( Catalog.defaultUserId, new QueryAnalyzer(), "Test Helper" );
    }


    @AfterAll
    public static void tearDown() {
        //LOG.info( "shutdown - closing DB connection" );
    }


    public static void addHsqldb( String name, Statement statement ) throws SQLException {
        executeSQL( statement, "ALTER ADAPTERS ADD \"" + name + "\" USING 'Hsqldb' AS 'Store'"
                + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
    }


    public static void addCsv( String name, Statement statement ) throws SQLException {
        executeSQL( statement, "ALTER ADAPTERS ADD \"" + name + "\" USING 'Csv' AS 'Store'"
                + " WITH '{}'" );
    }


    public static void dropAdapter( String name, Statement statement ) throws SQLException {
        executeSQL( statement, "ALTER ADAPTERS DROP \"" + name + "\"" );
    }


    public static void addPostgresSource( String name, String host, int port, String database, String username, String password, String table ) throws SQLException {
        executeSQL(
                "ALTER ADAPTERS ADD \"" + name + "\" USING 'PostgreSQL' AS 'Source' WITH "
                        + "'{"
                        + "\"mode\":\"remote\","
                        + "\"host\":\"" + host + "\","
                        + "\"port\":\"" + port + "\","
                        + "\"database\":\"" + database + "\","
                        + "\"username\":\"" + username + "\","
                        + "\"password\":\"" + password + "\","
                        + "\"maxConnections\":\"25\","
                        + "\"transactionIsolation\":\"SERIALIZABLE\","
                        + "\"tables\":\"" + table + "\""
                        + "}'" );
    }


    public static void addPostgresSource( String name, String host, int port, String database, String username, String password ) throws SQLException {
        addPostgresSource( name, host, port, database, username, password, "" );
    }


    public static void addMysqlSource( String name, String host, int port, String database, String username, String password, String table ) throws SQLException {
        executeSQL(
                "ALTER ADAPTERS ADD \"" + name + "\" USING 'MySQL' AS 'Source' WITH "
                        + "'{"
                        + "\"mode\":\"remote\","
                        + "\"host\":\"" + host + "\","
                        + "\"port\":\"" + port + "\","
                        + "\"database\":\"" + database + "\","
                        + "\"username\":\"" + username + "\","
                        + "\"password\":\"" + password + "\","
                        + "\"maxConnections\":\"25\","
                        + "\"transactionIsolation\":\"SERIALIZABLE\","
                        + "\"tables\":\"" + table + "\""
                        + "}'" );
    }


    public static void executeSQL( Statement statement, String sql ) throws SQLException {
        statement.execute( sql );
    }


    public static void executeSQL( String sql ) throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            try ( Statement statement = jdbcConnection.getConnection().createStatement() ) {
                statement.execute( sql );
            }
        }
    }


    public static LogicalTable awaitLogicalTable( long namespaceId, String tableName, int timeoutSeconds ) {
        for ( int i = 0; i < timeoutSeconds; i++ ) {
            var table = Catalog.snapshot().rel().getTable( namespaceId, tableName );
            if ( table.isPresent() ) {
                return table.orElseThrow();
            }
            try {
                TimeUnit.SECONDS.sleep( 1 );
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "Interrupted while waiting for table " + tableName, e );
            }
        }
        throw new IllegalStateException( "Table was not created in time: " + tableName );
    }


    public static void awaitLogicalTableAbsent( long namespaceId, String tableName, int timeoutSeconds ) {
        for ( int i = 0; i < timeoutSeconds; i++ ) {
            if ( Catalog.snapshot().rel().getTable( namespaceId, tableName ).isEmpty() ) {
                return;
            }
            try {
                TimeUnit.SECONDS.sleep( 1 );
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "Interrupted while waiting for table removal " + tableName, e );
            }
        }
        throw new IllegalStateException( "Table still exists after refresh: " + tableName );
    }


    public static long awaitSourceAdapterId( String adapterName, int timeoutSeconds ) {
        for ( int i = 0; i < timeoutSeconds; i++ ) {
            LogicalAdapter adapter = Catalog.snapshot().getAdapter( adapterName ).orElse( null );
            if ( adapter != null ) {
                return adapter.id;
            }
            try {
                TimeUnit.SECONDS.sleep( 1 );
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
                throw new RuntimeException( "Interrupted while waiting for source adapter " + adapterName, e );
            }
        }
        throw new IllegalStateException( "Source adapter was not created in time: " + adapterName );
    }


    public static List<String> getCatalogColumnNames( long entityId ) {

        return Catalog.snapshot().rel().getColumns( entityId ).stream().map( c -> c.name ).toList();
    }


    /**
     * Surprisingly often when testing the used ids are in a similar range and quite low, which can result in unexpected behaviour,
     * where tests seem to work but shouldn't.
     */
    public void randomizeCatalogIds() {
        Random random = new Random();
        int max = 200;
        Supplier<Integer> offset = () -> random.nextInt( max );

        try {
            PolyCatalog catalog = (PolyCatalog) Catalog.getInstance();
            Field field = catalog.getClass().getDeclaredField( "idBuilder" );
            field.setAccessible( true );
            field.set( catalog, new IdBuilder(
                    new AtomicLong( catalog.idBuilder.getSnapshotId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getEntityId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getFieldId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getUserId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getAllocId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getPhysicalId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getIndexId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getKeyId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getAdapterId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getInterfaceId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getConstraintId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getGroupId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getPartitionId().longValue() + offset.get() ),
                    new AtomicLong( catalog.idBuilder.getPlacementId().longValue() + offset.get() )
            ) );
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new RuntimeException( e );
        }
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
            assertTrue( i < expected.size(), "Result set has more rows than expected" );
            Object[] expectedRow = expected.get( i++ );
            assertEquals( expectedRow.length, rsmd.getColumnCount(), "Wrong number of columns" );
            int j = 0;
            while ( j < expectedRow.length ) {
                if ( expectedRow.length >= j + 1 ) {
                    int columnType = rsmd.getColumnType( j + 1 ); // this leads to errors if expected is different aka expected is decimal and actual is integer
                    String columnName = rsmd.getColumnName( j + 1 );

                    checkValue( isConvertingDecimals, row[j], expectedRow[j], columnType, columnName );
                    j++;
                } else {
                    fail( "More data available then expected." );
                }
            }
        }
        assertEquals( expected.size(), i, "Wrong number of rows in the result set" );
    }


    private static void checkValue( boolean isConvertingDecimals, Object actualValue, Object expectedValue, int columnType, String columnName ) {
        if ( columnType != Types.ARRAY ) {
            checkScalar( columnName, actualValue, expectedValue, isConvertingDecimals, columnType );
        } else {
            checkArray( columnName, actualValue, expectedValue, isConvertingDecimals, columnType );
        }
    }


    private static void checkScalar( String columnName, Object actualValue, Object expectedValue, boolean isConvertingDecimals, int columnType ) {
        if ( expectedValue == null ) {
            assertNull( actualValue, "Unexpected data in column '%s'".formatted( columnName ) );
        } else if ( columnType == Types.BINARY ) {
            assertEquals(
                    new String( (byte[]) expectedValue ),
                    new String( (byte[]) actualValue ),
                    "Unexpected data in column '%s'".formatted( columnName ) );
        } else if ( columnType == Types.FLOAT || columnType == Types.REAL ) {
            float diff = Math.abs( (float) expectedValue - (float) actualValue );
            assertTrue( diff < EPSILON,
                    "Unexpected data in column '%s': The difference between the expected float and the received float exceeds the epsilon. Difference: %s".formatted( columnName, diff - EPSILON ) );
        } else if ( columnType == Types.DOUBLE ) {
            double diff = Math.abs( (double) expectedValue - (double) actualValue );
            assertTrue( diff < EPSILON,
                    "Unexpected data in column '%s': The difference between the expected double and the received double exceeds the epsilon. Difference: %s".formatted( columnName, diff - EPSILON ) );
        } else if ( columnType == Types.DECIMAL || (expectedValue instanceof Float || expectedValue instanceof Double) ) { // Decimals are exact // but not for calculations?
            BigDecimal expectedResult = new BigDecimal( expectedValue.toString() );
            BigDecimal actualResult = new BigDecimal( actualValue.toString() );
            double diff = Math.abs( expectedResult.doubleValue() - actualResult.doubleValue() );
            if ( isConvertingDecimals ) {
                assertTrue( diff < EPSILON,
                        "Unexpected data in column '%s': The difference between the expected decimal and the received decimal exceeds the epsilon. Difference: %s".formatted( columnName, diff - EPSILON ) );
            } else {
                assertEquals( 0, expectedResult.doubleValue() - actualResult.doubleValue(), 0.0, "Unexpected data in column '%s'".formatted( columnName ) );
            }
        } else if ( expectedValue instanceof Number expectedNumber && actualValue instanceof Number actualNumber ) {
            assertEquals( expectedNumber.longValue(), actualNumber.longValue(), "Unexpected data in column '%s'".formatted( columnName ) );
        } else if ( expectedValue instanceof List<?> expectedList && actualValue instanceof List<?> actualList
                && (columnType == Types.ARRAY || columnType == Types.OTHER) ) {
            for ( int i = 0; i < expectedList.size(); i++ ) {
                checkValue( isConvertingDecimals, actualList.get( i ), expectedList.get( i ), Types.OTHER, columnName );
            }
        } else {
            assertEquals(
                    expectedValue,
                    actualValue,
                    "Unexpected data in column '%s'".formatted( columnName )
            );
        }

    }


    private static void checkArray( String columnName, Object actualValue, Object expectedValue, boolean isConvertingDecimals, int columnType ) {
        List<?> resultList = (List<?>) actualValue;

        if ( expectedValue == null ) {
            assertNull( resultList, "Unexpected data in column '%s'".formatted( columnName ) );
            return;
        }

        List<?> expectedArray = toList( (Object[]) expectedValue );

        for ( int k = 0; k < expectedArray.size(); k++ ) {
            checkValue( isConvertingDecimals, resultList.get( k ), expectedArray.get( k ), Types.OTHER, columnName ); // we have rather unspecific component types for arrays
        }
    }


    private static List<Object> toList( Object[] objects ) {
        List<Object> list = new ArrayList<>();
        for ( Object object : objects ) {
            if ( object instanceof Object[] ) {
                list.add( toList( (Object[]) object ) );
            } else {
                list.add( object );
            }
        }
        return list;
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
                    row[i - 1] = Functions.deepArrayToList( (Array) resultSet.getObject( i ) );
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


    public void resetCaches() {
        ImplementationCache.INSTANCE.reset();
        QueryPlanCache.INSTANCE.reset();
        RoutingPlanCache.INSTANCE.reset();
        RoutingManager.getInstance().getRouters().forEach( Router::resetCaches );
    }


    public void checkAllTrxClosed() {
        checkTrxStatus( 0 );
    }


    public void checkTrxStatus( int expected ) {
        long count = transactionManager.getNumberOfActiveTransactions();
        if ( count != expected ) {
            fail( "There are still " + count + " active transactions, while there should be " + expected );
            throw new RuntimeException( "There are still " + count + " active transactions, while there should be " + expected );
        }
    }


    public boolean storeSupportsIndex() {
        return !AdapterManager.getInstance().getStore( "hsqldb" ).orElseThrow().getAvailableIndexMethods().isEmpty();
    }


    public static abstract class HttpConnection {

        public static HttpRequest<?> buildQuery( String route, String query, String database ) {
            JsonObject data = new JsonObject();
            data.addProperty( "query", query );
            data.addProperty( "namespace", database );

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


        private MongoConnection() {
        }


        public static DocResult executeGetResponse( String mongoQl ) {
            return executeGetResponse( mongoQl, MONGO_DB );
        }


        public static DocResult executeGetResponse( String mongoQl, String database ) {
            return getBody( execute( MONGO_PREFIX, mongoQl, database ) );
        }


        private static DocResult getBody( HttpResponse<String> res ) {
            try {
                DocResult[] result = HttpServer.mapper.readValue( res.getBody(), DocResult[].class );
                if ( result.length == 1 ) {
                    if ( result[0].error != null ) {
                        throw new RuntimeException( result[0].error );
                    }
                    return result[0];
                } else if ( result.length == 0 ) {
                    return DocResult.builder().build();
                }
                return result[result.length - 1];

            } catch ( JsonSyntaxException | JsonProcessingException e ) {
                log.warn( "{}\nmessage: {}", res.getBody(), e.getMessage() );
                fail();
                throw new RuntimeException( "This cannot happen" );
            }
        }


        public static boolean checkDocResultSet( DocResult result, List<String> expected, boolean excludeId, boolean unordered ) {
            if ( result.getData() == null ) {
                fail( result.error );
            }
            assertEquals( expected.size(), result.getData().length );

            List<BsonValue> parsedResults = new ArrayList<>();

            for ( String data : result.getData() ) {

                BsonDocument doc = tryGetBson( data );
                if ( doc != null ) {

                    if ( excludeId && !doc.containsKey( DocumentType.DOCUMENT_ID ) ) {
                        fail();
                        throw new RuntimeException( "Should contain " + DocumentType.DOCUMENT_ID + " field." );
                    }
                    if ( excludeId ) {
                        doc.remove( DocumentType.DOCUMENT_ID );
                    }

                }
                parsedResults.add( doc );
            }
            List<BsonValue> parsedExpected = expected.stream().map( e -> e != null ? (BsonValue) BsonDocument.parse( e ) : null ).toList();

            if ( unordered ) {
                assertTrue( areDocumentEqual( parsedExpected, parsedResults ),
                        "Expected result does not contain all actual results: \nexpected: \n" + new BsonArray( parsedExpected ) + "\nactual: \n" + new BsonArray( parsedResults ) );
                assertTrue( areDocumentEqual( parsedResults, parsedExpected ),
                        "Actual result does not contain all expected results: \nexpected: \n" + new BsonArray( parsedExpected ) + "\nactual: \n" + new BsonArray( parsedResults ) );
            } else {
                List<Pair<BsonValue, BsonValue>> wrong = new ArrayList<>();
                for ( Pair<BsonValue, BsonValue> pair : Pair.zip( parsedExpected, parsedResults ) ) {
                    if ( !Objects.equals( pair.left, pair.right ) ) {
                        wrong.add( pair );
                    }
                }

                assertTrue( wrong.isEmpty(), "Expected and actual result do not contain the same element or order: \n"
                        + "expected: " + wrong.stream().map( p -> p.left.toString()
                        + " != "
                        + "actual: " + (p.right == null ? null : p.right.toString()) ).collect( Collectors.joining( ", \n" ) ) );
            }

            return true;
        }


        /**
         * Checks if all elements of parsedExpected are in parsedResults
         * This is needed because the order of the elements in the result is not guaranteed
         * The document model does not guarantee specific types like 8.0 and 8 are treated as equal
         *
         * @param parsedExpected list of expected documents
         * @param parsedResults list of actual documents
         * @return true if all elements of parsedExpected are in parsedResults and vice versa
         */
        private static boolean areDocumentEqual( List<BsonValue> parsedExpected, List<BsonValue> parsedResults ) {
            for ( BsonValue bsonValue : parsedExpected ) {
                if ( parsedResults.contains( bsonValue ) ) {
                    continue;
                }
                boolean found = false;
                for ( BsonValue parsedResult : parsedResults ) {
                    if ( areValueEqual( bsonValue, parsedResult ) ) {
                        found = true;
                        break;
                    }
                }
                if ( !found ) {
                    return false;
                }
            }
            return true;
        }


        private static boolean areValueEqual( BsonValue bsonValue, BsonValue parsedResult ) {
            if ( bsonValue.equals( parsedResult ) ) {
                return true;
            }
            if ( bsonValue.isDocument() && parsedResult.isDocument() ) {
                BsonDocument bsonDocument = bsonValue.asDocument();
                BsonDocument parsedDocument = parsedResult.asDocument();
                for ( String key : bsonDocument.keySet() ) {
                    if ( !parsedDocument.containsKey( key ) ) {
                        return false;
                    }
                    if ( !areValueEqual( bsonDocument.get( key ), parsedDocument.get( key ) ) ) {
                        return false;
                    }
                }
                return true;
            } else if ( bsonValue.isArray() && parsedResult.isArray() ) {
                BsonArray bsonArray = bsonValue.asArray();
                BsonArray parsedArray = parsedResult.asArray();
                for ( int i = 0; i < bsonArray.size(); i++ ) {
                    if ( !areValueEqual( bsonArray.get( i ), parsedArray.get( i ) ) ) {
                        return false;
                    }
                }
                return true;
            } else if ( bsonValue.isNumber() && parsedResult.isNumber() ) {
                return bsonValue.asNumber().doubleValue() == parsedResult.asNumber().doubleValue();
            }
            return false;
        }


        private static BsonDocument tryGetBson( String entry ) {
            BsonDocument doc = null;
            try {
                doc = BsonDocument.parse( entry );
            } catch ( Exception e ) {
                // empty on purpose
            }

            return doc;
        }


        public static String toDoc( String key, Object value ) {
            return String.format( "{\"%s\": %s}", key, value );
        }


        public static List<String> arrayToDoc( List<Object[]> values, String... names ) {
            List<String> docs = new ArrayList<>();
            for ( Object[] doc : values ) {
                docs.add( "{" +
                        Pair.zip( Arrays.asList( names ), Arrays.asList( doc ) )
                                .stream()
                                .map( p -> "\"" + p.left + "\"" + ":" +
                                        ((p.right != null
                                                ? (p.right instanceof String && !((String) p.right).startsWith( "{" ) && !((String) p.right).endsWith( "}" ) // special handling for string and document
                                                ? "\"" + p.right + "\""
                                                : p.right.toString())
                                                : null)) )
                                .collect( Collectors.joining( "," ) )
                        + "}" );
            }
            return docs;
        }

    }


    public static class CypherConnection extends HttpConnection {


        public static GraphResult executeGetResponse( String query ) {
            return getBody( execute( "/cypher", query, "test" ) );
        }


        public static GraphResult executeGetResponse( String query, String database ) {
            return getBody( execute( "/cypher", query, database ) );
        }


        private static GraphResult getBody( HttpResponse<String> res ) {
            try {
                GraphResult[] result = HttpServer.mapper.readValue( res.getBody(), GraphResult[].class );
                if ( result.length == 1 ) {
                    return HttpServer.mapper.readValue( res.getBody(), GraphResult[].class )[0];
                } else if ( result.length == 0 ) {
                    return GraphResult.builder().build();
                }
                fail( "There was more than one result in the response!" );
                throw new RuntimeException( "This cannot happen" );

            } catch ( JsonSyntaxException | JsonProcessingException e ) {
                log.warn( "{}\nmessage: {}", res.getBody(), e.getMessage() );
                fail();
                throw new RuntimeException( "This cannot happen" );
            }
        }

    }


    public static RelationalResult sendRefreshRequest( long entityId ) throws Exception {
        Object httpServer = HttpServer.getInstance();
        Field crudField = httpServer.getClass().getDeclaredField( "crud" );
        crudField.setAccessible( true );
        Object crud = crudField.get( httpServer );

        UIRequest request = UIRequest.builder()
                .type( "RefreshRequest" )
                .entityId( entityId )
                .namespace( Catalog.DEFAULT_NAMESPACE_NAME )
                .currentPage( 1 )
                .noLimit( false )
                .build();

        Method refreshMethod = crud.getClass().getMethod( "refreshSourceSchemaIfNeeded", UIRequest.class );
        refreshMethod.invoke( crud, request );

        Method getTableMethod = crud.getClass().getDeclaredMethod( "getTable", UIRequest.class );
        getTableMethod.setAccessible( true );
        return (RelationalResult) getTableMethod.invoke( crud, request );
    }


    @SuppressWarnings("unchecked")
    public static List<String> refreshSelectedSources( List<Long> sourceIds ) throws Exception {
        Object httpServer = HttpServer.getInstance();
        Field crudField = httpServer.getClass().getDeclaredField( "crud" );
        crudField.setAccessible( true );
        Object crud = crudField.get( httpServer );

        Method refreshMethod = crud.getClass().getMethod( "refreshSelectedSources", List.class );
        return (List<String>) refreshMethod.invoke( crud, sourceIds );
    }


    public static boolean isDockerDaemonAvailable() {
        try {
            Process process = new ProcessBuilder( "docker", "info" ).redirectErrorStream( true ).start();
            boolean finished = process.waitFor( 15, TimeUnit.SECONDS );
            return finished && process.exitValue() == 0;
        } catch ( Exception e ) {
            return false;
        }
    }


    public static boolean isLinuxDockerDaemonAvailable() {
        if ( !isDockerDaemonAvailable() ) {
            return false;
        }
        try {
            Process process = new ProcessBuilder( "docker", "info", "--format", "{{.OSType}}" ).redirectErrorStream( true ).start();
            boolean finished = process.waitFor( 15, TimeUnit.SECONDS );
            if ( !finished || process.exitValue() != 0 ) {
                return false;
            }
            String output = new String( process.getInputStream().readAllBytes() ).trim();
            return "linux".equalsIgnoreCase( output );
        } catch ( Exception e ) {
            return false;
        }
    }


    public static DockerPostgres startPostgresDocker( String database, String username, String password ) throws Exception {
        return DockerPostgres.start( database, username, password );
    }


    public static DockerMysql startMysqlDocker( String database, String username, String password ) throws Exception {
        return DockerMysql.start( database, username, password );
    }


    @Getter
    public static class JdbcConnection implements AutoCloseable {

        private final static String dbHost = "localhost";
        private final static int port = 20590;

        private final Connection conn;


        public JdbcConnection( boolean autoCommit, boolean strictMode ) throws SQLException {
            try {
                Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
            } catch ( ClassNotFoundException e ) {
                log.error( "Polypheny JDBC Driver not found", e );
            }
            final String url = "jdbc:polypheny://" + dbHost + ":" + port + "/?strict=" + strictMode;
            log.debug( "Connecting to database @ {}", url );

            conn = DriverManager.getConnection( url, "pa", "" );
            conn.setAutoCommit( autoCommit );
        }


        public JdbcConnection( boolean autoCommit ) throws SQLException {
            try {
                Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );
            } catch ( ClassNotFoundException e ) {
                log.error( "Polypheny JDBC Driver not found", e );
            }
            final String url = "jdbc:polypheny://" + dbHost + ":" + port + "/?strict=false";
            log.debug( "Connecting to database @ {}", url );

            conn = DriverManager.getConnection( url, "pa", "" );
            conn.setAutoCommit( autoCommit );
        }


        public Connection getConnection() {
            return conn;
        }


        @Override
        public void close() throws SQLException {
            if ( conn.isClosed() ) {
                return;
            }
            if ( !conn.getAutoCommit() ) {
                conn.commit();
            }
            conn.close();
        }

    }


    @SafeVarargs
    public static void executeSql( SqlBiConsumer<Connection, Statement>... queries ) {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                for ( BiConsumer<Connection, Statement> query : queries ) {
                    query.accept( connection, statement );
                }
            }
        } catch ( SQLException e ) {
            fail( e.getMessage() );
            throw new RuntimeException( e );
        }
    }


    @FunctionalInterface
    public interface SqlBiConsumer<C, T> extends BiConsumer<C, T> {

        @Override
        default void accept( final C elemC, final T elemT ) {
            try {
                acceptThrows( elemC, elemT );
            } catch ( final SQLException e ) {
                throw new RuntimeException( e );
            }
        }

        void acceptThrows( C elemC, T elem ) throws SQLException;

    }


    @FunctionalInterface
    public interface DelayedSupplier<T extends ResultSet> extends Supplier<T> {

        @Override
        default T get() {
            try {
                return getThrows();
            } catch ( final SQLException e ) {
                throw new RuntimeException( e );
            }
        }

        T getThrows() throws SQLException;

    }


    public static final class DockerPostgres implements AutoCloseable {

        private static final int POSTGRES_PORT = 5432;
        private static final long STARTUP_TIMEOUT_MS = TimeUnit.SECONDS.toMillis( 60 );

        private final DockerContainer container;
        @Getter
        private final String host;
        @Getter
        private final int port;
        private final String database;
        private final String username;
        private final String password;


        private DockerPostgres( DockerContainer container, String host, int port, String database, String username, String password ) {
            this.container = container;
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
        }


        public static DockerPostgres start( String database, String username, String password ) throws Exception {
            String containerName = "polypheny-refresh-test-" + UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
            DockerInstance instance = DockerManager.getInstance()
                    .getInstanceById( 0 )
                    .orElseThrow( () -> new IllegalStateException( "No docker instance with id 0" ) );

            DockerContainer container = instance.newBuilder( "postgres:16-alpine", containerName )
                    .withEnvironmentVariable( "POSTGRES_DB", database )
                    .withEnvironmentVariable( "POSTGRES_USER", username )
                    .withEnvironmentVariable( "POSTGRES_PASSWORD", password )
                    .createAndStart();

            try {
                HostAndPort connection = container.connectToContainer( POSTGRES_PORT );
                String host = connection.host();
                int port = connection.port();
                DockerPostgres postgres = new DockerPostgres( container, host, port, database, username, password );
                if ( !container.waitTillStarted( postgres::testConnection, STARTUP_TIMEOUT_MS ) ) {
                    throw new IllegalStateException( "PostgreSQL container did not become ready in time" );
                }
                return postgres;
            } catch ( Exception e ) {
                container.destroy();
                throw e;
            }
        }


        public void execute( String sql ) throws Exception {
            int exitCode = container.execute( List.of( "psql", "-U", username, "-d", database, "-c", sql ) );
            if ( exitCode != 0 ) {
                throw new IllegalStateException( "PostgreSQL command failed with exit code " + exitCode + ": " + sql );
            }
        }


        @Override
        public void close() {
            try {
                container.destroy();
            } catch ( Exception ignored ) {
                // Ignore cleanup failures to avoid masking test failures.
            }
        }


        private boolean testConnection() {
            try {
                return container.execute( List.of( "psql", "-U", username, "-d", database, "-c", "SELECT 1" ) ) == 0;
            } catch ( IOException e ) {
                // Ignore during startup polling.
            }
            return false;
        }

    }


    public static final class DockerMysql implements AutoCloseable {

        private static final int MYSQL_PORT = 3306;
        private static final long STARTUP_TIMEOUT_MS = TimeUnit.SECONDS.toMillis( 60 );
        private static final String ROOT_PASSWORD = "polypheny-root";

        private final DockerContainer container;
        @Getter
        private final String host;
        @Getter
        private final int port;
        private final String database;
        private final String username;
        private final String password;


        private DockerMysql( DockerContainer container, String host, int port, String database, String username, String password ) {
            this.container = container;
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
        }


        public static DockerMysql start( String database, String username, String password ) throws Exception {
            String containerName = "polypheny-refresh-mysql-test-" + UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
            DockerInstance instance = DockerManager.getInstance()
                    .getInstanceById( 0 )
                    .orElseThrow( () -> new IllegalStateException( "No docker instance with id 0" ) );

            DockerContainer container = instance.newBuilder( "mysql:8.4", containerName )
                    .withEnvironmentVariable( "MYSQL_DATABASE", database )
                    .withEnvironmentVariable( "MYSQL_USER", username )
                    .withEnvironmentVariable( "MYSQL_PASSWORD", password )
                    .withEnvironmentVariable( "MYSQL_ROOT_PASSWORD", ROOT_PASSWORD )
                    .createAndStart();

            try {
                HostAndPort connection = container.connectToContainer( MYSQL_PORT );
                String host = connection.host();
                int port = connection.port();
                DockerMysql mysql = new DockerMysql( container, host, port, database, username, password );
                if ( !container.waitTillStarted( mysql::testConnection, STARTUP_TIMEOUT_MS ) ) {
                    throw new IllegalStateException( "MySQL container did not become ready in time" );
                }
                return mysql;
            } catch ( Exception e ) {
                container.destroy();
                throw e;
            }
        }


        public void execute( String sql ) throws Exception {
            int exitCode = container.execute( List.of( "mysql", "-u", username, "-p" + password, database, "-e", sql ) );
            if ( exitCode != 0 ) {
                throw new IllegalStateException( "MySQL command failed with exit code " + exitCode + ": " + sql );
            }
        }


        @Override
        public void close() {
            try {
                container.destroy();
            } catch ( Exception ignored ) {
                // Ignore cleanup failures to avoid masking test failures.
            }
        }


        private boolean testConnection() {
            try {
                return container.execute( List.of(
                        "mysql",
                        "-u",
                        username,
                        "-p" + password,
                        database,
                        "-e",
                        "CREATE TABLE IF NOT EXISTS __polypheny_ready_check (id INT); DROP TABLE __polypheny_ready_check" ) ) == 0;
            } catch ( IOException e ) {
                // Ignore during startup polling.
            }
            return false;
        }

    }

}
