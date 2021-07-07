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

package org.polypheny.db;


import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.runtime.SqlFunctions;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;


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
            return transactionManager.startTransaction( "pa", "APP", true, "Test Helper" );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    @AfterClass
    public static void tearDown() {
        //LOG.info( "shutdown - closing DB connection" );
    }


    public static void checkResultSet( ResultSet resultSet, List<Object[]> expected ) throws SQLException {
        int i = 0;
        while ( resultSet.next() ) {
            Assert.assertTrue( "Result set has more rows than expected", i < expected.size() );
            Object[] expectedRow = expected.get( i++ );
            Assert.assertEquals( "Wrong number of columns", expectedRow.length, resultSet.getMetaData().getColumnCount() );
            int j = 0;
            while ( j < expectedRow.length ) {
                if ( expectedRow.length >= j + 1 ) {
                    int columnType = resultSet.getMetaData().getColumnType( j + 1 );
                    if ( resultSet.getMetaData().getColumnType( j + 1 ) == Types.BINARY ) {
                        if ( expectedRow[j] == null ) {
                            Assert.assertNull( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "': ", resultSet.getBytes( j + 1 ) );
                        } else {
                            Assert.assertEquals( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "'",
                                    new String( (byte[]) expectedRow[j] ),
                                    new String( resultSet.getBytes( j + 1 ) ) );
                        }
                    } else if ( columnType != Types.ARRAY ) {
                        if ( expectedRow[j] != null ) {
                            if ( columnType == Types.FLOAT ) {
                                Assert.assertTrue(
                                        "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "': The difference between the expected float and the received float exceeds the epsilon.",
                                        Math.abs( (float) expectedRow[j] - resultSet.getFloat( j + 1 ) ) < EPSILON );
                            } else if ( columnType == Types.DOUBLE ) {
                                Assert.assertTrue(
                                        "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "': The difference between the expected double and the received double exceeds the epsilon.",
                                        Math.abs( (double) expectedRow[j] - resultSet.getDouble( j + 1 ) ) < EPSILON );
                            } else if ( columnType == Types.DECIMAL ) { // Decimals are exact // but not for calculations?
                                BigDecimal expectedResult = (BigDecimal) expectedRow[j];
                                BigDecimal result = resultSet.getBigDecimal( j + 1 );
                                Assert.assertEquals( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "'", 0, expectedResult.doubleValue() - result.doubleValue(), 0.0 );
                            }
                        } else {
                            Assert.assertEquals(
                                    "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "'",
                                    expectedRow[j],
                                    resultSet.getObject( j + 1 )
                            );
                        }

                    } else {
                        List resultList = SqlFunctions.deepArrayToList( resultSet.getArray( j + 1 ) );
                        Object[] expectedArray = (Object[]) expectedRow[j];
                        if ( expectedArray == null ) {
                            Assert.assertNull( "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "': ", resultList );
                        } else {
                            for ( int k = 0; k < expectedArray.length; k++ ) {
                                Assert.assertEquals(
                                        "Unexpected data in column '" + resultSet.getMetaData().getColumnName( j + 1 ) + "' at position: " + k + 1,
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
