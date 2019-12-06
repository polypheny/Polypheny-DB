/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;


@Slf4j
public class TestHelper {

    private static final TestHelper INSTANCE = new TestHelper();

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
                polyphenyDb.runPolyphenyDb();
            } catch ( GenericCatalogException e ) {
                log.error( "Exception while starting Polypheny-DB", e );
            }
        };
        Thread thread = new Thread( runnable );
        thread.start();

        // TODO MV: Waiting is not a good solution. It would be better to recognize if the system is ready.
        // Wait 25 seconds
        try {
            TimeUnit.SECONDS.sleep( 25 );
        } catch ( InterruptedException e ) {
            // Ignore
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
            return transactionManager.startTransaction( "pa", "APP", true );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    @AfterClass
    public static void tearDown() {
        //LOG.info( "shutdown - closing DB connection" );
    }


    public static class JdbcConnection implements AutoCloseable {

        private Connection conn;

        private final static String dbHost = "localhost";
        private final static int port = 20591;


        public JdbcConnection() throws SQLException {
            try {
                Class.forName( "ch.unibas.dmi.dbis.polyphenydb.jdbc.Driver" );
            } catch ( ClassNotFoundException e ) {
                log.error( "Polypheny-DB Driver not found", e );
            }
            final String url = "jdbc:polypheny://" + dbHost + ":" + port;
            //String url = "jdbc:polypheny://" + dbHost + ":" + port + "/" + dbName + "?prepareThreshold=0";
            log.debug( "Connecting to database @ {}", url );

            Properties props = new Properties();
            props.setProperty( "user", "pa" );
            //props.setProperty( "password", password );
            //props.setProperty( "ssl", sslEnabled );
            props.setProperty( "wire_protocol", "PROTO3" );

            conn = DriverManager.getConnection( url, props );
            //conn.setAutoCommit( false );
        }


        public Connection getConnection() {
            return conn;
        }


        @Override
        public void close() throws SQLException {
            conn.close();
        }
    }

}
