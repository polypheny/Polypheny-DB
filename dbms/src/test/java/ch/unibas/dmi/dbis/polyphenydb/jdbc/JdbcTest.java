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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.PolyphenyDb;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JUnit4.class)
public class JdbcTest {

    private static final Logger LOG = LoggerFactory.getLogger( JdbcTest.class );

    private static final PolyphenyDb polyphenyDb = new PolyphenyDb();

    @BeforeClass
    public static void setup() {
        LOG.info( "Starting Polypheny-DB..." );
        Runnable runnable = () -> {
            try {
                polyphenyDb.runPolyphenyDb();
            } catch ( SQLException | ClassNotFoundException e ) {
                LOG.error( "Exception while starting Polypheny-DB", e );
            }
        };
        Thread thread = new Thread( runnable );
        thread.start();

        // Wait 10 seconds
        try {
            TimeUnit.SECONDS.sleep( 10 );
        } catch ( InterruptedException e ) {
            // Ignore
        }
    }


    @AfterClass
    public static void tearDown() {
        //LOG.info( "shutdown - closing DB connection" );
    }


    // --------------- Tests ---------------

    @Test
    public void testMetaGetTables() {
        try ( PolyphenyDbConnection polyphenyDbConnection = new PolyphenyDbConnection() ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getTables( null, null, "%", null );
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 14, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_SCHEM", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_NAME", rsmd.getColumnName( 3 ) );
            Assert.assertEquals( "Wrong column name", "TABLE_TYPE", rsmd.getColumnName( 4 ) );
            Assert.assertEquals( "Wrong column name", "REMARKS", rsmd.getColumnName( 5 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_CAT", rsmd.getColumnName( 6 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_SCHEM", rsmd.getColumnName( 7 ) );
            Assert.assertEquals( "Wrong column name", "TYPE_NAME", rsmd.getColumnName( 8 ) );
            Assert.assertEquals( "Wrong column name", "SELF_REFERENCING_COL_NAME", rsmd.getColumnName( 9 ) );
            Assert.assertEquals( "Wrong column name", "REF_GENERATION", rsmd.getColumnName( 10 ) );
            Assert.assertEquals( "Wrong column name", "OWNER", rsmd.getColumnName( 11 ) );
            Assert.assertEquals( "Wrong column name", "ENCODING", rsmd.getColumnName( 12 ) );
            Assert.assertEquals( "Wrong column name", "COLLATION", rsmd.getColumnName( 13 ) );
            Assert.assertEquals( "Wrong column name", "DEFINITION", rsmd.getColumnName( 14 ) );
        } catch ( SQLException e ) {
            LOG.error( "Exception while testing getTables()", e );
        }
    }


    class PolyphenyDbConnection implements AutoCloseable {

        private Connection conn;

        private final static String dbHost = "localhost";
        private final static int port = 20591;


        PolyphenyDbConnection() throws SQLException {
            try {
                Class.forName( "ch.unibas.dmi.dbis.polyphenydb.jdbc.Driver" );
            } catch ( ClassNotFoundException e ) {
                LOG.error( "Polypheny-DB Driver not found", e );
            }
            final String url = "jdbc:polypheny://" + dbHost + ":" + port;
            //String url = "jdbc:polypheny://" + dbHost + ":" + port + "/" + dbName + "?prepareThreshold=0";
            LOG.debug( "Connecting to database @ {}", url );

            Properties props = new Properties();
            props.setProperty( "user", "pa" );
            //props.setProperty( "password", password );
            //props.setProperty( "ssl", sslEnabled );
            props.setProperty( "wire_protocol", "PROTO3" );

            conn = DriverManager.getConnection( url, props );
            conn.setAutoCommit( false );
        }


        Connection getConnection() {
            return conn;
        }


        @Override
        public void close() throws SQLException {
            conn.close();
        }
    }

}
