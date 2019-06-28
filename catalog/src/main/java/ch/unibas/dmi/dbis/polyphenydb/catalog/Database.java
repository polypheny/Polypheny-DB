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

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.XAConnection;
import org.hsqldb.Server;
import org.hsqldb.jdbc.pool.JDBCXADataSource;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl.AclFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class represents the database systems.
 */
class Database {


    private static final Logger logger = LoggerFactory.getLogger( Database.class );

    private static final Database INSTANCE = new Database();
    private static final String DATABASE_NAME = "catalog";
    private static final int DATABASE_PORT = 9001;

    private final Server server;


    private Database() {
        try {
            Class.forName( "org.hsqldb.jdbcDriver" );
            logger.info( "Starting catalog database on port {} ...", DATABASE_PORT );
            HsqlProperties p = new HsqlProperties();
            p.setProperty( "server.database.0", "mem:" + DATABASE_NAME );
            p.setProperty( "server.dbname.0", DATABASE_NAME );
            p.setProperty( "server.port", DATABASE_PORT );
            p.setProperty( "server.shutdown", false );
            server = new Server();
            server.setProperties( p );
            server.setLogWriter( null );
            server.setErrWriter( null );
            server.setSilent( true );
            server.start();
            logger.info( "Catalog database started" );
        } catch ( IOException | ClassNotFoundException | AclFormatException e ) {
            logger.error( "Fatal exception while starting catalog database!", e );
            throw new RuntimeException( e );
        }

        Runtime.getRuntime().addShutdownHook( new Thread( server::shutdown ) );
    }


    static Database getInstance() {
        return INSTANCE;
    }


    XAConnection getXaConnection() throws SQLException {
        JDBCXADataSource xaDataSource = new JDBCXADataSource();
        xaDataSource.setURL( "jdbc:hsqldb:hsql://localhost:" + DATABASE_PORT + "/" + DATABASE_NAME );
        xaDataSource.setUser( "SA" );
        xaDataSource.setPassword( "" );
        return xaDataSource.getXAConnection();
    }


    Connection getConnection() throws SQLException {
        return DriverManager.getConnection( "jdbc:hsqldb:hsql://localhost:" + DATABASE_PORT + "/" + DATABASE_NAME, "SA", "" );
    }


}
