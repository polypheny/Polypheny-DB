/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.catalog;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.XAConnection;
import lombok.extern.slf4j.Slf4j;
import org.hsqldb.Server;
import org.hsqldb.jdbc.pool.JDBCXADataSource;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl.AclFormatException;
import org.polypheny.db.config.RuntimeConfig;


/**
 * This class represents the database systems.
 */
@Slf4j
class Database {

    private static final Database INSTANCE = new Database();
    private static final String DATABASE_NAME = "catalog";
    private static final int DATABASE_PORT = 9001;

    private final Server server;


    private Database() {
        try {
            Class.forName( "org.hsqldb.jdbcDriver" );
            log.info( "Starting catalog database on port {} ...", DATABASE_PORT );
            HsqlProperties p = new HsqlProperties();
            String caseSensitive = RuntimeConfig.CASE_SENSITIVE.getBoolean() ? "sql.ignore_case=false" : "sql.ignore_case=true";
            p.setProperty( "server.database.0", "mem:" + DATABASE_NAME + ";hsqldb.tx=mvcc;hsqldb.tx_level=SERIALIZABLE;" + caseSensitive );
            p.setProperty( "server.dbname.0", DATABASE_NAME );
            p.setProperty( "server.port", DATABASE_PORT );
            p.setProperty( "server.shutdown", false );
            server = new Server();
            server.setProperties( p );
            server.setLogWriter( null );
            server.setErrWriter( null );
            server.setSilent( true );
            server.start();
            log.info( "Catalog database started" );
        } catch ( IOException | ClassNotFoundException | AclFormatException e ) {
            log.error( "Fatal exception while starting catalog database!", e );
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
