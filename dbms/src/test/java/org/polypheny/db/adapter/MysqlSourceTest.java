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

package org.polypheny.db.adapter;

import com.google.gson.Gson;
import com.wix.mysql.EmbeddedMysql;
import com.wix.mysql.ScriptResolver;
import com.wix.mysql.distribution.Version;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings("SqlDialectInspection")
@Ignore
public class MysqlSourceTest extends AbstractSourceTest {

    private static EmbeddedMysql server;


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();

        // Start MariaDB and add default schema and data
        server = EmbeddedMysql.anEmbeddedMysql( Version.v5_7_latest )
                .addSchema( "test", ScriptResolver.classPathScript( "org/polypheny/db/adapter/mariadb-schema.sql" ) )
                .start();

        // Add adapter
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                Map<String, String> settings = new HashMap<>();
                settings.put( "database", "test" );
                settings.put( "host", "localhost" );
                settings.put( "maxConnections", "25" );
                settings.put( "password", "" );
                settings.put( "username", "root" );
                settings.put( "port", "" + server.getConfig().getPort() );
                settings.put( "transactionIsolation", "SERIALIZABLE" );
                settings.put( "tables", "auction,bid,category,picture,user" );
                Gson gson = new Gson();
                statement.executeUpdate( "ALTER ADAPTERS ADD mariadbunit USING 'org.polypheny.db.adapter.jdbc.sources.MysqlSource' WITH '" + gson.toJson( settings ) + "'" );
            }
        }
    }


    @AfterClass
    public static void end() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS DROP mariadbunit" );
            }
        }
        server.stop();
    }

}
