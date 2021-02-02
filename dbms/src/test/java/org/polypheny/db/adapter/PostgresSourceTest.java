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
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings("SqlDialectInspection")
public class PostgresSourceTest extends AbstractSourceTest {

    @ClassRule
    public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();

        // Start embedded Postgres and add default schema and data
        try ( Connection conn = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection() ) {
            InputStream file = ClassLoader.getSystemResourceAsStream( "org/polypheny/db/adapter/postgres-schema.sql" );
            // Check if file != null
            if ( file == null ) {
                throw new RuntimeException( "Unable to load schema definition file" );
            }
            Statement statement = conn.createStatement();
            try ( BufferedReader bf = new BufferedReader( new InputStreamReader( file ) ) ) {
                String line = bf.readLine();
                while ( line != null ) {
                    statement.executeUpdate( line );
                    line = bf.readLine();
                }
            } catch ( IOException e ) {
                throw new RuntimeException( "Exception while creating schema", e );
            }
        }

        // Add adapter
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                Map<String, String> settings = new HashMap<>();
                settings.put( "database", "postgres" );
                settings.put( "host", "localhost" );
                settings.put( "maxConnections", "25" );
                settings.put( "password", "" );
                settings.put( "username", "postgres" );
                settings.put( "port", "" + pg.getEmbeddedPostgres().getPort() );
                settings.put( "transactionIsolation", "SERIALIZABLE" );
                settings.put( "tables", "public.auction,public.bid,public.category,public.picture,public.user" );
                Gson gson = new Gson();
                statement.executeUpdate( "ALTER ADAPTERS ADD postgresunit USING 'org.polypheny.db.adapter.jdbc.sources.PostgresqlSource' WITH '" + gson.toJson( settings ) + "'" );
            }
        }
    }


    @AfterClass
    public static void end() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS DROP postgresunit" );
            }
        }
    }

}
