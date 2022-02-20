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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedDatabase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings("SqlDialectInspection")
@Ignore
public class MonetdbSourceTest extends AbstractSourceTest {

    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();

        // Start embedded Monetdb and add default schema and data
        MonetDBEmbeddedDatabase.startDatabase( null ); //in-memory mode
        MonetDBEmbeddedDatabase.createConnection();
        try ( Connection conn = DriverManager.getConnection( "jdbc:monetdb:embedded::memory:" ) ) {
            executeScript( conn, "org/polypheny/db/adapter/monetdb-schema.sql" );
        }

        // Add adapter to Polypheny-DB
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                Map<String, String> settings = new HashMap<>();
                settings.put( "database", "" );
                settings.put( "mode", "remote" );
                settings.put( "host", "running-embedded" );
                settings.put( "maxConnections", "25" );
                settings.put( "password", "" );
                settings.put( "username", "" );
                settings.put( "port", "" );
                settings.put( "tables", "public.auction,public.bid,public.category,public.picture,public.user" );
                Gson gson = new Gson();
                statement.executeUpdate( "ALTER ADAPTERS ADD monetdbunit USING 'org.polypheny.db.adapter.jdbc.sources.MonetdbSource' WITH '" + gson.toJson( settings ) + "'" );
            }
        }
    }


    @AfterClass
    public static void end() throws SQLException {
        MonetDBEmbeddedDatabase.stopDatabase();
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS DROP monetdbunit" );
            }
        }
    }

}
