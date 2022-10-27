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

package org.polypheny.db.postgres;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

//import static org.polypheny.db.postgresql.PGInterfaceInboundCommunicationHandler.ctx;

@Slf4j
public class PGInterfaceIntegrationTests {

    //select: SELECT * FROM public.PGInterfaceTestTable

    //insert: INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3);
    //create table: CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))
            //new Object[]{"REAL'S HOWTO"};



    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored

        TestHelper.getInstance();

        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate("ALTER INTERFACES ADD \"pgtestinterface\" USING 'org.polypheny.db.postgresql.PGInterface' WITH '{\"port\":\"5433\"}'");
            }
        }

        //ALTER INTERFACES ADD "sdf" USING 'org.polypheny.db.postgresql.PGInterface' WITH '{"port":"5433"}'
    }

    @AfterClass
    public static void stop() throws SQLException {

        try ( PsqlJdbcConnection psqlJdbcConnection = new PsqlJdbcConnection(false) ) {
            Connection connection = psqlJdbcConnection.getConnection();
            try(Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE public.pginterfacetesttable");
                //statement.executeUpdate( "ALTER INTERFACES DROP pgtestinerface" );
            }
        }
    }




    @Test
    public void testIfDMLIsExecuted() throws SQLException {

        try ( PsqlJdbcConnection psqlJdbcConnection = new PsqlJdbcConnection(false) ) {
            Connection connection = psqlJdbcConnection.getConnection();
            try(Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE TABLE pginterfacetesttable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))");
                statement.executeUpdate("INSERT INTO pginterfacetesttable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3);");
                ResultSet rs = statement.executeQuery("SELECT * FROM pginterfacetesttable;");

                TestHelper.checkResultSet(
                        rs,
                        ImmutableList.of(
                                new Object[]{1, "Franz", 1},
                                new Object[]{2, "Hello", 2},
                                new Object[]{3, "By", 3})
                );
            }
        }
    }


/*

 @Test
    public void testIfDDLIsExecuted() throws SQLException {

        try(Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))");
            CatalogTable catalogTable = Catalog.getInstance().getTable(Catalog.getInstance().getSchema(Catalog.defaultDatabaseId, "public").id , "PGInterfaceTestTable");
            assertEquals(catalogTable.name, "pginterfacetesttable");
        } catch (UnknownTableException e) {
            e.printStackTrace();
        } catch (UnknownSchemaException e) {
            e.printStackTrace();
        }
    }




    @Test
    public void testIfDMLIsExecuted() throws SQLException {

        try(Statement statement = c.createStatement()) {
            int status = statement.executeUpdate("INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3);");
            assertEquals(0, status);
        }

    }

    @Test
    public void testIfDQLIsExecuted() throws SQLException {
        try (Connection c = DriverManager.getConnection(url, connectionProps)) {
            try (Statement statement = c.createStatement()) {
                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT * FROM PGInterfaceTestTable;"),
                        ImmutableList.of(
                                new Object[]{1, "Franz", 1},
                                new Object[]{2, "Hello", 2},
                                new Object[]{3, "By", 3}));

            }
        }
    }

 */


    public static class PsqlJdbcConnection implements AutoCloseable {

        private final static String dbHost = "localhost";
        private final static int port = 5433;

        private final Connection conn;


        public PsqlJdbcConnection(boolean autoCommit ) throws SQLException {
            try {
                Class.forName( "org.postgresql.Driver" );
            } catch ( ClassNotFoundException e ) {
                log.error( "PostgreSQL JDBC Driver not found", e );
            }
            final String url = "jdbc:postgresql://" + dbHost + ":" + port + "/";
            log.debug( "Connecting to database @ {}", url );

            Properties connectionProps = new Properties();
            connectionProps.setProperty("sslmode", "disable");
            conn = DriverManager.getConnection(url, connectionProps);

            //conn.setAutoCommit( autoCommit );
        }


        public Connection getConnection() {
            return conn;
        }


        @Override
        public void close() throws SQLException {
            //conn.commit();
            conn.close();
        }

    }

}
