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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

//import static org.polypheny.db.postgresql.PGInterfaceInboundCommunicationHandler.ctx;


public class PGInterfaceIntegrationTests {

    //select: SELECT * FROM public.PGInterfaceTestTable
    private String dqlQuerySentByClient = "P\u0000\u0000\u00001\u0000SELECT * FROM public.PGInterfaceTestTable\u0000\u0000\u0000B\u0000\u0000\u0000\f\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000D\u0000\u0000\u0000\u0006P\u0000E\u0000\u0000\u0000\t\u0000\u0000\u0000\u0000\u0000S\u0000\u0000\u0000\u0004";

    //insert: INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3);
    private String dmlQuerySentByClient = "P\u0000\u0000\u0000�\u0000INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3)\u0000\u0000\u0000B\u0000\u0000\u0000\f\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000D\u0000\u0000\u0000\u0006P\u0000E\u0000\u0000\u0000\t\u0000\u0000\u0000\u0000\u0001S\u0000\u0000\u0000\u0004";

    //create table: CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))
    private String ddlQuerySentByClient = "P\u0000\u0000\u0000�\u0000CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))\u0000\u0000\u0000B\u0000\u0000\u0000\f\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000D\u0000\u0000\u0000\u0006P\u0000E\u0000\u0000\u0000\t\u0000\u0000\u0000\u0000\u0001S\u0000\u0000\u0000\u0004";
            //new Object[]{"REAL'S HOWTO"};



    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }

    @AfterClass
    public static void stop() {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("sslmode", "disable");
        String url = "jdbc:postgresql://localhost:5432/";
        try (Connection c = DriverManager.getConnection(url, connectionProps)) {
            try(Statement statement = c.createStatement()) {
                int status = statement.executeUpdate("DROP TABLE public.PGInterfaceTestTable");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }



    @Test
    public void testIfDDLIsExecuted() throws SQLException {

        try {
            Connection c = null;
            Properties connectionProps = new Properties();
            connectionProps.setProperty("sslmode", "disable");
            String url = "jdbc:postgresql://localhost:5444/";

            c = DriverManager.getConnection(url, connectionProps);
            Statement statement = c.createStatement();
            int status = statement.executeUpdate("CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))");

            statement.close();


        } catch (Exception e) {
            e.printStackTrace();
        }



        Properties connectionProps = new Properties();
        connectionProps.setProperty("sslmode", "disable");
        String url = "jdbc:postgresql://localhost:5432/";
        try (Connection c = DriverManager.getConnection(url, connectionProps)) {
            try(Statement statement = c.createStatement()) {
                int status = statement.executeUpdate("CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))");
                assertEquals(0, status);
            }
        }
    }

    @Test
    public void basicTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT ARRAY[1, 2] = ARRAY[1, 2], ARRAY[2, 4] = ARRAY[2, 3]" ),
                        ImmutableList.of( new Object[]{ true, false } ) );
            }
        }
    }


    @Test
    public void testIfDMLIsExecuted() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("sslmode", "disable");
        String url = "jdbc:postgresql://localhost:5432/";
        try (Connection c = DriverManager.getConnection(url, connectionProps)) {
            try(Statement statement = c.createStatement()) {
                int status = statement.executeUpdate("INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3);");
                assertEquals(0, status);
            }
        }
    }

    @Test
    public void testIfDQLIsExecuted() throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("sslmode", "disable");
        String url = "jdbc:postgresql://localhost:5432/";
        try (Connection c = DriverManager.getConnection(url, connectionProps)) {
            try (Statement statement = c.createStatement()) {
                TestHelper.checkResultSet(
                        statement.executeQuery("SELECT * FROM PGInterfaceTestTable"),
                        ImmutableList.of(
                                new Object[]{1, "Franz", 1},
                                new Object[]{2, "Hello", 2},
                                new Object[]{3, "By", 3}));

            }
        }
    }

}
