/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.clause;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class JoinTest {


    public static final String CREATE_OUTER_JOIN_CUSTOMERS = "CREATE TABLE Join_Customers("
            + "CustomerID INTEGER NOT NULL, "
            + "CustomerName VARCHAR(255), "
            + "ContactName VARCHAR(255), "
            + "Address VARCHAR(255), "
            + "City VARCHAR(255), "
            + "PostalCode VARCHAR(12), "
            + "Country VARCHAR(255), "
            + "PRIMARY KEY (CustomerID))";

    public static final String CREATE_OUTER_JOIN_ORDERS = "CREATE TABLE Join_Orders("
            + "OrderID INTEGER NOT NULL, "
            + "CustomerID INTEGER, "
            + "EmployeeID INTEGER, "
            + "OrderDate DATE, "
            + "ShipperId Integer, "
            + "PRIMARY KEY (OrderID))";
    public static final String CUSTOMER_1 = "INSERT INTO Join_Customers VALUES ("
            + "1, "
            + "'Alfreds Futterkiste', "
            + "'Maria Anders', "
            + "'Obere Str. 57', "
            + "'Berlin', "
            + "'12209', "
            + "'Germany')";
    public static final String CUSTOMER_2 = "INSERT INTO Join_Customers VALUES ("
            + "2, "
            + "'Ana Trujillo Emparedados y helados', "
            + "'Ana Trujillo', "
            + "'Avda. de la Constitución 2222', "
            + "'México D.F.', "
            + "'05021', "
            + "'Mexico')";
    public static final String CUSTOMER_3 = "INSERT INTO Join_Customers VALUES ("
            + "3, "
            + "'Antonio Moreno Taquería', "
            + "'Antonio Moreno', "
            + "'Mataderos 2312', "
            + "'México D.F.', "
            + "'05023', "
            + "'Mexico')";
    public static final String ORDER_1 = "INSERT INTO Join_Orders VALUES ("
            + "10308, "
            + "2, "
            + "7, "
            + "date '1996-09-18', "
            + "3)";
    public static final String ORDER_2 = "INSERT INTO Join_Orders VALUES ("
            + "10309, "
            + "37, "
            + "3, "
            + "date '1996-09-19', "
            + "1)";
    public static final String ORDER_3 = "INSERT INTO Join_Orders VALUES ("
            + "10310, "
            + "77, "
            + "8, "
            + "date '1996-09-20', "
            + "2)";


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE TableA(ID VARCHAR(255) NOT NULL, NAME VARCHAR(255), AMOUNT INTEGER, PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Ab', 'Name1', 10000.00)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Bc', 'Name2',  5000.00)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES ('Cd', 'Name3',  7000.00)" );

                statement.executeUpdate( CREATE_OUTER_JOIN_CUSTOMERS );
                statement.executeUpdate( CUSTOMER_1 );
                statement.executeUpdate( CUSTOMER_2 );
                statement.executeUpdate( CUSTOMER_3 );

                statement.executeUpdate( CREATE_OUTER_JOIN_ORDERS );
                statement.executeUpdate( ORDER_1 );
                statement.executeUpdate( ORDER_2 );
                statement.executeUpdate( ORDER_3 );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TableA" );
                statement.executeUpdate( "DROP TABLE Join_Customers" );
                statement.executeUpdate( "DROP TABLE Join_Orders" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void naturalJoinTests() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Name1", "Ab", 10000 },
                        new Object[]{ "Name2", "Bc", 5000 },
                        new Object[]{ "Name3", "Cd", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S NATURAL JOIN (SELECT name, Amount  FROM TableA) AS T" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void innerJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S INNER JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void nestedJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 10000, "Ab", "Name1", "Name1", "Name1" },
                        new Object[]{ 5000, "Bc", "Name2", "Name2", "Name2" },
                        new Object[]{ 7000, "Cd", "Name3", "Name3", "Name3" }
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S INNER JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name NATURAL JOIN (SELECT name, Amount  FROM TableA) AS X" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void leftJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S LEFT JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name GROUP BY T.name, S.name, S.id, T.Amount ORDER BY SUM(T.Amount + 1)" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void rightJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S RIGHT JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void fullJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Ab", "Name1", "Name1", 10000 },
                        new Object[]{ "Bc", "Name2", "Name2", 5000 },
                        new Object[]{ "Cd", "Name3", "Name3", 7000 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM (SELECT id, name FROM TableA) AS S FULL JOIN (SELECT name, Amount  FROM TableA) AS T ON S.name = T.name" ),
                        expectedResult,
                        true );
            }
        }
    }


    /**
     * The FULL OUTER JOIN keyword returns all matching records from both tables whether the other table matches or not.
     * If there are rows in "Customers" that do not have matches in "Orders",
     * or if there are rows in "Orders" that do not have matches in "Customers",
     * those rows will be listed as well.
     */
    @Test
    public void fullOuterJoinTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ null, 10309 },
                        new Object[]{ null, 10310 },
                        new Object[]{ "Alfreds Futterkiste", null },
                        new Object[]{ "Ana Trujillo Emparedados y helados", 10308 },
                        new Object[]{ "Antonio Moreno Taquería", null }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( """
                                SELECT Join_Customers.CustomerName, Join_Orders.OrderID
                                FROM Join_Customers
                                FULL OUTER JOIN Join_Orders ON Join_Customers.CustomerID = Join_Orders.CustomerID
                                ORDER BY Join_Customers.CustomerName""" ),
                        expectedResult,
                        true );
            }
        }
    }


    @Test
    public void fullTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeQuery( """
                        SELECT *
                        FROM Join_Customers, Join_Orders""" );
            }
        }
    }


    @Test
    public void fullMaxTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeQuery( """
                        SELECT Join_Customers.CustomerId
                        FROM Join_Customers, Join_Orders WHERE Join_Customers.CustomerId = (SELECT MAX(CustomerId) FROM Join_Customers)""" );
            }
        }
    }

}
