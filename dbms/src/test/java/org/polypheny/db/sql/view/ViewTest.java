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

package org.polypheny.db.sql.view;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class ViewTest {

    private final static String VIEWTESTEMPTABLE_SQL = "CREATE TABLE viewTestEmpTable ("
            + "empId INTEGER NOT NULL,"
            + "firstName VARCHAR(20),"
            + "lastName VARCHAR(20),"
            + "depId INTEGER NOT NULL,"
            + "PRIMARY KEY (empId))";

    private final static String VIEWTESTDEPTABLE_SQL = "CREATE TABLE viewTestDepTable ("
            + "depId INTEGER NOT NULL,"
            + "depName VARCHAR(20),"
            + "locationId INTEGER NOT NULL,"
            + "PRIMARY KEY (depId))";

    private final static String VIEWTESTLOCTABLE_SQL = "CREATE TABLE viewTestLocTable ("
            + "locationId INTEGER NOT NULL,"
            + "address VARCHAR(20),"
            + "postcode INTEGER,"
            + "city VARCHAR(20),"
            + "country VARCHAR(20),"
            + "PRIMARY KEY (locationId))";

    private final static String VIEWTESTEMPTABLE_DATA_SQL = "INSERT INTO viewTestEmpTable VALUES"
            + " ( 1, 'Max', 'Muster', 1 ),"
            + "( 2, 'Ernst', 'Walter', 2),"
            + "( 3, 'Elsa', 'Kuster', 3 )";

    private final static String VIEWTESTDEPTABLE_DATA_SQL = "INSERT INTO viewTestDepTable VALUES"
            + "( 1, 'IT', 1),"
            + "( 2, 'Sales', 2),"
            + "( 3, 'HR', 3)";

    private final static String VIEWTESTLOCTABLE_DATA_SQL = "INSERT INTO viewTestLocTable VALUES"
            + "(1, 'Bergstrasse 15', 4058, 'Basel', 'Switzerland'),"
            + "(2, 'Waldstrasse 11', 99900, 'Singen', 'Germany'),"
            + "(3, '5th Avenue 1234', 10001, 'New York', 'USA') ";


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void testSelect() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                    statement.executeUpdate( "CREATE VIEW viewTestEmpDep AS SELECT viewTestEmpTable.firstName, viewTestDepTable.depName FROM viewTestEmpTable INNER JOIN viewTestDepTable ON viewTestEmpTable.depId = viewTestDepTable.depId" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1 },
                                    new Object[]{ 2, "Ernst", "Walter", 2 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT viewTestEmp.firstName FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ "Max" },
                                    new Object[]{ "Ernst" },
                                    new Object[]{ "Elsa" }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmpDep" ),
                            ImmutableList.of(
                                    new Object[]{ "Max", "IT" },
                                    new Object[]{ "Ernst", "Sales" },
                                    new Object[]{ "Elsa", "HR" }
                            )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW viewTestEmp" );
                    statement.executeUpdate( "Drop VIEW viewTestEmpDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                }
            }
        }
    }


    @Test
    public void renameColumnTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                    statement.executeUpdate( "ALTER TABLE viewTestEmp RENAME COLUMN empId TO employeeId" );
                    statement.executeUpdate( "ALTER VIEW viewTestEmp RENAME COLUMN firstName TO fName" );
                    statement.executeUpdate( "ALTER VIEW viewTestEmp RENAME COLUMN lastName TO lName" );
                    statement.executeUpdate( "ALTER VIEW viewTestEmp RENAME COLUMN depId TO departmentId" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT employeeId FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1 },
                                    new Object[]{ 2 },
                                    new Object[]{ 3 } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT fName FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ "Max" },
                                    new Object[]{ "Ernst" },
                                    new Object[]{ "Elsa" } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT lName FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ "Muster" },
                                    new Object[]{ "Walter" },
                                    new Object[]{ "Kuster" } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT departmentId FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1 },
                                    new Object[]{ 2 },
                                    new Object[]{ 3 } ) );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                }

            }
        }
    }


    @Test
    public void renameViewTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );
                statement.executeUpdate( "CREATE VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                statement.executeUpdate( "CREATE VIEW viewTestDep AS SELECT * FROM viewTestDepTable" );

                try {
                    statement.executeUpdate( "ALTER VIEW viewTestEmp RENAME TO viewRenameEmpTest" );
                    statement.executeUpdate( "ALTER TABLE viewTestDep RENAME TO viewRenameDepTest" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewRenameEmpTest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1 },
                                    new Object[]{ 2, "Ernst", "Walter", 2 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewRenameDepTest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1 },
                                    new Object[]{ 2, "Sales", 2 },
                                    new Object[]{ 3, "HR", 3 }
                            )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW viewRenameEmpTest" );
                    statement.executeUpdate( "DROP VIEW viewRenameDepTest" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                }
            }
        }
    }


    //SELECT not possible if inner Select with MAX()
    @Ignore
    @Test
    public void selectAggregateInnerSelectTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTLOCTABLE_SQL );
                statement.executeUpdate( VIEWTESTLOCTABLE_DATA_SQL );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT viewTestLocTable.postcode FROM viewTestLocTable, viewTestDepTable WHERE viewTestLocTable.postcode = (SELECT max(postcode) FROM viewTestLocTable)" ),
                            ImmutableList.of(
                                    new Object[]{ 99900 }
                            )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    statement.executeUpdate( "DROP TABLE viewTestLocTable" );
                }
            }
        }
    }


    @Test
    public void selectAggregateViewTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTLOCTABLE_SQL );
                statement.executeUpdate( VIEWTESTLOCTABLE_DATA_SQL );

                statement.executeUpdate( "CREATE VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                statement.executeUpdate( "CREATE VIEW viewTestDep AS SELECT * FROM viewTestDepTable" );
                statement.executeUpdate( "CREATE VIEW viewTestLoc AS SELECT * FROM viewTestLocTable" );

                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT max(postcode) FROM viewTestLocTable" ),
                            ImmutableList.of(
                                    new Object[]{ 99900 }
                            )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT min(postcode) FROM viewTestLocTable" ),
                            ImmutableList.of(
                                    new Object[]{ 4058 }
                            )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT avg(postcode) FROM viewTestLocTable" ),
                            ImmutableList.of(
                                    new Object[]{ 37986 }
                            )
                    );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP VIEW viewTestDep" );
                    statement.executeUpdate( "DROP VIEW viewTestLoc" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    statement.executeUpdate( "DROP TABLE viewTestLocTable" );
                }
            }
        }
    }

}
