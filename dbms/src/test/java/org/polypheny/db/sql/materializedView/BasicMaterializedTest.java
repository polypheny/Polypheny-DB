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

package org.polypheny.db.sql.materializedView;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class BasicMaterializedTest {

    private final CountDownLatch waiter = new CountDownLatch( 1 );

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


    private final static String DROP_viewTestEmpTable = "DROP TABLE IF EXISTS viewTestEmpTable";
    private final static String DROP_viewTestDepTable = "DROP TABLE IF EXISTS viewTestDepTable";
    private final static String DROP_viewTestLocTable = "DROP TABLE IF EXISTS viewTestLocTable";


    public void dropTables( Statement statement ) throws SQLException {
        statement.executeUpdate( DROP_viewTestEmpTable );
        statement.executeUpdate( DROP_viewTestDepTable );
        statement.executeUpdate( DROP_viewTestLocTable );
    }


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
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS MANUAL" );
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmpDep AS SELECT viewTestEmpTable.firstName, viewTestDepTable.depName FROM viewTestEmpTable INNER JOIN viewTestDepTable ON viewTestEmpTable.depId = viewTestDepTable.depId FRESHNESS MANUAL" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
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
                                    new Object[]{ "Max", "IT", 0 },
                                    new Object[]{ "Ernst", "Sales", 1 },
                                    new Object[]{ "Elsa", "HR", 2 }
                            )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "Drop MATERIALIZED VIEW viewTestEmpDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testIfNotExists() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS MANUAL" );
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW IF NOT EXISTS viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS MANUAL" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
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
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                    //statement.executeUpdate( "ALTER TABLE viewTestEmp RENAME COLUMN empId TO employeeId" );
                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp RENAME COLUMN firstName TO fName" );
                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp RENAME COLUMN lastName TO lName" );
                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp RENAME COLUMN depId TO departmentId" );

                   /*TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT employeeId FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1 },
                                    new Object[]{ 2 },
                                    new Object[]{ 3 } ) );
                    */
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
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
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
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                //statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable" );

                try {
                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp RENAME TO viewRenameEmpTest" );
                    //statement.executeUpdate( "ALTER TABLE viewTestDep RENAME TO viewRenameDepTest" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewRenameEmpTest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );
                    /*
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewRenameDepTest" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1 },
                                    new Object[]{ 2, "Sales", 2 },
                                    new Object[]{ 3, "HR", 3 }
                            )
                    );
                     */
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewRenameEmpTest" );
                    //statement.executeUpdate( "DROP VIEW viewRenameDepTest" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
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

                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestLoc AS SELECT * FROM viewTestLocTable" );

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
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestDep" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestLoc" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    statement.executeUpdate( "DROP TABLE viewTestLocTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testMixedViewAndTable() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp, viewTestDepTable WHERE depname = 'IT'" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0, 1, "IT", 1 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1, 1, "IT", 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2, 1, "IT", 1 }
                            )
                    );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testMultipleMaterializedViews() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp, viewTestDep WHERE depname = 'IT'" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0, 1, "IT", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1, 1, "IT", 1, 0 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2, 1, "IT", 1, 0 }
                            )
                    );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testMaterializedFromMaterialized() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable" );
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable" );
                    statement.executeUpdate( "Create MATERIALIZED view viewFromView as Select * FROM viewTestEmp, viewTestDep WHERE depname = 'IT'" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewFromView" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0, 1, "IT", 1, 0, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1, 1, "IT", 1, 0, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2, 1, "IT", 1, 0, 2 }
                            )
                    );

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewFromView" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testBasicFreshnessInterval() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS INTERVAL 100 \"milliseconds\" " );
                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );
                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testBasicFreshnessUpdate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTEMPTABLE_DATA_SQL );

                try {
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS UPDATE 2 " );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testComplexFreshnessInterval() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );

                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS INTERVAL 100 \"milliseconds\" " );
                waiter.await( 2, TimeUnit.SECONDS );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testOneStores() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"store3\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );

                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable ON STORE \"store3\" FRESHNESS INTERVAL 100 \"milliseconds\" " );
                waiter.await( 2, TimeUnit.SECONDS );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store3\"" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }


    @Ignore
    public void testTwoStores() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"store2\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"store3\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );

                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable ON STORE \"store2\", \"store3\" FRESHNESS INTERVAL 100 \"milliseconds\" " );
                waiter.await( 2, TimeUnit.SECONDS );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store3\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store2\"" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testComplexFreshnessUpdate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS UPDATE 2 " );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                    statement.executeUpdate( "DELETE FROM viewTestEmpTable" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            )
                    );

                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testFreshnessManual() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS MANUAL " );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp FRESHNESS MANUAL" );

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );


                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testFreshnessManualInterval() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS INTERVAL 10 \"min\"" );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp FRESHNESS MANUAL" );

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );


                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testFreshnessManualUpdate() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS UPDATE 5" );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp FRESHNESS MANUAL" );

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );


                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testComplexFreshnessManual() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS MANUAL " );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp FRESHNESS MANUAL" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            )
                    );

                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            )
                    );

                    statement.executeUpdate( "ALTER MATERIALIZED VIEW viewTestEmp FRESHNESS MANUAL" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                } catch ( InterruptedException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            }
        }
    }


    @Test
    public void testTwoFreshnessIntervals() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable FRESHNESS INTERVAL 500 \"milliseconds\"" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS INTERVAL 100 \"milliseconds\" " );
                waiter.await( 2, TimeUnit.SECONDS );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 1, 'IT', 1)");
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 }
                            ) );

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 2, 'Sales', 2), ( 3, 'HR', 3)");
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 },
                                    new Object[]{ 2, "Sales", 2, 1 },
                                    new Object[]{ 3, "HR", 3, 2 }
                            ) );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testUpdateFreshnessUpdates() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable FRESHNESS UPDATE 2 " );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS UPDATE 2 " );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 1, 'IT', 1)" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of()
                    );

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 2, 'Sales', 2), ( 3, 'HR', 3)" );
                    connection.commit();
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 },
                                    new Object[]{ 2, "Sales", 2, 1 },
                                    new Object[]{ 3, "HR", 3, 2 }
                            ) );

                    statement.executeUpdate( "DELETE FROM viewTestEmpTable" );
                    connection.commit();
                    statement.executeUpdate( "TRUNCATE TABLE viewTestDepTable" );
                    connection.commit();

                    waiter.await( 2, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 },
                                    new Object[]{ 2, "Sales", 2, 1 },
                                    new Object[]{ 3, "HR", 3, 2 }
                            ) );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testUpdateFreshnessIntervals() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( VIEWTESTDEPTABLE_SQL );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestDep AS SELECT * FROM viewTestDepTable FRESHNESS INTERVAL 500 \"milliseconds\"" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS INTERVAL 100 \"milliseconds\" " );
                waiter.await( 1, TimeUnit.SECONDS );
                try {

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 1, 'IT', 1)" );
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 }
                            ) );

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 2, 'Sales', 2), ( 3, 'HR', 3)" );
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 2, 'Ernst', 'Walter', 2), ( 3, 'Elsa', 'Kuster', 3 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 },
                                    new Object[]{ 2, "Sales", 2, 1 },
                                    new Object[]{ 3, "HR", 3, 2 }
                            ) );

                    statement.executeUpdate( "DELETE FROM viewTestEmpTable" );
                    statement.executeUpdate( "TRUNCATE TABLE viewTestDepTable" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of()
                    );

                    statement.executeUpdate( "INSERT INTO viewTestDepTable VALUES ( 1, 'IT', 1)" );
                    statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestDep" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "IT", 1, 0 }
                            ) );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void testDeleteFreshnessInterval() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( VIEWTESTEMPTABLE_SQL );
                statement.executeUpdate( "INSERT INTO viewTestEmpTable VALUES ( 1, 'Max', 'Muster', 1 )" );
                statement.executeUpdate( "CREATE MATERIALIZED VIEW viewTestEmp AS SELECT * FROM viewTestEmpTable FRESHNESS INTERVAL 100 \"milliseconds\" " );
                waiter.await( 1, TimeUnit.SECONDS );
                try {

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 }
                            ) );

                    statement.executeUpdate( "DELETE FROM viewTestEmpTable" );
                    connection.commit();

                    waiter.await( 1, TimeUnit.SECONDS );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM viewTestEmp" ),
                            ImmutableList.of()
                    );

                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW viewTestEmp" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    dropTables( statement );
                }
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }

/*
    @Test
    public void testMaterializedViewFromView() throws SQLException {
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
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW materializedFromView AS SELECT * FROM viewTestEmp FRESHNESS MANUAL" );
                    statement.executeUpdate( "CREATE MATERIALIZED VIEW materializedFromComplexView AS SELECT * FROM viewTestEmpDep FRESHNESS MANUAL" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM materializedFromView ORDER BY empid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Max", "Muster", 1, 0 },
                                    new Object[]{ 2, "Ernst", "Walter", 2, 1 },
                                    new Object[]{ 3, "Elsa", "Kuster", 3, 2 }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT materializedFromView.firstName FROM materializedFromView ORDER BY materializedFromView.firstName" ),
                            ImmutableList.of(
                                    new Object[]{ "Elsa" },
                                    new Object[]{ "Ernst" },
                                    new Object[]{ "Max" }
                            )
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM materializedFromComplexView" ),
                            ImmutableList.of(
                                    new Object[]{ "Max", "IT", 0 },
                                    new Object[]{ "Ernst", "Sales", 1 },
                                    new Object[]{ "Elsa", "HR", 2 }
                            ),
                            true
                    );
                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP MATERIALIZED VIEW materializedFromView" );
                    statement.executeUpdate( "DROP MATERIALIZED VIEW materializedFromComplexView" );
                    statement.executeUpdate( "DROP VIEW viewTestEmp" );
                    statement.executeUpdate( "Drop VIEW viewTestEmpDep" );
                    statement.executeUpdate( "DROP TABLE viewTestEmpTable" );
                    statement.executeUpdate( "DROP TABLE viewTestDepTable" );
                }
            }
        }
    }

 */

}
