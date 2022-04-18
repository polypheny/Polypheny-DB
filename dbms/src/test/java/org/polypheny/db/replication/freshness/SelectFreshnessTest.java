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

package org.polypheny.db.replication.freshness;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


public class SelectFreshnessTest {

    // TODO @HENNLO ADD FreshnessManager Test


    @Test
    public void testFreshnessAfterDMLOperations() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessoperations( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Freshness Queries cannot be executed if DML operations have already been executed within TX
                    statement.executeUpdate( "INSERT INTO testfreshnessoperations VALUES ( 1 , 10, 'Foo')" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessoperations ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 10, "Foo" } ) );

                    // Check if for a freshness query the TX statement aborts since a DML operation has already been executed.
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnessoperations WITH FRESHNESS 3 HOUR ABSOLUTE" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );


                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessoperations" );
                }

            }
        }
    }


    @Test
    public void testDMLAfterFreshnessOperations() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessoperations( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // DML queries cannot go through if a Freshness Query has already been executed within TX
                    statement.executeUpdate( "SELECT * FROM testfreshnessoperations WITH FRESHNESS 3 HOUR ABSOLUTE" );

                    // Check if for a freshness query the TX statement aborts since a DML operation has already been executed.
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "INSERT INTO testfreshnessoperations VALUES ( 1 , 10, 'Foo')" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );


                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessoperations" );
                }

            }
        }
    }


    /**
     * Checks if PRIMARY and SECONDARY are correctly locked
     */
    @Test
    public void testFreshnessLocking() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnesslocking( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesslocking" );
                }
            }
        }
    }


    @Test
    public void testFreshnessAbsoluteDelay() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessabsolutedelay( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed

                    // Includes DELAY in SECONDS
                    statement.executeUpdate( "SELECT * FROM testfreshnessabsolutedelay WITH FRESHNESS 10 SECOND ABSOLUTE" );

                    // Includes DELAY in MINUTES
                    statement.executeUpdate( "SELECT * FROM testfreshnessabsolutedelay WITH FRESHNESS 10 MINUTE ABSOLUTE" );

                    // Includes DELAY in HOURS
                    statement.executeUpdate( "SELECT * FROM testfreshnessabsolutedelay WITH FRESHNESS 3 HOUR ABSOLUTE" );

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL values (negative time delay)
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnessabsolutedelay WITH FRESHNESS -10 MINUTE ABSOLUTE" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE testfreshnessabsolutedelay "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );

/*
                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnessabsolutedelay VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO testfreshnessabsolutedelay VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessabsolutedelay ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );


                    // Disable automatic refresh operations to validate the deviation
                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( false );


                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnessabsolutedelay VALUES (3, 100, 'foobar')" );

                    // Check main result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessabsolutedelay ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );

                    // Check outdated result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessabsolutedelay ORDER BY tprimary WITH FRESHNESS 1 HOUR ABSOLUTE" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" }) );


                    // Check fallback to primary if it cannot be fulfilled
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessabsolutedelay ORDER BY tprimary WITH FRESHNESS 1 SECOND ABSOLUTE" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );
*/

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessabsolutedelay" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }


    @Test
    public void testFreshnessPercentage() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnesspercentage( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed
                    statement.executeUpdate( "SELECT * FROM testfreshnesspercentage WITH FRESHNESS 60%" );

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL PERCENTAGE (out of bound)
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnesspercentage WITH FRESHNESS 101%" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnesspercentage WITH FRESHNESS -10%" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check if it falls back to primary placements
                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE testfreshnesspercentage "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );

/*
                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnesspercentage VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO testfreshnesspercentage VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesspercentage ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );


                    // Disable automatic refresh operations to validate the deviation
                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( false );


                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnesspercentage VALUES (3, 100, 'foobar')" );

                    // Check main result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesspercentage ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );

                    // Check outdated result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesspercentage ORDER BY tprimary WITH FRESHNESS 50%" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" }) );


                    // Check fallback to primary if it cannot be fulfilled
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesspercentage ORDER BY tprimary WITH FRESHNESS 90%" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );


 */
                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesspercentage" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }


    @Test
    public void testFreshnessRelativeDelay() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessrelativedelay( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed

                    // Includes DELAY in SECONDS
                    statement.executeUpdate( "SELECT * FROM testfreshnessrelativedelay WITH FRESHNESS 10 SECOND DELAY" );

                    // Includes DELAY in MINUTES
                    statement.executeUpdate( "SELECT * FROM testfreshnessrelativedelay WITH FRESHNESS 10 MINUTE DELAY" );

                    // Includes DELAY in HOURS
                    statement.executeUpdate( "SELECT * FROM testfreshnessrelativedelay WITH FRESHNESS 3 HOUR DELAY" );

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL values (negative time delay)
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnessrelativedelay WITH FRESHNESS -10 MINUTE DELAY" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check if it falls back to primary placements
// Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE testfreshnessrelativedelay "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );

/*
                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnessrelativedelay VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO testfreshnessrelativedelay VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessrelativedelay ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );


                    // Disable automatic refresh operations to validate the deviation
                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( false );


                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnessrelativedelay VALUES (3, 100, 'foobar')" );

                    // Check main result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessrelativedelay ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );

                    // Check outdated result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessrelativedelay ORDER BY tprimary WITH FRESHNESS 1 MINUTE DELAY" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" }) );


                    // Check fallback to primary if it cannot be fulfilled
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessrelativedelay ORDER BY tprimary WITH FRESHNESS 1 SECOND DELAY" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );


 */
                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessrelativedelay" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }


    @Test
    public void testFreshnessIndex() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessindex( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed
                    statement.executeUpdate( "SELECT * FROM testfreshnessindex WITH FRESHNESS 0.5" );

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL PERCENTAGE (out of bound)
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnessindex WITH FRESHNESS 1.1" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    failed = false;
                    try {
                        statement.executeUpdate( "SELECT * FROM testfreshnessindex WITH FRESHNESS -0.5" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check if it falls back to primary placements
// Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE testfreshnessindex "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );

/*
                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnessindex VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO testfreshnessindex VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessindex ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );


                    // Disable automatic refresh operations to validate the deviation
                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( false );


                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnessindex VALUES (3, 100, 'foobar')" );

                    // Check main result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessindex ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );

                    // Check outdated result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessindex ORDER BY tprimary WITH FRESHNESS 0.5" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" }) );


                    // Check fallback to primary if it cannot be fulfilled
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnessindex ORDER BY tprimary WITH FRESHNESS 0.9" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );



 */
                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessindex" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }


    @Test
    public void testFreshnessTime() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnesstime( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed

                    // Includes Timestamps
                    statement.executeUpdate( "SELECT * FROM testfreshnesstime WITH FRESHNESS TIMESTAMP '2022-03-04 09:11:37.003' " );

                    // Includes Time
                    // statement.executeUpdate( "SELECT * FROM testfreshnesstime WITH FRESHNESS '09:11:37.003' ");

                    // Includes Date
                    //statement.executeUpdate( "SELECT * FROM testfreshnesstime WITH FRESHNESS '2022-03-04'");

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL TIME (in future)

                    // Test ILLEGAL TIME (invalid Timestamp)

                    // Test ILLEGAL TIME (incomplete time)

                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE testfreshnesstime "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );
/*

                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnesstime VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO testfreshnesstime VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesstime ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );


                    // Disable automatic refresh operations to validate the deviation
                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( false );


                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO testfreshnesstime VALUES (3, 100, 'foobar')" );

                    // Check main result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesstime ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );

                    // Check outdated result again
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesstime ORDER BY tprimary WITH FRESHNESS 50%" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" }) );


                    // Check fallback to primary if it cannot be fulfilled
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM testfreshnesstime ORDER BY tprimary WITH FRESHNESS 90%" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" },
                                    new Object[]{ 3, 100, "foobar" }) );


 */
                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesstime" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }

}
