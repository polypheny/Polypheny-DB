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

package org.polypheny.db.misc;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
public class HorizontalPartitioningTest {

    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void basicHorizontalPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE horizontalparttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                //Partition table after creation
                statement.executeUpdate( "ALTER TABLE horizontalparttest "
                        + "PARTITION BY HASH (tinteger) "
                        + "PARTITIONS 4" );

                //Cannot partition a table that has already been partitioned
                boolean failed = false;
                try {
                    statement.executeUpdate( "ALTER TABLE horizontalparttest "
                            + "PARTITION BY HASH (tinteger) "
                            + "PARTITIONS 2" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                //check assert False. Wrong partition column
                failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE horizontalparttestfalsepartition( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (othercolumn) "
                            + "PARTITIONS 3" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                // Drop tables and stores
                statement.executeUpdate( "DROP TABLE horizontalparttest" );
                statement.executeUpdate( "DROP TABLE horizontalparttestfalsepartition" );
            }
        }
    }


    @Test
    public void modifyPartitionTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE horizontalparttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 3" );

                // Deploy additional store
                statement.executeUpdate( "ALTER STORES ADD \"store3\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );

                // Add placement
                statement.executeUpdate( "ALTER TABLE \"horizontalparttest\" ADD PLACEMENT (tvarchar) ON STORE \"store3\"" );

                //Modify partitons on new placement
                statement.executeUpdate( "ALTER TABLE \"horizontalparttest\" MODIFY PARTITIONS (0,1) ON STORE \"store3\" " );

                //AsserTFalse
                //Modify partitions out of index error
                boolean failed = false;
                try {
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttest\" MODIFY PARTITIONS (0,1,4) ON STORE \"store1\" " );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                //Create another table with initial partitoning
                statement.executeUpdate( "CREATE TABLE horizontalparttestextension( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 3" );

                // Deploy additional store
                statement.executeUpdate( "ALTER STORES ADD \"store2\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );

                //Merge partiton
                statement.executeUpdate( "ALTER TABLE horizontalparttestextension MERGE PARTITIONs" );

                //Add placement for seconf table
                statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" ADD PLACEMENT (tvarchar) ON STORE \"store2\"" );

                // Partition by name
                statement.executeUpdate( "ALTER TABLE horizontalparttestextension "
                        + "PARTITION BY HASH (tinteger) "
                        + " WITH (name1, name2, name3)" );

                //name partitioning can be modified with index
                statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MODIFY PARTITIONS (1) ON STORE \"store2\" " );

                //name partitioning can be modified with name
                statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MODIFY PARTITIONS (name2, name3) ON STORE \"store2\" " );

                //check assert False. modify with false name no partition exists with name22
                failed = false;
                try {
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MODIFY PARTITIONS (name22) ON STORE \"store2\" " );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                // Drop tables and stores
                statement.executeUpdate( "DROP TABLE horizontalparttestextension" );
                statement.executeUpdate( "DROP TABLE horizontalparttest" );
                statement.executeUpdate( "ALTER STORES DROP \"store3\"" );
                statement.executeUpdate( "ALTER STORES DROP \"store2\"" );
            }
        }
    }


    //Check if partitions have enough partitions
    @Test
    public void partitionNumberTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                //invalid partitionsize
                boolean failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE horizontalparttestfalseNEW( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "PARTITIONS 1" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                //assert false partitioning only with partiotn name is not allowed
                failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE horizontal2( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "WITH (name1)" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                statement.executeUpdate( "DROP TABLE horizontalparttestfalseNEW" );
                statement.executeUpdate( "DROP TABLE horizontal2" );
            }
        }
    }


    @Test
    public void hashPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                //Create basic setup
                statement.executeUpdate( "CREATE TABLE hashpartition( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 3" );

                //AsserTFalse
                //HASH Partitioning cant be created using values
                boolean failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE hashpartitioning( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "( PARTITION parta VALUES('abc'), "
                            + "PARTITION partb VALUES('def'))" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                //ADD store
                statement.executeUpdate( "ALTER STORES ADD \"storehash\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );

                //ADD FullPlacement
                statement.executeUpdate( "ALTER TABLE \"hashpartition\" ADD PLACEMENT ON STORE \"storehash\"" );

                // Change placement on second store
                statement.executeUpdate( "ALTER TABLE \"hashpartition\" MODIFY PARTITIONS (0,1) ON STORE \"storehash\"" );

                // Change placement on second store
                //check partition distribution violation
                failed = false;
                try {
                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" MODIFY PARTITIONS (2) ON STORE \"hsqldb\"" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                //You can't change the distribution unless there exists at least one full partition placement of each column as a fallback
                failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE hashpartitioningValidate( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "( PARTITION parta VALUES('abc'), "
                            + "PARTITION partb VALUES('def'))" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                statement.executeUpdate( "DROP TABLE hashpartitioning" );
                statement.executeUpdate( "DROP TABLE hashpartition" );
                statement.executeUpdate( "DROP TABLE hashpartitioningValidate" );
                statement.executeUpdate( "ALTER STORES DROP \"storehash\"" );
            }
        }
    }


    @Test
    public void listPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                //AsserTFalse
                //LIST Partitioning should be created using values
                statement.executeUpdate( "CREATE TABLE listpartitioning( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY LIST (tvarchar) "
                        + "( PARTITION parta VALUES('abc'), "
                        + "PARTITION partb VALUES('def', 'qrs'))" );

                //LIST Partitioning check if unbound partiiton is correctly added when only specifying oen explicit partition

                statement.executeUpdate( "CREATE TABLE listpartitioning3( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY LIST (tvarchar) "
                        + "( PARTITION parta VALUES('abc','def') )" );

                //LIST partitoining can't be created with only empty lists
                boolean failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE listpartitioning2( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY LIST (tvarchar) "
                            + "PARTITIONS 3" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                // TODO: check partition distribution violation

                // TODO: Chek unbound partitions

                statement.executeUpdate( "DROP TABLE listpartitioning" );
                statement.executeUpdate( "DROP TABLE listpartitioning2" );
                statement.executeUpdate( "DROP TABLE listpartitioning3" );
            }
        }
    }


    @Test
    public void rangePartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE rangepartitioning1( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY RANGE (tinteger) "
                        + "( PARTITION parta VALUES(1,5), "
                        + "PARTITION partb VALUES(6,10))" );

                statement.executeUpdate( "INSERT INTO rangepartitioning1 VALUES (1, 3, 'hans')" );
                statement.executeUpdate( "INSERT INTO rangepartitioning1 VALUES (2, 7, 'bob')" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM rangepartitioning1" ),
                        ImmutableList.of(
                                new Object[]{ 1, 3, "hans" },
                                new Object[]{ 2, 7, "bob" } ) );

                statement.executeUpdate( "UPDATE rangepartitioning1 SET tinteger = 4 WHERE tinteger = 7" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM rangepartitioning1" ),
                        ImmutableList.of(
                                new Object[]{ 1, 3, "hans" },
                                new Object[]{ 2, 4, "bob" } ) );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM rangepartitioning1 WHERE tinteger = 4" ),
                        ImmutableList.of(
                                new Object[]{ 2, 4, "bob" } ) );

                // RANGE partitioning can't be created without specifying ranges
                boolean failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE rangepartitioning2( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY RANGE (tinteger) "
                            + "PARTITIONS 3" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                statement.executeUpdate( "DROP TABLE rangepartitioning1" );
                statement.executeUpdate( "DROP TABLE rangepartitioning2" );
            }
        }
    }

}
