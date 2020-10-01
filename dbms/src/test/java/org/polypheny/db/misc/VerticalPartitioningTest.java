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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class VerticalPartitioningTest {


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void basicTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE partitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                // Deploy additional store
                statement.executeUpdate( "ALTER STORES ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );

                // Add placement
                statement.executeUpdate( "ALTER TABLE \"partitioningtest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );

                // Change placement on intial store
                statement.executeUpdate( "ALTER TABLE \"partitioningtest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );

                // Insert data
                statement.executeUpdate( "INSERT INTO partitioningtest VALUES (1,5,'foo')" );
                statement.executeUpdate( "INSERT INTO partitioningtest VALUES (2,22,'bar'),(3,69,'xyz')" );

                // Update data
                statement.executeUpdate( "UPDATE partitioningtest SET tinteger = 33 WHERE tprimary = 1" );
                statement.executeUpdate( "UPDATE partitioningtest SET tprimary = 4 WHERE tprimary = 2" );

                // Delete data
                statement.executeUpdate( "DELETE FROM partitioningtest WHERE tprimary = 3" );

                // Checks
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM partitioningtest ORDER BY tprimary" ),
                        ImmutableList.of(
                                new Object[]{ 1, 33, "foo" },
                                new Object[]{ 4, 22, "bar" } ) );
                // Drop table and store
                statement.executeUpdate( "DROP TABLE partitioningtest" );
                statement.executeUpdate( "ALTER STORES DROP \"store1\"" );
            }
        }
    }


    @Test
    public void preparedBatchTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE partitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                // Deploy additional store
                statement.executeUpdate( "ALTER STORES ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );

                // Add placement
                statement.executeUpdate( "ALTER TABLE \"partitioningtest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );

                // Change placement on intial store
                statement.executeUpdate( "ALTER TABLE \"partitioningtest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );

                // Insert Data
                PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO partitioningtest VALUES (?, ?, ?)" );
                preparedInsert.setInt( 1, 1 );
                preparedInsert.setInt( 2, 33 );
                preparedInsert.setString( 3, "foo" );
                preparedInsert.addBatch();
                preparedInsert.setInt( 1, 2 );
                preparedInsert.setInt( 2, 22 );
                preparedInsert.setString( 3, "bar" );
                preparedInsert.addBatch();
                preparedInsert.setInt( 1, 3 );
                preparedInsert.setInt( 2, 69 );
                preparedInsert.setString( 3, "foobar" );
                preparedInsert.addBatch();
                preparedInsert.executeBatch();

                // Checks
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM partitioningtest ORDER BY tprimary" ),
                        ImmutableList.of(
                                new Object[]{ 1, 33, "foo" },
                                new Object[]{ 2, 22, "bar" },
                                new Object[]{ 3, 69, "foobar" } ) );

                // Drop table and store
                statement.executeUpdate( "DROP TABLE partitioningtest" );
                statement.executeUpdate( "ALTER STORES DROP \"store1\"" );
            }
        }
    }

}
