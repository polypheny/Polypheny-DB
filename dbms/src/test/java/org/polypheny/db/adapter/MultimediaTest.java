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

package org.polypheny.db.adapter;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
@Slf4j
public class MultimediaTest {


    private static TestHelper helper;


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        helper = TestHelper.getInstance();

        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mm\" USING 'Hsqldb' AS 'Store'"
                        + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void end() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS DROP mm" );
                connection.commit();
            }
        }
        helper.checkAllTrxClosed();
    }


    @Test
    public void testMultimediaFiles() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE preparedTest (a INTEGER NOT NULL, b INTEGER, PRIMARY KEY (a))" );

                    preparedTest( connection );
                    batchTest( connection );

                    // check inserts
                    int insertCount = statement.executeUpdate( "INSERT INTO preparedTest (a,b) VALUES (1,2),(3,4),(5,null)" );
                    assertEquals( 3, insertCount );
                    // insert only into one column
                    insertCount = statement.executeUpdate( "INSERT INTO preparedTest (a) VALUES (6)" );
                    assertEquals( 1, insertCount );

                    // test conditions
                    ResultSet rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE a = 3" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 3, 4 } ) );
                    rs.close();
                    // test prepared select
                    PreparedStatement preparedStatement = connection.prepareStatement( "SELECT * FROM preparedTest  WHERE a = ?" );
                    preparedStatement.setInt( 1, 1 );
                    rs = preparedStatement.executeQuery();
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 1, 2 } ) );
                    rs.close();
                    preparedStatement.close();

                    rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b = 4" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 3, 4 } ) );
                    rs.close();

                    // update
                    statement.executeUpdate( "UPDATE preparedTest SET b = 5 WHERE b = 4" );//b=6 where b = 5
                    // prepared update
                    preparedStatement = connection.prepareStatement( "UPDATE preparedTest SET b = 6 WHERE b = ?" );
                    preparedStatement.setInt( 1, 5 );
                    preparedStatement.executeUpdate();
                    preparedStatement.close();
                    // check updated value
                    rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b = 6" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 3, 6 } ) );
                    rs.close();

                    // is null
                    rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b IS NULL ORDER BY a" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 5, null }, new Object[]{ 6, null } ) );
                    rs.close();
                    // x = null should always return false
                    rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE b = NULL" );
                    TestHelper.checkResultSet( rs, ImmutableList.of() );
                    rs.close();
                    // check greater equals and check that prepared and batch inserts work
                    rs = statement.executeQuery( "SELECT * FROM preparedTest  WHERE a >= 10 ORDER BY a" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 10, 20 }, new Object[]{ 11, 21 }, new Object[]{ 12, 22 } ) );
                    rs.close();

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE public.preparedTest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testDateTime() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE testDateTime (a INTEGER NOT NULL, b DATE, c TIME, d TIMESTAMP, PRIMARY KEY (a))" );

                    PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO testDateTime (a,b,c,d) VALUES (?,?,?,?)" );
                    preparedStatement.setInt( 1, 1 );
                    preparedStatement.setDate( 2, Date.valueOf( LocalDate.now() ) );
                    preparedStatement.setTime( 3, Time.valueOf( LocalTime.now() ) );
                    preparedStatement.setTimestamp( 4, Timestamp.valueOf( LocalDateTime.now() ) );
                    preparedStatement.addBatch();
                    preparedStatement.clearParameters();
                    preparedStatement.setInt( 1, 2 );
                    preparedStatement.setDate( 2, Date.valueOf( LocalDate.now() ) );
                    preparedStatement.setTime( 3, Time.valueOf( LocalTime.now() ) );
                    preparedStatement.setTimestamp( 4, Timestamp.valueOf( LocalDateTime.now() ) );
                    preparedStatement.addBatch();
                    preparedStatement.executeBatch();
                    preparedStatement.close();

                    connection.commit();
                } finally {
                    statement.executeUpdate( "DROP TABLE public.testDateTime" );
                    connection.commit();
                }
            }
        }
    }


    private void preparedTest( final Connection connection ) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO preparedTest (a,b) VALUES (?,?)" );
        preparedStatement.setInt( 1, 10 );
        preparedStatement.setInt( 2, 20 );
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }


    private void batchTest( final Connection connection ) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement( "INSERT INTO preparedTest (a,b) VALUES (?,?)" );
        preparedStatement.setInt( 1, 11 );
        preparedStatement.setInt( 2, 21 );
        preparedStatement.addBatch();
        preparedStatement.clearParameters();
        preparedStatement.setInt( 1, 12 );
        preparedStatement.setInt( 2, 22 );
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        preparedStatement.close();
    }


    @Test
    public void testFile() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE public.bin (id INTEGER NOT NULL, blb FILE, PRIMARY KEY (id))" );

                try {
                    PreparedStatement ps = connection.prepareStatement( "INSERT INTO public.bin VALUES (?,x'6869')" );
                    ps.setInt( 1, 1 );
                    ps.executeUpdate();
                    ps.close();
                    ps = connection.prepareStatement( "INSERT INTO public.bin VALUES (?,?)" );
                    ps.setInt( 1, 2 );
                    ps.setBytes( 2, "hello".getBytes() );
                    ps.executeUpdate();
                    ps.close();
                    ResultSet rs = statement.executeQuery( "SELECT id, blb FROM public.bin ORDER BY id ASC" );
                    TestHelper.checkResultSet( rs, ImmutableList.of(
                            new Object[]{ 1, "hi".getBytes() },
                            new Object[]{ 2, "hello".getBytes() }
                    ) );
                    rs.close();

                    //retrieve a single multimedia item
                    rs = statement.executeQuery( "SELECT blb FROM public.bin WHERE id = 1" );
                    TestHelper.checkResultSet( rs, ImmutableList.of(
                            new Object[]{ "hi".getBytes() }
                    ) );
                    rs.close();
                } finally {
                    statement.executeUpdate( "DROP TABLE public.bin" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void verticalPartitioningTest() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE public.partitioned ( id INTEGER NOT NULL, username VARCHAR(20), pic FILE, PRIMARY KEY(id))" );
                try {
                    statement.executeUpdate( "ALTER TABLE public.partitioned ADD PLACEMENT (username) ON STORE \"hsqldb\"" );
                    statement.executeUpdate( "ALTER TABLE public.partitioned MODIFY PLACEMENT (pic) ON STORE \"mm\"" );

                    PreparedStatement ps = connection.prepareStatement( "INSERT INTO public.partitioned (id, username, pic) VALUES(?,?,?)" );
                    ps.setInt( 1, 1 );
                    ps.setString( 2, "user1" );
                    ps.setBytes( 3, "polypheny".getBytes() );
                    ps.addBatch();
                    ps.clearParameters();
                    ps.setInt( 1, 2 );
                    ps.setString( 2, "user2" );
                    ps.setBytes( 3, "basel".getBytes() );
                    ps.addBatch();
                    ps.executeBatch();

                    ResultSet rs = statement.executeQuery( "SELECT username, pic FROM public.partitioned WHERE id = 1" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ "user1", "polypheny".getBytes() } ) );
                    rs.close();
                    rs = statement.executeQuery( "SELECT username, pic FROM public.partitioned WHERE id = 2" );
                    TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ "user2", "basel".getBytes() } ) );
                    rs.close();
                } finally {
                    statement.executeUpdate( "DROP TABLE public.partitioned" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testRollback() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            Statement statement = connection.createStatement();
            try {
                statement.executeUpdate( "CREATE TABLE testRollback (a INTEGER NOT NULL, b INTEGER NOT NULL, PRIMARY KEY(a))" );
                statement.executeUpdate( "INSERT INTO testRollback (a,b) VALUES (1,2)" );
                connection.commit();
                statement.close();

                connection = jdbcConnection.getConnection();
                statement = connection.createStatement();
                statement.executeUpdate( "INSERT INTO testRollback (a,b) VALUES (3,4)" );
                connection.rollback();
                statement.close();

                connection = jdbcConnection.getConnection();
                statement = connection.createStatement();
                ResultSet rs = statement.executeQuery( "SELECT a,b FROM testRollback" );
                TestHelper.checkResultSet( rs, ImmutableList.of( new Object[]{ 1, 2 } ) );
                rs.close();
                connection.commit();
                statement.close();

            } finally {
                connection = jdbcConnection.getConnection();
                statement = connection.createStatement();
                statement.executeUpdate( "DROP TABLE testRollback" );
                connection.commit();
            }
        }
    }

}
