/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.jdbc;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class JdbcBooleanArrayTest {

    @BeforeAll
    public static void start() throws SQLException {
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate(
                        "CREATE TABLE booleanarraytest( id INTEGER NOT NULL, bvec BOOLEAN ARRAY(1,3), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO booleanarraytest VALUES (1, ARRAY[TRUE, FALSE, TRUE])" );
                statement.executeUpdate( "INSERT INTO booleanarraytest VALUES (2, ARRAY[FALSE, FALSE, FALSE])" );
                statement.executeUpdate( "INSERT INTO booleanarraytest VALUES (3, ARRAY[TRUE, TRUE, TRUE])" );
                statement.executeUpdate( "INSERT INTO booleanarraytest VALUES (4, NULL)" );
                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE booleanarraytest" );
            }
        }
    }


    @Test
    void selectAllRowsDoesNotThrow() throws SQLException {
        // verify the BIT read path doesn't throw PSQLException.
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                ResultSet rs = statement.executeQuery( "SELECT * FROM booleanarraytest ORDER BY id" );
                int count = 0;
                while ( rs.next() ) {
                    count++;
                }
                assertEquals( 4, count );
            }
        }
    }


    @Test
    void countReturnsCorrectRowCount() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(id) FROM booleanarraytest" ),
                        ImmutableList.of( new Object[]{ 4L } )
                );
            }
        }
    }


    @Test
    void nullBooleanArrayIsInsertedAndReadBack() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(id) FROM booleanarraytest WHERE bvec IS NULL" ),
                        ImmutableList.of( new Object[]{ 1L } )
                );
            }
        }
    }


    @Test
    void filterByIdReturnsSingleRow() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id FROM booleanarraytest WHERE id = 1" ),
                        ImmutableList.of( new Object[]{ 1 } )
                );
            }
        }
    }


    @Test
    void singleBooleanArray() throws SQLException {
        // BOOLEAN ARRAY(1,1) - a 1-bit bitbooleanarray.
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate(
                        "CREATE TABLE booleanarray1test( id INTEGER NOT NULL, bvec BOOLEAN ARRAY(1,1), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO booleanarray1test VALUES (1, ARRAY[TRUE])" );
                statement.executeUpdate( "INSERT INTO booleanarray1test VALUES (2, ARRAY[FALSE])" );
                connection.commit();
            }
        } try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                ResultSet rs = statement.executeQuery( "SELECT * FROM booleanarray1test ORDER BY id" );
                int count = 0;
                while ( rs.next() ) count++;
                assertEquals( 2, count );
            }
        }
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE booleanarray1test" );
            }
        }
    }


    @Test
    void verifyDataIntegrity() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT bvec FROM booleanarraytest WHERE id = 1" ),
                        ImmutableList.of( new Object[]{ true, false, true } )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT bvec FROM booleanarraytest WHERE id = 2" ),
                        ImmutableList.of( new Object[]{ false, false, false } )
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT bvec FROM booleanarraytest WHERE id = 4" ),
                        ImmutableList.of( new Object[]{ null } )
                );
            }
        }
    }


    @Test
    void insertWithPreparedStatement() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( java.sql.PreparedStatement ps = connection.prepareStatement( "INSERT INTO booleanarraytest(id, bvec) VALUES (?, ?)" ) ) {
                ps.setInt( 1, 10 );
                Boolean[] bits = new Boolean[]{ true, true, false };
                java.sql.Array sqlArray = connection.createArrayOf( "BOOLEAN", bits );
                ps.setArray( 2, sqlArray );
                ps.executeUpdate();
                connection.commit();
            }
        }

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT bvec FROM booleanarraytest WHERE id = 10" ),
                        ImmutableList.of( new Object[]{ ImmutableList.of( true, true, false ) } )
                );
                statement.executeUpdate( "DELETE FROM booleanarraytest WHERE id = 10" );
                connection.commit();
            }
        }
    }

}
