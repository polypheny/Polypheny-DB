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

package org.polypheny.db.jdbc;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class JdbcDmlTest {


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void multiInsertTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE multiinserttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    statement.executeUpdate( "INSERT INTO multiinserttest VALUES (1,1,'foo'), (2,5,'bar'), (3,7,'foobar')" );
                    statement.executeUpdate( "INSERT INTO multiinserttest(tprimary,tinteger,tvarchar) VALUES (4,6,'hans'), (5,3,'georg'), (6,2,'jack')" );

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM multiinserttest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, "foo" },
                                    new Object[]{ 2, 5, "bar" },
                                    new Object[]{ 3, 7, "foobar" },
                                    new Object[]{ 4, 6, "hans" },
                                    new Object[]{ 5, 3, "georg" },
                                    new Object[]{ 6, 2, "jack" } ) );
                    connection.commit();
                } finally {
                    // Drop table
                    statement.executeUpdate( "DROP TABLE multiinserttest" );
                }
            }
        }
    }


    @Test
    public void simpleTransactionTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try {
                Statement statement = connection.createStatement();
                statement.executeUpdate( "CREATE TABLE transactiontest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );
                statement.executeUpdate( "INSERT INTO transactiontest VALUES (1,1,'foo')" );
                connection.commit();

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM transactiontest ORDER BY tprimary" ),
                        ImmutableList.of( new Object[]{ 1, 1, "foo" } ) );

                statement = connection.createStatement();
                statement.executeUpdate( "INSERT INTO transactiontest VALUES (2,5,'bar')" );
                statement.executeUpdate( "INSERT INTO transactiontest VALUES (3,7,'foobar')" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM transactiontest ORDER BY tprimary" ),
                        ImmutableList.of(
                                new Object[]{ 1, 1, "foo" },
                                new Object[]{ 2, 5, "bar" },
                                new Object[]{ 3, 7, "foobar" } ) );
                connection.rollback();

                statement = connection.createStatement();
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM transactiontest ORDER BY tprimary" ),
                        ImmutableList.of( new Object[]{ 1, 1, "foo" } ) );
                connection.commit();

                // Same check again to make sure that the previous commit has not committed something which should
                // have been rolled back
                statement = connection.createStatement();
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM transactiontest ORDER BY tprimary" ),
                        ImmutableList.of( new Object[]{ 1, 1, "foo" } ) );
                connection.commit();
            } finally {
                Statement statement = connection.createStatement();
                statement.executeUpdate( "DROP TABLE transactiontest" );
                connection.commit();
            }
        }
    }


}
