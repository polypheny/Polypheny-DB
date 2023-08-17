/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
@Slf4j
@RunWith(Parameterized.class)
@Category(AdapterTestSuite.class)
public class JdbcAutoCommitTest {

    @Parameters(name = "Use Auto-Commit: {0}")
    public static Object[] data() {
        return new Object[]{ false, true };
    }


    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS autoCommit( "
            + "id INTEGER NOT NULL, "
            + "age INTEGER NULL, "
            + "name VARCHAR(20) NULL, "
            + "PRIMARY KEY (id) )";


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    private final boolean useAutoCommit;


    public JdbcAutoCommitTest( boolean useAutoCommit ) {
        this.useAutoCommit = useAutoCommit;
    }


    @Test
    public void testDDl() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( useAutoCommit ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( CREATE_TABLE );

                try ( JdbcConnection polyphenyDbConnection2 = new JdbcConnection( useAutoCommit ) ) {
                    Connection connection2 = polyphenyDbConnection2.getConnection();
                    try ( Statement statement2 = connection2.createStatement() ) {
                        // DDL are auto-commit anyway so it should not matter
                        TestHelper.checkResultSet(
                                statement2.executeQuery( "SELECT * FROM autoCommit" ), ImmutableList.of() );
                    }
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS autocommit" );
                }
            }
        }
    }


    @Test
    public void testDml() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( useAutoCommit ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( CREATE_TABLE );
                statement.executeUpdate( "INSERT INTO autoCommit VALUES(1, 28, 'David')" );

                try ( JdbcConnection polyphenyDbConnection2 = new JdbcConnection( useAutoCommit ) ) {
                    Connection connection2 = polyphenyDbConnection2.getConnection();
                    try ( Statement statement2 = connection2.createStatement() ) {

                        connection.rollback();

                        if ( useAutoCommit ) {
                            // insert still visible before rollback
                            TestHelper.checkResultSet(
                                    statement2.executeQuery( "SELECT * FROM autoCommit" ),
                                    ImmutableList.of( new Object[]{ 1, 28, "David" } ) );
                        } else {
                            TestHelper.checkResultSet(
                                    statement2.executeQuery( "SELECT * FROM autoCommit" ),
                                    ImmutableList.of() );
                        }

                    }
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS autocommit" );
                }
            }
        }
    }

}
