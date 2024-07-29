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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
@Slf4j
@Tag("adapter")
@DisplayName("Use Auto-Commit: {0}")
public class JdbcAutoCommitTest {


    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS autoCommit( "
            + "id INTEGER NOT NULL, "
            + "age INTEGER NULL, "
            + "name VARCHAR(20) NULL, "
            + "PRIMARY KEY (id) )";


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @ParameterizedTest(name = "Auto-Committing: {0}")
    @ValueSource(booleans = { false, true })
    public void testDDl( boolean useAutoCommit ) throws SQLException {
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


    @ParameterizedTest(name = "Auto-Committing: {0}")
    @ValueSource(booleans = { false, true })
    public void testDml( boolean useAutoCommit ) throws SQLException {
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
