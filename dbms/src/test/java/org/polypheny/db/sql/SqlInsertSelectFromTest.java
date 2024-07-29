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

package org.polypheny.db.sql;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
public class SqlInsertSelectFromTest {

    private static final String CREATE_TABLE_TEST_1 =
            "CREATE TABLE IF NOT EXISTS select_insert_test_1 (" +
                    "ctid INTEGER NOT NULL, " +
                    "a INTEGER NOT NULL, " +
                    "b INTEGER NOT NULL, " +
                    "c INTEGER NOT NULL, " +
                    "PRIMARY KEY (ctid) )";

    private static final String CREATE_TABLE_TEST_2 =
            "CREATE TABLE IF NOT EXISTS select_insert_test_2 (" +
                    "ctid INTEGER NOT NULL, " +
                    "a INTEGER NOT NULL, " +
                    "b INTEGER NOT NULL, " +
                    "c INTEGER NOT NULL, " +
                    "PRIMARY KEY (ctid) )";


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void testSimpleInsertFrom() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( CREATE_TABLE_TEST_1 );
                    statement.executeUpdate( "INSERT INTO select_insert_test_1 VALUES (1, 1, 1, 1), (2, 2, 2, 2)" );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM select_insert_test_1 ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 }
                            )
                    );

                    statement.executeUpdate( CREATE_TABLE_TEST_2 );

                    int i = statement.executeUpdate( "INSERT INTO select_insert_test_2 SELECT * FROM select_insert_test_1" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM select_insert_test_2 ORDER BY ctid" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 1, 1, 1 },
                                    new Object[]{ 2, 2, 2, 2 }
                            )
                    );


                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS select_insert_test_1 " );
                    statement.executeUpdate( "DROP TABLE IF EXISTS select_insert_test_2" );
                }
            }
        }
    }

}
