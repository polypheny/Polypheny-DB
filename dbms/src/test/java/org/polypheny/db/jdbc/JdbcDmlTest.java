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

package org.polypheny.db.jdbc;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class JdbcDmlTest {


    @BeforeClass
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
                            statement.executeQuery( "SELECT * FROM multiinserttest" ),
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


}
