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
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class DataMigratorTest {


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
                statement.executeUpdate( "CREATE TABLE datamigratortest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );
                statement.executeUpdate( "INSERT INTO datamigratortest VALUES (1,5,'foo')" );

                // Add data store
                statement.executeUpdate( "ALTER STORES ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );
                // Add placement
                statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT ON STORE \"store1\"" );

                // Remove placement on initial store
                statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                // Checks
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM datamigratortest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 5, "foo" } ) );

                // Drop table and store
                statement.executeUpdate( "DROP TABLE datamigratortest" );
                statement.executeUpdate( "ALTER STORES DROP \"store1\"" );
            }
        }
    }


    @Test
    public void partialPlacementsTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE datamigratortest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );
                statement.executeUpdate( "INSERT INTO datamigratortest VALUES (1,5,'foo')" );

                // Add data store
                statement.executeUpdate( "ALTER STORES ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory}'" );

                // Add placement
                statement.executeUpdate( "ALTER TABLE \"datamigratortest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM datamigratortest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 5, "foo" } ) );

                // Remove tvarchar column placement on initial store
                statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM datamigratortest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 5, "foo" } ) );

                // Add tinteger column placement on hsqldb store
                statement.executeUpdate( "ALTER TABLE \"datamigratortest\" MODIFY PLACEMENT (tinteger,tvarchar) ON STORE \"store1\"" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * FROM datamigratortest" ),
                        ImmutableList.of(
                                new Object[]{ 1, 5, "foo" } ) );

                // Remove placement on initial store
                statement.executeUpdate( "ALTER TABLE \"datamigratortest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                // Drop table and store
                statement.executeUpdate( "DROP TABLE datamigratortest" );
                statement.executeUpdate( "ALTER STORES DROP \"store1\"" );
            }
        }
    }

}
