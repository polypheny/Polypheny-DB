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

package org.polypheny.db.misc;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class TriggerTest {

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
                statement.executeUpdate( "CREATE TABLE triggertest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // add view
                    statement.executeUpdate( "CREATE VIEW myView AS SELECT tprimary FROM triggertest" );

                    // add procedure
                    statement.executeUpdate( "CREATE PROCEDURE myProcedure @id=1, @name='John' $ sql(insert into triggertest VALUES(:id, 1, 'some'));$" );

                    // add trigger
                    statement.executeUpdate("CREATE TRIGGER myTrigger ON \"myTable\" after insert $ 'exec procedure myProcedure @id='");

                    // invoke insert
                    statement.executeUpdate( "INSERT INTO myView VALUES (1)" );

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM triggertest" ),
                            ImmutableList.of(
                                    new Object[]{ 1 } ) );
                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP PROCEDURE myProcedure" );
                    statement.executeUpdate( "DROP TRIGGER myTrigger" );
                    statement.executeUpdate( "DROP VIEW myView" );
                    statement.executeUpdate( "DROP TABLE datamigratortest" );
                }
            }
        }
    }
}
