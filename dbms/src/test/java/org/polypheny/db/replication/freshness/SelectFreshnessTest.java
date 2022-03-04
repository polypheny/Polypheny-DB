/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.replication.freshness;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.polypheny.db.TestHelper.JdbcConnection;


public class SelectFreshnessTest {

    // TODO @HENNLO ADD FreshnessManage rTest


    @Test
    public void testFreshnessDelay() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessdelay( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed

                    // Includes DELAY in SECONDS

                    // Includes DELAY in MINUTES

                    // Includes DELAY in HOURS

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessdelay" );
                }
            }
        }
    }


    @Test
    public void testFreshnessPercentage() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnesspercentage( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesspercentage" );
                }
            }
        }
    }


    @Test
    public void testFreshnessTime() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnesstime( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if queries can even be executed and are correctly parsed

                    // Includes Timestamps

                    // Includes Time

                    // Includes Date

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesstime" );
                }
            }
        }
    }

}
