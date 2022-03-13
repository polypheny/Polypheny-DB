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

    // TODO @HENNLO ADD FreshnessManager Test


    @Test
    public void testFreshnessDMLOperations() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessoperations( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // DML queries cannot go through if a Freshness Query has already been executed within TX

                    // Freshness Queries cannot be executed if DML operations have already been executed within TX

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessoperations" );
                }
            }
        }
    }


    /**
     * Checks if PRIMARY and SECONDARY are correctly locked
     */
    @Test
    public void testFreshnessLocking() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessLocking( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessLocking" );
                }
            }
        }
    }


    @Test
    public void testFreshnessFallback() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnessfallback( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if it falls back to primary placements

                    // Gets the correct results

                    // And Re-Acquires the locks on primary placements

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnessfallback" );
                }
            }
        }
    }


    @Test
    public void testGeneralFreshnessSelection() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testfreshnesspercentage( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesspercentage" );
                }
            }
        }
    }


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

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL values (negative time delay)

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

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL PERCENTAGE (out of bound)

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

                    // Test with WHERE clause

                    // Test with ORDER BY clause

                    // Test with HAVING clause

                    // Test ILLEGAL TIME (in future)

                    // Test ILLEGAL TIME (invalid Timestamp)

                    // Test ILLEGAL TIME (incomplete time)

                } finally {
                    statement.executeUpdate( "DROP TABLE testfreshnesstime" );
                }
            }
        }
    }

}
