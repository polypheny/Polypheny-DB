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

package org.polypheny.db.replication;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.polypheny.db.TestHelper.JdbcConnection;


public class IsolationTest {

    @Test
    public void testIsolationCriteriaNone() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testisolationnriterianone( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if IsolationLevel of TX is set to NONE automatically

                    // Make sure that TX automatically commits that we don't have any interference with shared locks

                } finally {
                    statement.executeUpdate( "DROP TABLE testisolationnriterianone" );
                }

            }
        }
    }


    public void testIsolationCriteriaForFreshness() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testisolationcriteriaforfreshness( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if IsolationLevel of TX is set to NONE automatically

                } finally {
                    statement.executeUpdate( "DROP TABLE testisolationcriteriaforfreshness" );
                }

            }
        }
    }


    public void testRegularIsolationCriteria() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( "CREATE TABLE testregularisolationcriteria( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {

                    // Check if IsolationLevel of TX is set to STRICT automatically

                } finally {
                    statement.executeUpdate( "DROP TABLE testregularisolationcriteria" );
                }

            }
        }
    }

}
