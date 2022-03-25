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
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogTable;


@Category({ AdapterTestSuite.class })
public class LazyPlacementReplicationTest {

    @Test
    public void generalEagerReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE generaleagerreplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "generaleagerreplicationtest" ) ).get( 0 );

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ALL placements and partitions need to be locked

                    // Assert that they both have the correct number of updates received

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS generaleagerreplicationtest" );
                }
            }
        }
    }


    @Test
    public void generalLazyReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE generallazyreplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "generallazyreplicationtest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS generallazyreplicationtest" );
                }
            }
        }
    }


    @Test
    public void replicationAfterPlacementRoleChangeTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE replicationafterplacementrolechangetest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "replicationafterplacementrolechangetest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Assert that they both have the correct number of updates received

                    // Change role of one of the placements to REFRESHABLE

                    // Insert several updates

                    // Assert that they both have different  number of updates received

                    // Assert that the REFRESHABLE placement has still the same number of updates as before

                    // Alter REFRESHABLE placement back zo UPTODATE

                    // Assert that this placement should now immediately receive the delta

                    // Assert that they both have they both immediately have the same commit/update timestamp

                    // Assert that they both have the correct number of updates received and are equal

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS replicationafterplacementrolechangetest" );
                }
            }
        }
    }
}


