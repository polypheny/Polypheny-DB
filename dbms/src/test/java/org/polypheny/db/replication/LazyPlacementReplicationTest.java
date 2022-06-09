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


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.ConfigManager;


@Category({ AdapterTestSuite.class })
@Ignore
public class LazyPlacementReplicationTest {

    private final CountDownLatch waiter = new CountDownLatch( 1 );


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
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "generaleagerreplicationtest" ) ).get( 0 );

                    // Create two placements for one table
                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE generaleagerreplicationtest "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 " );

                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO generaleagerreplicationtest VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO generaleagerreplicationtest VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM generaleagerreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );

                    // Get the single DataPlacement
                    List<CatalogDataPlacement> allDataPlacements = Catalog.getInstance().getDataPlacements( entity.id );

                    List<CatalogPartitionPlacement> allPartitionPlacements = Catalog.getInstance().getPartitionPlacements( entity.id );

                    // Check locking: ALL placements and partitions need to be locked
                    long txId = allPartitionPlacements.get( 0 ).updateInformation.txId;
                    long commitTimestamp = allPartitionPlacements.get( 0 ).updateInformation.commitTimestamp;
                    long updateTimestamp = allPartitionPlacements.get( 0 ).updateInformation.updateTimestamp;
                    long modifications = allPartitionPlacements.get( 0 ).updateInformation.modifications;

                    // Assert that they both have the correct number of updates received
                    for ( CatalogPartitionPlacement partitionPlacement : allPartitionPlacements ) {
                        Assert.assertEquals( txId, partitionPlacement.updateInformation.txId );
                        Assert.assertEquals( commitTimestamp, partitionPlacement.updateInformation.commitTimestamp );
                        Assert.assertEquals( updateTimestamp, partitionPlacement.updateInformation.updateTimestamp );
                        Assert.assertEquals( modifications, partitionPlacement.updateInformation.modifications );
                    }


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS generaleagerreplicationtest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
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

                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "generallazyreplicationtest" ) ).get( 0 );

                    // Create two placements for one table
                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    CatalogAdapter store1 = Catalog.getInstance().getAdapter( "store1" );

                    statement.executeUpdate( "ALTER TABLE generallazyreplicationtest "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );

                    // Disable automatic refresh operations to validate the deviation
                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( false );

                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO generallazyreplicationtest VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO generallazyreplicationtest VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM generallazyreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );

                    LazyReplicationEngine lazyReplicationEngine = (LazyReplicationEngine) ReplicationEngineProvider.getInstance().getReplicationEngine( ReplicationStrategy.LAZY );

                    List<CatalogPartitionPlacement> allPartitionPlacements = Catalog.getInstance().getAllPartitionPlacementsByTable( entity.id );

                    // Check that queue is correctly enriched with two INSERTS
                    Assert.assertEquals( 2, lazyReplicationEngine.getPendingReplicationsPerPlacementSize( store1.id, allPartitionPlacements.get( 0 ).partitionId ) );

                    // Get the single DataPlacement
                    List<CatalogDataPlacement> allDataPlacements = Catalog.getInstance().getDataPlacements( entity.id );

                    // Check locking: NOT all placements and partitions need to be locked

                    long txId = 0;
                    long commitTimestamp = 0;
                    long updateTimestamp = 0;
                    long modifications = 0;

                    boolean firstIteration = true;
                    // Assert that they both have different number of updates received
                    for ( CatalogPartitionPlacement partitionPlacement : allPartitionPlacements ) {

                        if ( firstIteration ) {
                            txId = allPartitionPlacements.get( 0 ).updateInformation.txId;
                            commitTimestamp = allPartitionPlacements.get( 0 ).updateInformation.commitTimestamp;
                            updateTimestamp = allPartitionPlacements.get( 0 ).updateInformation.updateTimestamp;
                            modifications = allPartitionPlacements.get( 0 ).updateInformation.modifications;
                            continue;
                        }

                        // Sine one placement is eager and the other one is lazy, there update timestamps should be different
                        Assert.assertFalse( txId != partitionPlacement.updateInformation.txId );
                        Assert.assertFalse( commitTimestamp != partitionPlacement.updateInformation.commitTimestamp );
                        Assert.assertFalse( updateTimestamp != partitionPlacement.updateInformation.updateTimestamp );
                        Assert.assertFalse( modifications != partitionPlacement.updateInformation.modifications );
                    }

                    // Enable automatic data Replication again
                    c1.setBoolean( true );

                    // Now updates should be all equal
                    for ( CatalogPartitionPlacement partitionPlacement : allPartitionPlacements ) {
                        Assert.assertEquals( txId, partitionPlacement.updateInformation.txId );
                        Assert.assertEquals( commitTimestamp, partitionPlacement.updateInformation.commitTimestamp );
                        Assert.assertEquals( updateTimestamp, partitionPlacement.updateInformation.updateTimestamp );
                        Assert.assertEquals( modifications, partitionPlacement.updateInformation.modifications );
                    }

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM generallazyreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );

                    // Check if first initial primary store ( which received updates eagerly ) is dropped. If
                    // Secondary still has all updates
                    statement.executeUpdate( "ALTER TABLE generallazyreplicationtest "
                            + "MODIFY PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION EAGER" );

                    statement.executeUpdate( "ALTER TABLE generallazyreplicationtest "
                            + "DROP PLACEMENT "
                            + "ON STORE hsqldb " );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM generallazyreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );

                    // Check locking: ONLY Primary placements and partitions need to be locked

                } catch ( UnknownAdapterException e ) {
                    e.printStackTrace();
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS generallazyreplicationtest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
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
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "replicationafterplacementrolechangetest" ) ).get( 0 );

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


    @Test
    public void replicationConstraintTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE replicationcontrainttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "replicationcontrainttest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS replicationcontrainttest" );
                }
            }
        }
    }


    @Test
    public void preparedReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE preparedreplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "preparedreplicationtest" ) ).get( 0 );


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS preparedreplicationtest" );
                }
            }
        }
    }


    @Test
    public void insertReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE insertreplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "insertreplicationtest" ) ).get( 0 );

                    LazyReplicationEngine lazyReplicationEngine = (LazyReplicationEngine) ReplicationEngineProvider.getInstance().getReplicationEngine( ReplicationStrategy.LAZY );

                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER TABLE insertreplicationtest "
                            + "ADD PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION LAZY" );

                    CatalogAdapter store1 = Catalog.getInstance().getAdapter( "store1" );

                    ConfigManager cm = ConfigManager.getInstance();
                    Config c1 = cm.getConfig( "replication/automaticLazyDataReplication" );
                    c1.setBoolean( true );

                    // Insert several MODIFICATIONS
                    statement.executeUpdate( "INSERT INTO insertreplicationtest VALUES (1, 30, 'foo')" );
                    statement.executeUpdate( "INSERT INTO insertreplicationtest VALUES (2, 70, 'bar')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM insertreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );

                    List<CatalogPartitionPlacement> allPartitionPlacements = Catalog.getInstance().getAllPartitionPlacementsByTable( entity.id );

                    // Check that queue is correctly enriched with two INSERTS
                    Assert.assertEquals( 2, lazyReplicationEngine.getPendingReplicationsPerPlacementSize( store1.id, allPartitionPlacements.get( 0 ).partitionId ) );

                    // Get the single DataPlacement
                    List<CatalogDataPlacement> allDataPlacements = Catalog.getInstance().getDataPlacements( entity.id );

                    // Check locking: NOT all placements and partitions need to be locked

                    long txId = allPartitionPlacements.get( 0 ).updateInformation.txId;
                    long commitTimestamp = allPartitionPlacements.get( 0 ).updateInformation.commitTimestamp;
                    long updateTimestamp = allPartitionPlacements.get( 0 ).updateInformation.updateTimestamp;
                    long modifications = allPartitionPlacements.get( 0 ).updateInformation.modifications;

                    // Now updates should be all equal
                    for ( CatalogPartitionPlacement partitionPlacement : allPartitionPlacements ) {
                        Assert.assertEquals( txId, partitionPlacement.updateInformation.txId );
                        Assert.assertEquals( commitTimestamp, partitionPlacement.updateInformation.commitTimestamp );
                        Assert.assertEquals( updateTimestamp, partitionPlacement.updateInformation.updateTimestamp );
                        Assert.assertEquals( modifications, partitionPlacement.updateInformation.modifications );
                    }

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM insertreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );

                    // Check if first initial primary store ( which received updates eagerly ) is dropped. If
                    // Secondary still has all updates
                    statement.executeUpdate( "ALTER TABLE insertreplicationtest "
                            + "MODIFY PLACEMENT "
                            + "ON STORE store1 "
                            + "WITH REPLICATION EAGER" );

                    statement.executeUpdate( "ALTER TABLE insertreplicationtest "
                            + "DROP PLACEMENT "
                            + "ON STORE hsqldb " );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM insertreplicationtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 30, "foo" },
                                    new Object[]{ 2, 70, "bar" } ) );


                } catch ( UnknownAdapterException e ) {
                    e.printStackTrace();
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS insertreplicationtest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }


    @Test
    public void updateReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE updatereplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "updatereplicationtest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS updatereplicationtest" );
                }
            }
        }
    }


    @Test
    public void deleteReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE deletereplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "deletereplicationtest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS deletereplicationtest" );
                }
            }
        }
    }


    @Test
    public void replicationWithVerticalPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE replicationwithverticalpartitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "replicationwithverticalpartitioningtest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS replicationwithverticalpartitioningtest" );
                }
            }
        }
    }


    @Test
    public void replicationWithHorizontalPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE replicationwithhorizontalpartitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "replicationwithhorizontalpartitioningtest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS replicationwithhorizontalpartitioningtest" );
                }
            }
        }
    }


    @Test
    public void replicationWithHybridPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE replicationwithhybridpartitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "replicationwithhybridpartitioningtest" ) ).get( 0 );

                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS replicationwithhybridpartitioningtest" );
                }
            }
        }
    }


    @Test
    public void multiInsertReplicationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE multiinsertreplicationtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogEntity entity = Catalog.getInstance().getTables( null, null, new Pattern( "multiinsertreplicationtest" ) ).get( 0 );

                    statement.executeUpdate( "INSERT INTO multiinsert(tprimary,tvarchar,tinteger) VALUES (1,'Hans',5),(2,'Eva',7),(3,'Alice',89)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM multiinsert ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Hans", 5 },
                                    new Object[]{ 2, "Eva", 7 },
                                    new Object[]{ 3, "Alice", 89 } ) );
                    // Disable automatic refresh operations to validate the deviation

                    // Create two placements for one table

                    // Insert several updates

                    // Check locking: ONLY Primary placements and partitions need to be locked

                    // Assert that they both have the received different numbers of updates

                    // Assert that they both data placements have different commit/update timestamps

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS multiinsertreplicationtest" );
                }
            }
        }
    }
}


