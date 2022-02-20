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

package org.polypheny.db.misc;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.excluded.CassandraExcluded;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class VerticalPartitioningTest {


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
                statement.executeUpdate( "CREATE TABLE partitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Deploy additional store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );

                    // Change placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );

                    // Insert data
                    statement.executeUpdate( "INSERT INTO partitioningtest VALUES (1,5,'foo')" );
                    statement.executeUpdate( "INSERT INTO partitioningtest VALUES (2,22,'bar'),(3,69,'xyz')" );

                    // Update data
                    statement.executeUpdate( "UPDATE partitioningtest SET tinteger = 33 WHERE tprimary = 1" );
                    statement.executeUpdate( "UPDATE partitioningtest SET tprimary = 4 WHERE tprimary = 2" );

                    // Delete data
                    statement.executeUpdate( "DELETE FROM partitioningtest WHERE tprimary = 3" );

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM partitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 33, "foo" },
                                    new Object[]{ 4, 22, "bar" } ) );
                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP TABLE partitioningtest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store1\"" );
                }
            }
        }
    }


    @Test
    public void preparedBatchTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE partitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Deploy additional store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );

                    // Change placement on initial store
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );

                    // Insert Data
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO partitioningtest VALUES (?, ?, ?)" );
                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setInt( 2, 33 );
                    preparedInsert.setString( 3, "foo" );
                    preparedInsert.addBatch();
                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setInt( 2, 22 );
                    preparedInsert.setString( 3, "bar" );
                    preparedInsert.addBatch();
                    preparedInsert.setInt( 1, 3 );
                    preparedInsert.setInt( 2, 69 );
                    preparedInsert.setString( 3, "foobar" );
                    preparedInsert.addBatch();
                    preparedInsert.executeBatch();

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM partitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 33, "foo" },
                                    new Object[]{ 2, 22, "bar" },
                                    new Object[]{ 3, 69, "foobar" } ) );
                } finally {
                    // Drop table and store
                    statement.executeUpdate( "DROP TABLE partitioningtest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store1\"" );
                }
            }
        }
    }


    @Test
    public void dataPlacementTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE verticalDataPlacementTest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "verticaldataplacementtest" ) ).get( 0 );

                    // Check if initially as many DataPlacements are created as requested (one for each store)
                    Assert.assertEquals( 1, table.dataPlacements.size() );

                    CatalogDataPlacement dataPlacement = Catalog.getInstance().getDataPlacement( table.dataPlacements.get( 0 ), table.id );

                    // Check how many columnPlacements are added to the one DataPlacement
                    Assert.assertEquals( table.columnIds.size(), dataPlacement.columnPlacementsOnAdapter.size() );

                    // Check how many partitionPlacements are added to the one DataPlacement
                    Assert.assertEquals( 1, dataPlacement.partitionPlacementsOnAdapter.size() );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" ADD PLACEMENT ON STORE \"anotherstore\"" );

                    // Check if we now have two  dataPlacements in table
                    table = Catalog.getInstance().getTable( table.id );
                    Assert.assertEquals( 2, Catalog.getInstance().getDataPlacements( table.id ).size() );

                    // Modify columns on second store
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary) ON STORE anotherstore" );
                    List<CatalogDataPlacement> dataPlacements = Catalog.getInstance().getDataPlacements( table.id );

                    int adapterId = -1;
                    int initialAdapterId = -1;
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.getAdapterName().equals( "anotherstore" ) ) {
                            Assert.assertEquals( 1, dp.columnPlacementsOnAdapter.size() );
                            adapterId = dp.adapterId;
                            Assert.assertEquals( 1, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                        } else {
                            initialAdapterId = dp.adapterId;
                        }
                    }

                    // MODIFY by adding single column on second store
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary, tvarchar) ON STORE anotherstore" );
                    dataPlacements = Catalog.getInstance().getDataPlacements( table.id );
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            Assert.assertEquals( 2, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            Assert.assertEquals( 3, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( initialAdapterId, table.id ).size() );
                        }
                    }

                    // MODIFY by adding single column on first store
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tinteger) ON STORE hsqldb" );
                    dataPlacements = Catalog.getInstance().getDataPlacements( table.id );
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            Assert.assertEquals( 2, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsByAdapter( table.id ).get( adapterId ).size() );
                            Assert.assertEquals( 1, Catalog.getInstance().getPartitionPlacementsByAdapter( table.id ).get( adapterId ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            Assert.assertEquals( 2, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( initialAdapterId, table.id ).size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsByAdapter( table.id ).get( initialAdapterId ).size() );
                            Assert.assertEquals( 1, Catalog.getInstance().getPartitionPlacementsByAdapter( table.id ).get( initialAdapterId ).size() );
                        }
                    }

                    // By executing the following statement, technically the column tprimary would not be present
                    // on any DataPlacement anymore. Therefore, it has to fail and all placements should remain
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary) ON STORE hsqldb" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // ADD single column on second store
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT ADD COLUMN tinteger ON STORE anotherstore" );
                    dataPlacements = Catalog.getInstance().getDataPlacements( table.id );
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            Assert.assertEquals( 3, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getColumnPlacementsByAdapter( table.id ).get( adapterId ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            Assert.assertEquals( 2, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( initialAdapterId, table.id ).size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsByAdapter( table.id ).get( initialAdapterId ).size() );
                        }
                    }

                    // MODIFY first store and adding a full placement again
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary, tinteger, tvarchar) ON STORE hsqldb" );

                    // REMOVE single column on second store
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT DROP COLUMN tvarchar ON STORE anotherstore" );
                    dataPlacements = Catalog.getInstance().getDataPlacements( table.id );
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            Assert.assertEquals( 2, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsByAdapter( table.id ).get( adapterId ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            Assert.assertEquals( 3, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( initialAdapterId, table.id ).size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getColumnPlacementsByAdapter( table.id ).get( initialAdapterId ).size() );
                        }
                    }

                    Assert.assertEquals( 2, dataPlacements.size() );
                    // DROP STORE and verify number of dataPlacements
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" DROP PLACEMENT ON STORE \"anotherstore\"" );
                    Assert.assertEquals( 1, Catalog.getInstance().getDataPlacements( table.id ).size() );

                    //Check also if ColumnPlacements have been correctly removed
                    Assert.assertEquals( 0, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS verticalDataPlacementTest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP anotherstore" );
                }
            }
        }
    }


    @Test
    public void dataDistributionTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE verticalDataPlacementTest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "verticaldataplacementtest" ) ).get( 0 );

                    CatalogDataPlacement dataPlacement = Catalog.getInstance().getDataPlacement( table.dataPlacements.get( 0 ), table.id );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" ADD PLACEMENT ON STORE \"anotherstore\"" );

                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tinteger) ON STORE anotherstore" );

                    // By executing the following statement, technically the column tprimary would not be present
                    // on any DataPlacement anymore. Therefore, it has to fail and all placements should remain
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT DROP COLUMN tvarchar ON STORE hsqldb" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary,tvarchar) ON STORE hsqldb" );

                    failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" DROP PLACEMENT ON STORE anotherstore" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS verticalDataPlacementTest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP anotherstore" );
                }
            }
        }
    }

}
