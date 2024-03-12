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

package org.polypheny.db.misc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Pattern;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
public class VerticalPartitioningTest {


    private static TestHelper helper;


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        helper = TestHelper.getInstance();
    }


    @BeforeEach
    public void beforeEach() {
        helper.randomizeCatalogIds();
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
                    // Deploy additional storeId
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );

                    // Change placement on initial storeId
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" MODIFY PLACEMENT (tinteger) ON STORE \"hsqldb\"" );

                    // Insert data
                    statement.executeUpdate( "INSERT INTO partitioningtest VALUES (1,5,'foo')" );

                    // Checks
                    /*TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM partitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" } ) );*/

                    statement.executeUpdate( "INSERT INTO partitioningtest VALUES (2,22,'bar'),(3,69,'xyz')" );

                    // Checks
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM partitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 5, "foo" },
                                    new Object[]{ 2, 22, "bar" },
                                    new Object[]{ 3, 69, "xyz" } ) );

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
                    // Drop table and storeId
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
                    // Deploy additional storeId
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"partitioningtest\" ADD PLACEMENT (tvarchar) ON STORE \"store1\"" );

                    // Change placement on initial storeId
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
                    // Drop table and storeId
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
                    LogicalTable table = Catalog.snapshot().rel().getTables( null, new Pattern( "verticaldataplacementtest" ) ).get( 0 );

                    // Check if initially as many DataPlacements are created as requested (one for each storeId)
                    assertEquals( 1, Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).size() );

                    AllocationPlacement dataPlacement = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).get( 0 );

                    // Check how many columnPlacements are added to the one DataPlacement
                    assertEquals( table.getColumnIds().size(), Catalog.snapshot().alloc().getColumns( dataPlacement.id ).size() );

                    // Check how many partitionPlacements are added to the one DataPlacement
                    assertEquals( 1, Catalog.snapshot().alloc().getPartitionsFromLogical( table.id ).size() );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" ADD PLACEMENT ON STORE \"anotherstore\"" );

                    // Check if we now have two  dataPlacements in table
                    table = Catalog.snapshot().rel().getTable( table.id ).orElseThrow();
                    assertEquals( 2, Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).size() );

                    // Modify columns on second storeId
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary) ON STORE anotherstore" );
                    List<AllocationPlacement> dataPlacements = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id );

                    long adapterId = -1;
                    long initialAdapterId = -1;
                    for ( AllocationPlacement dp : dataPlacements ) {
                        if ( Catalog.snapshot().getAdapter( dp.adapterId ).orElseThrow().uniqueName.equals( "anotherstore" ) ) {
                            adapterId = dp.adapterId;
                            assertEquals( 1, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                        } else {
                            initialAdapterId = dp.adapterId;
                        }
                    }

                    // MODIFY by adding single column on second storeId
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary, tvarchar) ON STORE anotherstore" );
                    dataPlacements = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id );
                    for ( AllocationPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            assertEquals( 2, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            assertEquals( 3, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                        }
                    }

                    // MODIFY by adding single column on first storeId
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tinteger) ON STORE hsqldb" );
                    dataPlacements = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id );
                    for ( AllocationPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            assertEquals( 2, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( adapterId, table.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id ).get( adapterId ).size() );
                            assertEquals( 1, Catalog.snapshot().alloc().getEntitiesOnAdapter( adapterId ).orElseThrow().size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            assertEquals( 2, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( initialAdapterId, table.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id ).get( initialAdapterId ).size() );
                            assertEquals( 1, Catalog.snapshot().alloc().getEntitiesOnAdapter( adapterId ).orElseThrow().size() );
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
                    assertTrue( failed );

                    // ADD single column on second storeId
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT ADD COLUMN tinteger ON STORE anotherstore" );
                    dataPlacements = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id );
                    for ( AllocationPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            assertEquals( 3, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                            //Assertions.assertEquals( 3, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( adapterId, table.id ).size() );
                            //Assertions.assertEquals( 3, Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id ).get( adapterId ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            assertEquals( 2, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( initialAdapterId, table.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id ).get( initialAdapterId ).size() );
                        }
                    }

                    // MODIFY first storeId and adding a full placement again
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary, tinteger, tvarchar) ON STORE hsqldb" );

                    // REMOVE single column on second storeId
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT DROP COLUMN tvarchar ON STORE anotherstore" );
                    dataPlacements = Catalog.snapshot().alloc().getPlacementsFromLogical( table.id );
                    for ( AllocationPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            assertEquals( 2, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( adapterId, table.id ).size() );
                            //Assertions.assertEquals( 2, Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id ).get( adapterId ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            assertEquals( 3, Catalog.snapshot().alloc().getColumns( dp.id ).size() );
                            //Assertions.assertEquals( 3, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( initialAdapterId, table.id ).size() );
                            //Assertions.assertEquals( 3, Catalog.snapshot().alloc().getColumnPlacementsByAdapters( table.id ).get( initialAdapterId ).size() );
                        }
                    }

                    assertEquals( 2, dataPlacements.size() );
                    // DROP STORE and verify number of dataPlacements
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" DROP PLACEMENT ON STORE \"anotherstore\"" );
                    assertEquals( 1, Catalog.snapshot().alloc().getPlacementsFromLogical( table.id ).size() );

                    //Check also if ColumnPlacements have been correctly removed
                    //Assertions.assertEquals( 0, Catalog.snapshot().alloc().getColumnPlacementsOnAdapterPerEntity( adapterId, table.id ).size() );
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

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'Hsqldb' AS 'Store'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" ADD PLACEMENT ON STORE \"anotherstore\"" );

                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tinteger) ON STORE anotherstore" );

                    // By executing the following statement, technically the column tinteger would not be present
                    // on any of the partitions of the placement anymore. Therefore, it has to fail and all placements should remain
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT DROP COLUMN tvarchar ON STORE hsqldb" );
                        fail();
                    } catch ( AvaticaSqlException e ) {
                        // empty on purpose
                    }

                    statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" MODIFY PLACEMENT (tprimary,tvarchar) ON STORE hsqldb" );

                    try {
                        statement.executeUpdate( "ALTER TABLE \"verticalDataPlacementTest\" DROP PLACEMENT ON STORE anotherstore" );
                        fail();
                    } catch ( AvaticaSqlException e ) {
                        // empty on purpose
                    }

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS verticalDataPlacementTest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP anotherstore" );
                }
            }
        }
    }

}
