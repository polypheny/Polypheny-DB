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
import java.util.ArrayList;
import java.util.Arrays;
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
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.partition.PartitionManager;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.TemperaturePartitionProperty;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;


@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class HorizontalPartitioningTest {

    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void basicHorizontalPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE horizontalparttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Partition table after creation
                    statement.executeUpdate( "ALTER TABLE horizontalparttest "
                            + "PARTITION BY HASH (tinteger) "
                            + "PARTITIONS 4" );

                    // Cannot partition a table that has already been partitioned
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE horizontalparttest "
                                + "PARTITION BY HASH (tinteger) "
                                + "PARTITIONS 2" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // check assert False. Wrong partition column
                    failed = false;
                    try {
                        statement.executeUpdate( "CREATE TABLE horizontalparttestfalsepartition( "
                                + "tprimary INTEGER NOT NULL, "
                                + "tinteger INTEGER NULL, "
                                + "tvarchar VARCHAR(20) NULL, "
                                + "PRIMARY KEY (tprimary) )"
                                + "PARTITION BY HASH (othercolumn) "
                                + "PARTITIONS 3" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE horizontalparttest" );
                    //statement.executeUpdate( "DROP TABLE horizontalparttestfalsepartition" );
                }
            }
        }
    }


    @Test
    public void modifyPartitionTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE horizontalparttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 3" );

                try {
                    // Deploy additional store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store3\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttest\" ADD PLACEMENT (tvarchar) ON STORE \"store3\"" );

                    //Modify partitions on new placement
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttest\" MODIFY PARTITIONS (0,1) ON STORE \"store3\" " );

                    //AsserTFalse
                    //Modify partitions out of index error
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE \"horizontalparttest\" MODIFY PARTITIONS (0,1,4) ON STORE \"store1\" " );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    //Create another table with initial partitioning
                    statement.executeUpdate( "CREATE TABLE horizontalparttestextension( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "PARTITIONS 3" );

                    // Deploy additional store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store2\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add placement for second table
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" ADD PLACEMENT (tvarchar) ON STORE \"store2\"" );

                    statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MERGE PARTITIONS" );

                    // DROP Table to repartition
                    statement.executeUpdate( "DROP TABLE \"horizontalparttestextension\" " );

                    // Partition by name
                    statement.executeUpdate( "CREATE TABLE horizontalparttestextension( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tinteger) "
                            + " WITH (name1, name2, name3)" );

                    // Add placement for second table
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" ADD PLACEMENT (tvarchar) ON STORE \"store2\"" );

                    // name partitioning can be modified with index
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MODIFY PARTITIONS (1) ON STORE \"store2\" " );

                    // name partitioning can be modified with name
                    statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MODIFY PARTITIONS (name2, name3) ON STORE \"store2\" " );

                    // check assert False. modify with false name no partition exists with name22
                    failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE \"horizontalparttestextension\" MODIFY PARTITIONS (name22) ON STORE \"store2\" " );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE horizontalparttestextension" );
                    statement.executeUpdate( "DROP TABLE horizontalparttest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store3\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"store2\"" );
                }
            }
        }
    }


    @Test
    public void alterColumnsPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE columnparttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Partition table after creation
                    statement.executeUpdate( "ALTER TABLE columnparttest "
                            + "PARTITION BY HASH (tinteger) "
                            + "PARTITIONS 4" );

                    statement.executeUpdate( "ALTER TABLE columnparttest ADD COLUMN newColumn BIGINT" );

                    statement.executeUpdate( "ALTER TABLE columnparttest RENAME COLUMN newColumn to veryNewColumn" );

                    statement.executeUpdate( "ALTER TABLE columnparttest DROP COLUMN veryNewColumn " );


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE columnparttest" );
                }
            }
        }
    }


    // Check if partitions have enough partitions
    @Test
    public void partitionNumberTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // invalid partition size
                boolean failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE horizontalparttestfalseNEW( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "PARTITIONS 1" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                // assert false partitioning only with partition name is not allowed
                failed = false;
                try {
                    statement.executeUpdate( "CREATE TABLE horizontal2( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY HASH (tvarchar) "
                            + "WITH (name1)" );
                } catch ( AvaticaSqlException e ) {
                    failed = true;
                }
                Assert.assertTrue( failed );

                statement.executeUpdate( "DROP TABLE horizontalparttestfalseNEW" );
                statement.executeUpdate( "DROP TABLE horizontal2" );
            }
        }
    }


    @Test
    public void dataMigrationTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE TABLE hashpartition( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER , "
                            + "tvarchar VARCHAR(20) , "
                            + "PRIMARY KEY (tprimary) )" );

                    statement.executeUpdate( "INSERT INTO hashpartition VALUES (1, 3, 'hans')" );
                    statement.executeUpdate( "INSERT INTO hashpartition VALUES (2, 7, 'bob')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hashpartition ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 7, "bob" } ) );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"storehash\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" ADD PLACEMENT (tprimary, tinteger, tvarchar) ON STORE \"storehash\"" );

                    statement.executeUpdate( "ALTER TABLE hashpartition "
                            + "PARTITION BY HASH (tvarchar) "
                            + "PARTITIONS 3" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hashpartition ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 7, "bob" } ) );

                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" MERGE PARTITIONS" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hashpartition ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 7, "bob" } ) );

                    //Combined with verticalPartitioning

                    statement.executeUpdate( "ALTER TABLE hashpartition MODIFY PLACEMENT"
                            + " DROP COLUMN tvarchar ON STORE storehash" );

                    statement.executeUpdate( "ALTER TABLE hashpartition MODIFY PLACEMENT"
                            + " DROP COLUMN tinteger ON STORE hsqldb" );

                    statement.executeUpdate( "ALTER TABLE hashpartition "
                            + "PARTITION BY HASH (tvarchar) "
                            + "PARTITIONS 3" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hashpartition ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 7, "bob" } ) );

                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" MERGE PARTITIONS" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hashpartition ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 7, "bob" } ) );

                } finally {
                    statement.executeUpdate( "DROP TABLE hashpartition" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"storehash\"" );
                }
            }
        }
    }


    @Test
    public void hashPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // Create basic setup
                statement.executeUpdate( "CREATE TABLE hashpartition( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 3" );

                try {
                    //AsserTFalse
                    //HASH Partitioning cant be created using values
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "CREATE TABLE hashpartitioning( "
                                + "tprimary INTEGER NOT NULL, "
                                + "tinteger INTEGER NULL, "
                                + "tvarchar VARCHAR(20) NULL, "
                                + "PRIMARY KEY (tprimary) )"
                                + "PARTITION BY HASH (tvarchar) "
                                + "( PARTITION parta VALUES('abc'), "
                                + "PARTITION partb VALUES('def'))" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"storehash\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" ADD PLACEMENT ON STORE \"storehash\"" );

                    // Change placement on second store
                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" MODIFY PARTITIONS (0,1) ON STORE \"storehash\"" );

                    statement.executeUpdate( "ALTER TABLE \"hashpartition\" MERGE PARTITIONS" );

                    // You can't change the distribution unless there exists at least one full partition placement of each column as a fallback
                    failed = false;
                    try {
                        statement.executeUpdate( "CREATE TABLE hashpartitioningValidate( "
                                + "tprimary INTEGER NOT NULL, "
                                + "tinteger INTEGER NULL, "
                                + "tvarchar VARCHAR(20) NULL, "
                                + "PRIMARY KEY (tprimary) )"
                                + "PARTITION BY HASH (tvarchar) "
                                + "( PARTITION parta VALUES('abc'), "
                                + "PARTITION partb VALUES('def'))" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );
                } finally {
                    statement.executeUpdate( "DROP TABLE hashpartition" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS hashpartitioning" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS hashpartitioningvalidate" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"storehash\"" );
                }
            }
        }
    }


    @Test
    public void listPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                //AsserTFalse
                //LIST Partitioning should be created using values
                statement.executeUpdate( "CREATE TABLE listpartitioning( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY LIST (tvarchar) "
                        + "( PARTITION parta VALUES('abc'), "
                        + "PARTITION partb VALUES('def', 'qrs'))" );

                try {
                    //LIST Partitioning check if unbound partition is correctly added when only specifying oen explicit partition

                    statement.executeUpdate( "CREATE TABLE listpartitioning3( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY LIST (tvarchar) "
                            + "( PARTITION parta VALUES('abc','def') )" );

                    //LIST partitioning can't be created with only empty lists
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "CREATE TABLE listpartitioning2( "
                                + "tprimary INTEGER NOT NULL, "
                                + "tinteger INTEGER NULL, "
                                + "tvarchar VARCHAR(20) NULL, "
                                + "PRIMARY KEY (tprimary) )"
                                + "PARTITION BY LIST (tvarchar) "
                                + "PARTITIONS 3" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // TODO: Check partition distribution violation

                    // TODO: Check unbound partitions
                } finally {
                    statement.executeUpdate( "DROP TABLE listpartitioning" );
                    statement.executeUpdate( "DROP TABLE listpartitioning2" );
                    statement.executeUpdate( "DROP TABLE listpartitioning3" );
                }
            }
        }
    }


    @Test
    @Category(CassandraExcluded.class)
    public void rangePartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE rangepartitioning1( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY RANGE (tinteger) "
                        + "( PARTITION parta VALUES(1,5), "
                        + "PARTITION partb VALUES(6,10))" );

                try {
                    statement.executeUpdate( "INSERT INTO rangepartitioning1 VALUES (1, 3, 'hans')" );
                    statement.executeUpdate( "INSERT INTO rangepartitioning1 VALUES (2, 7, 'bob')" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM rangepartitioning1 ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 7, "bob" } ) );

                    statement.executeUpdate( "UPDATE rangepartitioning1 SET tinteger = 6 WHERE tinteger = 7" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM rangepartitioning1 ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, 3, "hans" },
                                    new Object[]{ 2, 6, "bob" } ) );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM rangepartitioning1 WHERE tinteger = 6" ),
                            ImmutableList.of(
                                    new Object[]{ 2, 6, "bob" } ) );

                    // Checks if the input is ordered correctly. e.g. if the range for MIN and MAX is swapped when necessary
                    statement.executeUpdate( "CREATE TABLE rangepartitioning3( "
                            + "tprimary INTEGER NOT NULL, "
                            + "tinteger INTEGER NULL, "
                            + "tvarchar VARCHAR(20) NULL, "
                            + "PRIMARY KEY (tprimary) )"
                            + "PARTITION BY RANGE (tinteger) "
                            + "( PARTITION parta VALUES(5,4), "
                            + "PARTITION partb VALUES(10,6))" );

                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "rangepartitioning3" ) ).get( 0 );

                    List<CatalogPartition> catalogPartitions = Catalog.getInstance().getPartitionsByTable( table.id );

                    Assert.assertEquals( new ArrayList<>( Arrays.asList( "4", "5" ) )
                            , catalogPartitions.get( 0 ).partitionQualifiers );

                    Assert.assertEquals( new ArrayList<>( Arrays.asList( "6", "10" ) )
                            , catalogPartitions.get( 1 ).partitionQualifiers );

                    // RANGE partitioning can't be created without specifying ranges
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "CREATE TABLE rangepartitioning3( "
                                + "tprimary INTEGER NOT NULL, "
                                + "tinteger INTEGER NULL, "
                                + "tvarchar VARCHAR(20) NULL, "
                                + "PRIMARY KEY (tprimary) )"
                                + "PARTITION BY RANGE (tinteger) "
                                + "PARTITIONS 3" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );
                } finally {
                    statement.executeUpdate( "DROP TABLE rangepartitioning1" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS rangepartitioning2" );
                    statement.executeUpdate( "DROP TABLE IF EXISTS rangepartitioning3" );
                }
            }
        }
    }


    @Test
    public void partitionFilterTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            long partitionsToCreate = 4;

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE physicalPartitionFilter( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "tinteger INTEGER NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "WITH (foo, bar, foobar, barfoo) " );

                try {

                    statement.executeUpdate( "INSERT INTO physicalPartitionFilter VALUES (10, 'e', 100)" );
                    statement.executeUpdate( "INSERT INTO physicalPartitionFilter VALUES (21, 'f', 200)" );

                    // Check if filter on partitionValue can be applied
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM physicalPartitionFilter WHERE tvarchar = 'e'" ),
                            ImmutableList.of(
                                    new Object[]{ 10, "e", 100 } ) );

                    // Check if negative Value can be used on partitionColumn
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM physicalPartitionFilter WHERE tvarchar != 'e'" ),
                            ImmutableList.of(
                                    new Object[]{ 21, "f", 200 } ) );

                    // Check if filter can be applied to arbitrary column != partitionColumn
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM physicalPartitionFilter WHERE tinteger = 100" ),
                            ImmutableList.of(
                                    new Object[]{ 10, "e", 100 } ) );

                    // Check if FILTER Compound can be used - OR
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM physicalPartitionFilter WHERE tvarchar = 'e' OR tvarchar = 'f' ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 10, "e", 100 },
                                    new Object[]{ 21, "f", 200 } ) );

                    // Check if FILTER Compound can be used - AND
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM physicalPartitionFilter WHERE tvarchar = 'e' AND tvarchar = 'f'" ),
                            ImmutableList.of() );
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS physicalPartitionFilter" );
                }
            }
        }
    }


    @Test
    public void partitionPlacementTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            long partitionsToCreate = 4;

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE physicalPartitionTest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "WITH (foo, bar, foobar, barfoo) " );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "physicalpartitiontest" ) ).get( 0 );
                    // Check if sufficient PartitionPlacements have been created

                    // Check if initially as many partitionPlacements are created as requested
                    Assert.assertEquals( partitionsToCreate, Catalog.getInstance().getAllPartitionPlacementsByTable( table.id ).size() );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );
                    List<CatalogPartitionPlacement> debugPlacements = Catalog.getInstance().getAllPartitionPlacementsByTable( table.id );
                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"physicalPartitionTest\" ADD PLACEMENT ON STORE \"anotherstore\"" );
                    Assert.assertEquals( partitionsToCreate * 2, Catalog.getInstance().getAllPartitionPlacementsByTable( table.id ).size() );
                    debugPlacements = Catalog.getInstance().getAllPartitionPlacementsByTable( table.id );
                    // Modify partitions on second store
                    statement.executeUpdate( "ALTER TABLE \"physicalPartitionTest\" MODIFY PARTITIONS (\"foo\") ON STORE anotherstore" );
                    Assert.assertEquals( partitionsToCreate + 1, Catalog.getInstance().getAllPartitionPlacementsByTable( table.id ).size() );
                    debugPlacements = Catalog.getInstance().getAllPartitionPlacementsByTable( table.id );
                    // After MERGE should only hold one partition
                    statement.executeUpdate( "ALTER TABLE \"physicalPartitionTest\" MERGE PARTITIONS" );
                    Assert.assertEquals( 2, Catalog.getInstance().getAllPartitionPlacementsByTable( table.id ).size() );
                    debugPlacements = Catalog.getInstance().getAllPartitionPlacementsByTable( table.id );
                    // DROP STORE and verify number of partition Placements
                    statement.executeUpdate( "ALTER TABLE \"physicalPartitionTest\" DROP PLACEMENT ON STORE \"anotherstore\"" );
                    Assert.assertEquals( 1, Catalog.getInstance().getAllPartitionPlacementsByTable( table.id ).size() );

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS physicalPartitionTest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP anotherstore" );
                }
            }
        }
    }


    @Test
    public void temperaturePartitionTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // Sets the background processing of Workload Monitoring a Temperature monitoring to one second to get immediate results
                ConfigManager cm = ConfigManager.getInstance();
                Config c1 = cm.getConfig( "runtime/partitionFrequencyProcessingInterval" );
                c1.setEnum( TaskSchedulingType.EVERY_FIVE_SECONDS );

                statement.executeUpdate( "CREATE TABLE temperaturetest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY TEMPERATURE(tvarchar)"
                        + "(PARTITION hot VALUES(12%),"
                        + "PARTITION cold VALUES(14%))"
                        + " USING FREQUENCY write  INTERVAL 10 minutes WITH  20 HASH PARTITIONS" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "temperaturetest" ) ).get( 0 );

                    // Check if partition properties are correctly set and parsed
                    Assert.assertEquals( 600, ((TemperaturePartitionProperty) table.partitionProperty).getFrequencyInterval() );
                    Assert.assertEquals( 12, ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageIn() );
                    Assert.assertEquals( 14, ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageOut() );
                    Assert.assertEquals( PartitionType.HASH, ((TemperaturePartitionProperty) table.partitionProperty).getInternalPartitionFunction() );

                    Assert.assertEquals( 2, table.partitionProperty.getPartitionGroupIds().size() );
                    Assert.assertEquals( 20, table.partitionProperty.getPartitionIds().size() );

                    // Check if initially as many partitionPlacements are created as requested and stored in the partition property
                    Assert.assertEquals( table.partitionProperty.getPartitionIds().size(), Catalog.getInstance().getAllPartitionPlacementsByTable( table.id ).size() );

                    // Retrieve partition distribution
                    // Get percentage of tables which can remain in HOT
                    long numberOfPartitionsInHot = (table.partitionProperty.partitionIds.size() * ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageIn()) / 100;
                    //These are the tables than can remain in HOT
                    long allowedTablesInHot = (table.partitionProperty.partitionIds.size() * ((TemperaturePartitionProperty) table.partitionProperty).getHotAccessPercentageOut()) / 100;
                    if ( numberOfPartitionsInHot == 0 ) {
                        numberOfPartitionsInHot = 1;
                    }
                    if ( allowedTablesInHot == 0 ) {
                        allowedTablesInHot = 1;
                    }
                    long numberOfPartitionsInCold = table.partitionProperty.partitionIds.size() - numberOfPartitionsInHot;

                    List<CatalogPartition> hotPartitions = Catalog.getInstance().getPartitions( ((TemperaturePartitionProperty) table.partitionProperty).getHotPartitionGroupId() );
                    List<CatalogPartition> coldPartitions = Catalog.getInstance().getPartitions( ((TemperaturePartitionProperty) table.partitionProperty).getColdPartitionGroupId() );

                    Assert.assertTrue( (numberOfPartitionsInHot == hotPartitions.size()) || (numberOfPartitionsInHot == allowedTablesInHot) );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"hot\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    statement.executeUpdate( "ALTER ADAPTERS ADD \"cold\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    String partitionValue = "Foo";

                    statement.executeUpdate( "INSERT INTO temperaturetest VALUES (1, 3, '" + partitionValue + "')" );
                    statement.executeUpdate( "INSERT INTO temperaturetest VALUES (2, 4, '" + partitionValue + "')" );
                    statement.executeUpdate( "INSERT INTO temperaturetest VALUES (3, 5, '" + partitionValue + "')" );
                    statement.executeUpdate( "INSERT INTO temperaturetest VALUES (4, 6, '" + partitionValue + "')" );

                    //Do batch INSERT to check if BATCH INSERT works for partitioned tables
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO temperaturetest(tprimary,tinteger,tvarchar) VALUES (?, ?, ?)" );

                    preparedInsert.setInt( 1, 7 );
                    preparedInsert.setInt( 2, 55 );
                    preparedInsert.setString( 3, partitionValue );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();
                    // This should execute two DML INSERTS on the target PartitionId and therefore redistribute the data

                    // Verify that the partition is now in HOT and was not before
                    CatalogTable updatedTable = Catalog.getInstance().getTables( null, null, new Pattern( "temperaturetest" ) ).get( 0 );

                    // Manually get the target partitionID of query
                    PartitionManagerFactory partitionManagerFactory = PartitionManagerFactory.getInstance();
                    PartitionManager partitionManager = partitionManagerFactory.getPartitionManager( table.partitionProperty.partitionType );
                    long targetId = partitionManager.getTargetPartitionId( table, partitionValue );

                    List<CatalogPartition> hotPartitionsAfterChange = Catalog.getInstance().getPartitions( ((TemperaturePartitionProperty) updatedTable.partitionProperty).getHotPartitionGroupId() );
                    Assert.assertTrue( hotPartitionsAfterChange.contains( Catalog.getInstance().getPartition( targetId ) ) );

                    //Todo @Hennlo check number of access
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS temperaturetest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP hot" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP cold" );
                }
            }
        }
    }


    @Test
    public void multiInsertTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE multiinsert( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "tinteger INTEGER NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 20" );

                try {
                    statement.executeUpdate( "INSERT INTO multiinsert(tprimary,tvarchar,tinteger) VALUES (1,'Hans',5),(2,'Eva',7),(3,'Alice',89)" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM multiinsert ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Hans", 5 },
                                    new Object[]{ 2, "Eva", 7 },
                                    new Object[]{ 3, "Alice", 89 } ) );

                    // Check if the values are correctly associated with the corresponding partition
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM multiinsert WHERE tvarchar = 'Hans' ORDER BY tprimary" ),
                            ImmutableList.of( new Object[]{ 1, "Hans", 5 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM multiinsert WHERE tvarchar = 'Eva' ORDER BY tprimary" ),
                            ImmutableList.of( new Object[]{ 2, "Eva", 7 } ) );

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM multiinsert WHERE tvarchar = 'Alice' ORDER BY tprimary" ),
                            ImmutableList.of( new Object[]{ 3, "Alice", 89 } ) );

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS batchtest" );
                }
            }
        }
    }


    @Test
    @Category(FileExcluded.class)
    public void batchPartitionTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE batchtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "tinteger INTEGER NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "PARTITIONS 20" );

                try {
                    //
                    // INSERT
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO batchtest(tprimary,tvarchar,tinteger) VALUES (?, ?, ?)" );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.setInt( 3, 4 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setString( 2, "Bar" );
                    preparedInsert.setInt( 3, 55 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 3 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.setInt( 3, 67 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 4 );
                    preparedInsert.setString( 2, "FooBar" );
                    preparedInsert.setInt( 3, 89 );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM batchtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    //
                    // UPDATE
                    PreparedStatement preparedUpdate = connection.prepareStatement( "UPDATE batchtest SET tinteger = ? WHERE tprimary = ?" );

                    preparedUpdate.setInt( 1, 31 );
                    preparedUpdate.setInt( 2, 1 );
                    preparedUpdate.addBatch();

                    preparedUpdate.setInt( 1, 32 );
                    preparedUpdate.setInt( 2, 2 );
                    preparedUpdate.addBatch();

                    preparedUpdate.setInt( 1, 33 );
                    preparedUpdate.setInt( 2, 3 );
                    preparedUpdate.addBatch();

                    preparedUpdate.executeBatch();

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM batchtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 31 },
                                    new Object[]{ 2, "Bar", 32 },
                                    new Object[]{ 3, "Foo", 33 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    //
                    // DELETE
                    PreparedStatement preparedDelete = connection.prepareStatement( "DELETE FROM batchtest WHERE tinteger = ?" );

                    preparedDelete.setInt( 1, 31 );
                    preparedDelete.addBatch();

                    preparedDelete.setInt( 1, 89 );
                    preparedDelete.addBatch();

                    preparedDelete.executeBatch();

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM batchtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 32 },
                                    new Object[]{ 3, "Foo", 33 } ) );

                    statement.executeUpdate( "ALTER TABLE \"batchtest\" MERGE PARTITIONS" );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM batchtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 32 },
                                    new Object[]{ 3, "Foo", 33 } ) );

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS batchtest" );
                }
            }
        }
    }


    @Test
    public void hybridPartitioningTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                // Add Table
                statement.executeUpdate( "CREATE TABLE hybridpartitioningtest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "tinteger INTEGER NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Add data
                    PreparedStatement preparedInsert = connection.prepareStatement( "INSERT INTO hybridpartitioningtest(tprimary,tvarchar,tinteger) VALUES (?, ?, ?)" );

                    preparedInsert.setInt( 1, 1 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.setInt( 3, 4 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 2 );
                    preparedInsert.setString( 2, "Bar" );
                    preparedInsert.setInt( 3, 55 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 3 );
                    preparedInsert.setString( 2, "Foo" );
                    preparedInsert.setInt( 3, 67 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 4 );
                    preparedInsert.setString( 2, "FooBar" );
                    preparedInsert.setInt( 3, 89 );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    // Add second Adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Add second placement for table on that new adapter
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" ADD PLACEMENT ON STORE \"anotherstore\"" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    // Partition Data with HASH 4
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" PARTITION BY HASH (tvarchar) "
                            + "WITH (\"one\", \"two\", \"three\", \"four\")" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    // Place Partition 1 & 2 on first adapter
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" MODIFY PARTITIONS (\"one\", \"two\" ) ON STORE hsqldb" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    // Place Partition 3 & 4 on second adapter
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" MODIFY PARTITIONS (\"three\", \"four\" ) ON STORE anotherstore" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 } ) );

                    // Add more data
                    preparedInsert = connection.prepareStatement( "INSERT INTO hybridpartitioningtest(tprimary,tvarchar,tinteger) VALUES (?, ?, ?)" );
                    preparedInsert.setInt( 1, 407 );
                    preparedInsert.setString( 2, "BarFoo" );
                    preparedInsert.setInt( 3, 67 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 12 );
                    preparedInsert.setString( 2, "FooBar" );
                    preparedInsert.setInt( 3, 43 );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 1, "Foo", 4 },
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 3, "Foo", 67 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 } ) );

                    // Remove data
                    statement.executeUpdate( "DELETE FROM \"hybridpartitioningtest\" where tvarchar = 'Foo' " );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 } ) );

                    // Place Partition all partitions on second adapter
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" MODIFY PARTITIONS (\"one\", \"two\", \"three\", \"four\" ) ON STORE anotherstore" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 } ) );

                    // Remove initial placement from adapter
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" DROP PLACEMENT ON STORE \"hsqldb\"" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 } ) );

                    // Merge table
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" MERGE PARTITIONS" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 } ) );

                    // Add Data
                    preparedInsert = connection.prepareStatement( "INSERT INTO hybridpartitioningtest(tprimary,tvarchar,tinteger) VALUES (?, ?, ?)" );
                    preparedInsert.setInt( 1, 408 );
                    preparedInsert.setString( 2, "New" );
                    preparedInsert.setInt( 3, 22 );
                    preparedInsert.addBatch();

                    preparedInsert.setInt( 1, 409 );
                    preparedInsert.setString( 2, "Work" );
                    preparedInsert.setInt( 3, 23 );
                    preparedInsert.addBatch();

                    preparedInsert.executeBatch();

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 },
                                    new Object[]{ 408, "New", 22 },
                                    new Object[]{ 409, "Work", 23 } ) );

                    // Repartition with different number of partitions
                    statement.executeUpdate( "ALTER TABLE \"hybridpartitioningtest\" PARTITION BY HASH (tvarchar) "
                            + "WITH (\"one\", \"two\", \"three\", \"four\", \"five\", \"six\", \"seven\", \"eight\", \"nine\")" );

                    // Assert and Check if Table has the desired entries
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM hybridpartitioningtest ORDER BY tprimary" ),
                            ImmutableList.of(
                                    new Object[]{ 2, "Bar", 55 },
                                    new Object[]{ 4, "FooBar", 89 },
                                    new Object[]{ 12, "FooBar", 43 },
                                    new Object[]{ 407, "BarFoo", 67 },
                                    new Object[]{ 408, "New", 22 },
                                    new Object[]{ 409, "Work", 23 } ) );

                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS hybridpartitioningtest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP anotherstore" );
                }
            }
        }
    }


    @Test
    public void dataPlacementTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            long partitionsToCreate = 4;

            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE horizontalDataPlacementTest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )"
                        + "PARTITION BY HASH (tvarchar) "
                        + "WITH (foo, bar, foobar, barfoo) " );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "horizontaldataplacementtest" ) ).get( 0 );
                    // Check if sufficient PartitionPlacements have been created

                    // Check if initially as many DataPlacements are created as requested
                    // One for each store
                    Assert.assertEquals( 1, table.dataPlacements.size() );

                    CatalogDataPlacement dataPlacement = Catalog.getInstance().getDataPlacement( table.dataPlacements.get( 0 ), table.id );

                    // Check how many columnPlacements are added to the one DataPlacement
                    Assert.assertEquals( table.columnIds.size(), dataPlacement.columnPlacementsOnAdapter.size() );

                    // Check how many partitionPlacements are added to the one DataPlacement
                    Assert.assertEquals( partitionsToCreate, dataPlacement.partitionPlacementsOnAdapter.size() );

                    // ADD adapter
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"anotherstore\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FullPlacement
                    statement.executeUpdate( "ALTER TABLE \"horizontalDataPlacementTest\" ADD PLACEMENT ON STORE \"anotherstore\"" );

                    // Check if we now have two  dataPlacements in table
                    table = Catalog.getInstance().getTable( table.id );
                    Assert.assertEquals( 2, Catalog.getInstance().getDataPlacements( table.id ).size() );

                    // Modify partitions on second store
                    statement.executeUpdate( "ALTER TABLE \"horizontalDataPlacementTest\" MODIFY PARTITIONS (\"foo\") ON STORE anotherstore" );
                    List<CatalogDataPlacement> dataPlacements = Catalog.getInstance().getDataPlacements( table.id );

                    int adapterId = -1;
                    int initialAdapterId = -1;
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.getAdapterName().equals( "anotherstore" ) ) {
                            adapterId = dp.adapterId;
                            Assert.assertEquals( 1, dp.partitionPlacementsOnAdapter.size() );
                        } else {
                            initialAdapterId = dp.adapterId;
                            Assert.assertEquals( 4, dp.partitionPlacementsOnAdapter.size() );
                        }
                    }

                    // Modify columns on second store
                    statement.executeUpdate( "ALTER TABLE \"horizontalDataPlacementTest\" MODIFY PLACEMENT (tinteger) "
                            + "ON STORE anotherstore WITH partitions (\"bar\", \"barfoo\", \"foo\") " );

                    dataPlacements = Catalog.getInstance().getDataPlacements( table.id );
                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        if ( dp.adapterId == adapterId ) {
                            Assert.assertEquals( 2, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 3, dp.partitionPlacementsOnAdapter.size() );
                            Assert.assertEquals( 2, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( adapterId, table.id ).size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getPartitionsOnDataPlacement( adapterId, table.id ).size() );
                        } else if ( dp.adapterId == initialAdapterId ) {
                            Assert.assertEquals( 3, dp.columnPlacementsOnAdapter.size() );
                            Assert.assertEquals( 4, dp.partitionPlacementsOnAdapter.size() );
                            Assert.assertEquals( 3, Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( initialAdapterId, table.id ).size() );
                            Assert.assertEquals( 4, Catalog.getInstance().getPartitionsOnDataPlacement( initialAdapterId, table.id ).size() );
                        }
                    }

                    // After MERGE should only hold one partition
                    statement.executeUpdate( "ALTER TABLE \"horizontalDataPlacementTest\" MERGE PARTITIONS" );
                    dataPlacements = Catalog.getInstance().getDataPlacements( table.id );

                    for ( CatalogDataPlacement dp : dataPlacements ) {
                        Assert.assertEquals( 1, dp.partitionPlacementsOnAdapter.size() );
                    }

                    //Still two data placements left
                    Assert.assertEquals( 2, dataPlacements.size() );

                    // DROP STORE and verify number of dataPlacements
                    statement.executeUpdate( "ALTER TABLE \"horizontalDataPlacementTest\" DROP PLACEMENT ON STORE \"anotherstore\"" );
                    Assert.assertEquals( 1, Catalog.getInstance().getDataPlacements( table.id ).size() );

                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS horizontalDataPlacementTest" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP anotherstore" );
                }
            }
        }
    }

}
