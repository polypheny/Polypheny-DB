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
import java.util.List;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.DataPlacementRole;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;


@Category({ AdapterTestSuite.class })
public class PlacementPropertyTest {

    @Test
    public void parseSqlAlterPlacementProperty() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE parsesqlalterplacementproperty( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Verify general specification for property: 'ROLE'
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                            + "MODIFY PLACEMENT "
                            + "ON STORE hsqldb SET ROLE UPTODATE" );

                    // Verify another specification for property: 'ROLE'
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                            + "MODIFY PLACEMENT "
                            + "ON STORE hsqldb "
                            + "SET ROLE REFRESHABLE" );

                    // Check assert False. Wrong placement property value for property 'ROLE'
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb "
                                + "SET ROLE randomValue" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check assert False. Wrong placement property
                    failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb "
                                + "SET randomProperty UPTODATE" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check assert False. Incomplete specification
                    failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb "
                                + "SET" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );


                    // Deploy a new store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store1\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // Correct handling of Add a placement
                    statement.executeUpdate( "ALTER TABLE \"parsesqlalterplacementproperty\" ADD PLACEMENT (tvarchar) ON STORE \"store1\" "
                            + "SET ROLE REFRESHABLE" );

                    statement.executeUpdate( "ALTER TABLE \"parsesqlalterplacementproperty\" MODIFY PLACEMENT (tvarchar,tinteger) ON STORE \"store1\" "
                            + "SET ROLE PRIMARY" );


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE parsesqlalterplacementproperty" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store1" );
                }
            }
        }
    }


    @Test
    public void verifySqlAlterPlacementPropertyRole() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE parsesqlalterplacementpropertyrole( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Get Table
                    CatalogTable table = Catalog.getInstance().getTables(
                            null, null, new Pattern( "parsesqlalterplacementpropertyrole" )
                    ).get( 0 );


                    // Get the single DataPlacement
                    List<CatalogDataPlacement> allDataPlacements = Catalog.getInstance().getDataPlacements( table.id );

                    // Check before role assignment that this initial DataPlacement is labeled as UPTODATE
                    Assert.assertEquals( allDataPlacements.get( 0 ).dataPlacementRole, DataPlacementRole.UPTODATE );

                    // Check assert False. Constraint violation because no other primaries would be in place
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementpropertyrole "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb "
                                + "SET ROLE REFRESHABLE" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Deploy a new store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store2\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    // ADD FULL Placement to mimic the Primary copy
                    statement.executeUpdate( "ALTER TABLE \"parsesqlalterplacementpropertyrole\" ADD PLACEMENT ON STORE \"store2\" ");

                    // Assert that they directly are created using ROLE = UPTODATE by default
                    // Get both DataPlacements
                    allDataPlacements = Catalog.getInstance().getDataPlacements( table.id );

                    // Check before role assignment that all DataPlacement are labeled as UPTODATE
                    for ( CatalogDataPlacement dataPlacement : allDataPlacements ) {
                        Assert.assertEquals( dataPlacement.dataPlacementRole, DataPlacementRole.UPTODATE );
                    }

                    // Get the new StoreId
                    CatalogAdapter hsqldb = Catalog.getInstance().getAdapter( "hsqldb" );
                    CatalogAdapter store2 = Catalog.getInstance().getAdapter( "store2" );


                    // ALTER the first placement that they allow REFRESHABLE
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementpropertyrole "
                            + "MODIFY PLACEMENT "
                            + "ON STORE \"hsqldb\" SET ROLE REFRESHABLE" );


                    // Assert DataPlacements again
                    // Get that specific hsqldb placement
                    CatalogDataPlacement dataPlacementHsqlDb = Catalog.getInstance().getDataPlacement( hsqldb.id, table.id );

                    // Check before role assignment that all DataPlacement are labeled as UPTODATE
                    Assert.assertEquals( dataPlacementHsqlDb.dataPlacementRole, DataPlacementRole.REFRESHABLE );



                    // Create another store
                    statement.executeUpdate( "ALTER ADAPTERS ADD \"store3\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    CatalogAdapter store3 = Catalog.getInstance().getAdapter( "store3" );

                    // Add a new Placement to new storeand directly assign REFRESHABLE nodes
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementpropertyrole "
                            + "ADD PLACEMENT "
                            + "ON STORE \"store3\" "
                            + "SET ROLE REFRESHABLE" );

                    // Get that specific store3 placement
                    CatalogDataPlacement dataPlacementStore3 = Catalog.getInstance().getDataPlacement( store3.id, table.id );

                    // Assert if these new placements are correctly created with the desired role
                    Assert.assertEquals( dataPlacementStore3.dataPlacementRole, DataPlacementRole.REFRESHABLE );


                    // Modify the first placement again back to UPTODATE
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementpropertyrole "
                            + "MODIFY PLACEMENT "
                            + "ON STORE \"store3\" "
                            + "SET ROLE UPTODATE" );

                    // Get new version of dataPlacement
                    dataPlacementHsqlDb = Catalog.getInstance().getDataPlacement( hsqldb.id, table.id );

                    // Assert if it is correct again
                    Assert.assertEquals( dataPlacementHsqlDb.dataPlacementRole, DataPlacementRole.REFRESHABLE );


                    // Use together with columns and partitions
                    // and for each distinctively

                } catch ( UnknownAdapterException e ) {
                    e.printStackTrace();
                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE parsesqlalterplacementpropertyrole" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store2" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP store3" );
                }
            }
        }
    }


    @Test
    public void partitionPropertyConstraintTest() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE partitionpropertyconstrainttest( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    CatalogTable table = Catalog.getInstance().getTables( null, null, new Pattern( "partitionpropertyconstrainttest" ) ).get( 0 );

                    // Create two placements for one table

                    // Assert FALSE
                    // Try to change both to refreshable

                    // Assert FALSE
                    // Also do this for distinct partitions

                    // ASSERT a correct distribution at the end


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE IF EXISTS partitionpropertyconstrainttest" );
                }
            }
        }
    }

}
