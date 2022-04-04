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

package org.polypheny.db.adaptiveness;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adaptiveness.models.PolicyChangeRequest;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.docker.DockerManagerTest;

@Category(DockerManagerTest.class)
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class PolicyTest {

    private final static String SCHEMA = "CREATE SCHEMA statisticschema";

    private final static String NATION_TABLE = "CREATE TABLE statisticschema.nation ( "
            + "n_nationkey  INTEGER NOT NULL,"
            + "n_name VARCHAR(25) NOT NULL,"
            + "n_regionkey INTEGER NOT NULL,"
            + "n_comment VARCHAR(152),"
            + "PRIMARY KEY (n_nationkey) )";


    @BeforeClass
    public static void initClass() {
        TestHelper.getInstance();
    }


    @Test
    public void testFullyPersistentFalse() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                PoliciesManager policiesManager = PoliciesManager.getInstance();
                PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "FULLY_PERSISTENT", "POLYPHENY", true, -1L );
                policiesManager.addClause( policyChangeRequest );
                policiesManager.updateClauses( policyChangeRequest );

                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mongodb\" USING 'org.polypheny.db.adapter.mongodb.MongoStore'"
                        + " WITH '{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"33000\",\"trxLifetimeLimit\":\"1209600\",\"persistent\":\"false\"}'" );

                statement.executeUpdate( SCHEMA );
                connection.commit();
                try {
                    //AssertFalse
                    //Not possible to create table because there is no persistent store
                    boolean failed = false;
                    try {
                        statement.executeUpdate( NATION_TABLE );
                    } catch ( AvaticaSqlException e ) {
                        log.warn( "testing" );
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    connection.commit();

                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"mongodb\"" );
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "FULLY_PERSISTENT", "POLYPHENY", false, -1L );
                    policiesManager.updateClauses( policyChangeRequest );
                    policiesManager.deleteClause( policyChangeRequest );
                }
            }
        }
    }


    @Test
    public void testFullyPersistentTrue() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                PoliciesManager policiesManager = PoliciesManager.getInstance();
                PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "FULLY_PERSISTENT", "POLYPHENY", true, -1L );
                policiesManager.addClause( policyChangeRequest );
                policiesManager.updateClauses( policyChangeRequest );

                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mongodbTrue\" USING 'org.polypheny.db.adapter.mongodb.MongoStore'"
                        + " WITH '{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"33000\",\"trxLifetimeLimit\":\"1209600\",\"persistent\":\"true\"}'" );

                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mongodbFalse\" USING 'org.polypheny.db.adapter.mongodb.MongoStore'"
                        + " WITH '{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"33001\",\"trxLifetimeLimit\":\"1209600\",\"persistent\":\"false\"}'" );

                statement.executeUpdate( SCHEMA );
                connection.commit();
                try {

                    statement.executeUpdate( NATION_TABLE );

                    Assert.assertEquals( 1, Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.size() );

                    Assert.assertTrue( AdapterManager.getInstance().getStore(
                            Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.get( 0 ) ).isPersistent() );

                    connection.commit();

                } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"mongodbTrue\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"mongodbFalse\"" );
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "FULLY_PERSISTENT", "POLYPHENY", false, -1L );
                    policiesManager.updateClauses( policyChangeRequest );
                    policiesManager.deleteClause( policyChangeRequest );
                }
            }
        }
    }

    @Test
    public void testDockerStoreFalse() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                PoliciesManager policiesManager = PoliciesManager.getInstance();
                PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_DOCKER", "POLYPHENY", true, -1L );
                policiesManager.addClause( policyChangeRequest );
                policiesManager.updateClauses( policyChangeRequest );

                statement.executeUpdate( "ALTER ADAPTERS ADD \"hsqldb\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );


                statement.executeUpdate( SCHEMA );
                connection.commit();
                try {
                    //AssertFalse
                    //Not possible to create table because there is no docker store
                    boolean failed = false;
                    try {
                        statement.executeUpdate( NATION_TABLE );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    connection.commit();

                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"hsqldb\"" );
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_DOCKER", "POLYPHENY", false, -1L );
                    policiesManager.updateClauses( policyChangeRequest );
                    policiesManager.deleteClause( policyChangeRequest );
                }
            }
        }
    }

    @Test
    public void testDockerStoreTrue() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                PoliciesManager policiesManager = PoliciesManager.getInstance();
                PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_DOCKER", "POLYPHENY", true, -1L );
                policiesManager.addClause( policyChangeRequest );
                policiesManager.updateClauses( policyChangeRequest );

                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"postDocker\" USING 'org.polypheny.db.adapter.jdbc.stores.PostgresqlStore'"
                        + "WITH '{\"mode\":\"docker\",\"password\":\"polypheny\",\"instanceId\":\"0\",\"port\":\"5432\",\"maxConnections\":\"25\"}'" );


                statement.executeUpdate( "ALTER ADAPTERS ADD \"hsqldbEmbeded\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                statement.executeUpdate( SCHEMA );
                connection.commit();
                try {
                    statement.executeUpdate( NATION_TABLE );

                    Assert.assertEquals( 1, Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.size() );

                    Assert.assertEquals( DeployMode.DOCKER, AdapterManager.getInstance().getAdapter(
                            Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.get( 0 ) ).getDeployMode());

                    connection.commit();

                } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"postDocker\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"hsqldbEmbeded\"" );
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_DOCKER", "POLYPHENY", false, -1L );
                    policiesManager.updateClauses( policyChangeRequest );
                    policiesManager.deleteClause( policyChangeRequest );
                }
            }
        }
    }

    @Test
    public void testEmbeddedStoreFalse() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                PoliciesManager policiesManager = PoliciesManager.getInstance();
                PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_EMBEDDED", "POLYPHENY", true, -1L );
                policiesManager.addClause( policyChangeRequest );
                policiesManager.updateClauses( policyChangeRequest );

                statement.executeUpdate( "ALTER ADAPTERS ADD \"postDocker\" USING 'org.polypheny.db.adapter.jdbc.stores.PostgresqlStore'"
                        + "WITH '{\"mode\":\"docker\",\"password\":\"polypheny\",\"instanceId\":\"0\",\"port\":\"5432\",\"maxConnections\":\"25\"}'" );

                statement.executeUpdate( SCHEMA );
                connection.commit();
                try {
                    //AssertFalse
                    //Not possible to create table because there is no embedded store
                    boolean failed = false;
                    try {
                        statement.executeUpdate( NATION_TABLE );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    connection.commit();

                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"postDocker\"" );
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_EMBEDDED", "POLYPHENY", false, -1L );
                    policiesManager.updateClauses( policyChangeRequest );
                    policiesManager.deleteClause( policyChangeRequest );
                }
            }
        }
    }

    @Test
    public void testEmbeddedStoreTrue() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                PoliciesManager policiesManager = PoliciesManager.getInstance();
                PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_EMBEDDED", "POLYPHENY", true, -1L );
                policiesManager.addClause( policyChangeRequest );
                policiesManager.updateClauses( policyChangeRequest );

                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"postDocker\" USING 'org.polypheny.db.adapter.jdbc.stores.PostgresqlStore'"
                        + "WITH '{\"mode\":\"docker\",\"password\":\"polypheny\",\"instanceId\":\"0\",\"port\":\"5432\",\"maxConnections\":\"25\"}'" );


                statement.executeUpdate( "ALTER ADAPTERS ADD \"hsqldbEmbeded\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                        + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                statement.executeUpdate( SCHEMA );
                connection.commit();
                try {
                    statement.executeUpdate( NATION_TABLE );

                    Assert.assertEquals( 1, Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.size() );

                    Assert.assertEquals( DeployMode.EMBEDDED, AdapterManager.getInstance().getAdapter(
                            Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.get( 0 ) ).getDeployMode());

                    connection.commit();

                } catch ( UnknownDatabaseException | UnknownTableException | UnknownSchemaException e ) {
                    e.printStackTrace();
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"postDocker\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"hsqldbEmbeded\"" );
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "ONLY_EMBEDDED", "POLYPHENY", false, -1L );
                    policiesManager.updateClauses( policyChangeRequest );
                    policiesManager.deleteClause( policyChangeRequest );
                }
            }
        }
    }


}
