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

package org.polypheny.db.policy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.models.PolicyChangeRequest;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptivAgentImpl;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.docker.DockerManagerTest;

@Category(DockerManagerTest.class)
@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class AdaptiveTest {

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
    public void testStoreSelectionBasedOnModel() throws SQLException {
        PoliciesManager policiesManager = PoliciesManager.getInstance();
        PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "LANGUAGE_OPTIMIZATION", "POLYPHENY", true, -1L );
        policiesManager.addClause( policyChangeRequest );
        policiesManager.updateClauses( policyChangeRequest );
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA );
                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mongodb1\" USING 'org.polypheny.db.adapter.mongodb.MongoStore'"
                        + " WITH '{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"33009\",\"trxLifetimeLimit\":\"1209600\",\"persistent\":\"false\"}'" );

                connection.commit();

                try {
                    statement.executeUpdate( NATION_TABLE );

                    Assert.assertEquals( 1, Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.size() );

                    Assert.assertEquals( SchemaType.RELATIONAL, ((DataStore) AdapterManager.getInstance().getAdapter(
                            Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.get( 0 ) )).getAdapterDefault().getPreferredSchemaType() );

                    connection.commit();

                } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                    log.error( "Caught exception test", e );
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS \"statisticschema\".\"nation\"" );
                    statement.executeUpdate( "DROP SCHEMA \"statisticschema\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"mongodb1\"");
                }
            }
        }
    }


    @Ignore
    public void testAdaptBasedOnModel() throws SQLException {
        PoliciesManager policiesManager = PoliciesManager.getInstance();
        PolicyChangeRequest policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", "LANGUAGE_OPTIMIZATION", "POLYPHENY", true, -1L );
        policiesManager.addClause( policyChangeRequest );
        policiesManager.updateClauses( policyChangeRequest );
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                AdapterManager adapterManager = AdapterManager.getInstance();
                Map<String, DataStore> datastores = adapterManager.getStores();
                for ( DataStore value : datastores.values() ) {
                    statement.executeUpdate( "ALTER ADAPTERS DROP " + value.getAdapterName() );
                }

                // Deploy additional store
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mongodb2\" USING 'org.polypheny.db.adapter.mongodb.MongoStore'"
                        + " WITH '{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"33007\",\"trxLifetimeLimit\":\"1209600\",\"persistent\":\"false\"}'" );

                statement.executeUpdate( SCHEMA );

                connection.commit();

                try {
                    statement.executeUpdate( NATION_TABLE );

                    Assert.assertNotEquals( SchemaType.RELATIONAL, ((DataStore) AdapterManager.getInstance().getAdapter(
                            Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.get( 0 ) )).getAdapterDefault().getPreferredSchemaType() );

                    statement.executeUpdate( "ALTER ADAPTERS ADD \"hsqldb2\" USING 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore'"
                            + " WITH '{maxConnections:\"25\",path:., trxControlMode:locks,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'" );

                    SelfAdaptivAgentImpl.getInstance().addAllDecisionsToQueue();

                    Assert.assertEquals( 1, Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.size() );

                    Assert.assertEquals( SchemaType.RELATIONAL, ((DataStore) AdapterManager.getInstance().getAdapter(
                            Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).dataPlacements.get( 0 ) )).getAdapterDefault().getPreferredSchemaType() );

                    connection.commit();

                } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                    log.error( "Caught exception test", e );
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS \"statisticschema\".\"nation\"" );
                    statement.executeUpdate( "DROP SCHEMA \"statisticschema\"" );
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"hsqldb2\"");
                    statement.executeUpdate( "ALTER ADAPTERS DROP \"mongodb2\"");
                }
            }
        }
    }


}
