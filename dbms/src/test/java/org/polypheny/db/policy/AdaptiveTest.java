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
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.docker.DockerManagerTest;
import org.polypheny.db.adaptiveness.models.PolicyChangeRequest;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.util.Pair;

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
                statement.executeUpdate( "ALTER ADAPTERS ADD \"mongodb\" USING 'org.polypheny.db.adapter.mongodb.MongoStore'"
                        + " WITH '{\"mode\":\"docker\",\"instanceId\":\"0\",\"port\":\"33001\",\"trxLifetimeLimit\":\"1209600\",\"persistent\":\"false\"}'" );

                connection.commit();

                try {
                    statement.executeUpdate( NATION_TABLE );

                    Assert.assertEquals( SchemaType.RELATIONAL, Catalog.getInstance().getTable( "APP", "statisticschema", "nation" ).getSchemaType() );

                    connection.commit();

                } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                    log.error( "Caught exception test", e );
                } finally {
                    statement.executeUpdate( "DROP TABLE IF EXISTS statisticschema.nation" );
                    statement.executeUpdate( "DROP SCHEMA statisticschema" );
                }
            }
        }
    }



}
