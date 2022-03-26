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

package org.polypheny.db.statistics;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class StatisticsTest {

    private final CountDownLatch waiter = new CountDownLatch( 1 );

    private final static String DROP_TABLES_NATION = "DROP TABLE IF EXISTS nation";
    private final static String DROP_TABLES_REGION = "DROP TABLE IF EXISTS region";
    private final static String DROP_TABLES_NATION1 = "DROP TABLE IF EXISTS nationdelete";
    private final static String DROP_SCHEMA = "DROP SCHEMA statisticschema";

    private final static String SCHEMA = "CREATE SCHEMA statisticschema";

    private final static String NATION_TABLE = "CREATE TABLE statisticschema.nation ( "
            + "n_nationkey  INTEGER NOT NULL,"
            + "n_name VARCHAR(25) NOT NULL,"
            + "n_regionkey INTEGER NOT NULL,"
            + "n_comment VARCHAR(152),"
            + "PRIMARY KEY (n_nationkey) )";

    private final static String NATION_TABLE_DATA = "INSERT INTO statisticschema.nation VALUES ("
            + "1,"
            + "'Switzerland',"
            + "1,"
            + "'nice'"
            + "),"
            + "(2,"
            + "'Germany',"
            + "2,"
            + "'amazing'),"
            + "(3,"
            + "'Italy',"
            + "3,"
            + "'beautiful')";

    private final static ImmutableList<Object[]> NATION_TEST_DATA = ImmutableList.of(
            new Object[]{
                    1,
                    "Switzerland",
                    1,
                    "nice"
            },
            new Object[]{
                    2,
                    "Germany",
                    2,
                    "amazing"
            },
            new Object[]{
                    3,
                    "Italy",
                    3,
                    "beautiful"
            } );


    private final static String NATION_TABLE_FOR_DELETE = "CREATE TABLE statisticschema.nationdelete ( "
            + "n_nationkey  INTEGER NOT NULL,"
            + "n_name VARCHAR(25) NOT NULL,"
            + "n_regionkey INTEGER NOT NULL,"
            + "n_comment VARCHAR(152),"
            + "PRIMARY KEY (n_nationkey) )";

    private final static String NATION_TABLE_DATA_FOR_DELETE = "INSERT INTO statisticschema.nationdelete VALUES ("
            + "1,"
            + "'Switzerland',"
            + "1,"
            + "'nice'"
            + "),"
            + "(2,"
            + "'Germany',"
            + "2,"
            + "'amazing'),"
            + "(3,"
            + "'Italy',"
            + "3,"
            + "'beautiful')";

    private final static String REGION_TABLE = "CREATE TABLE statisticschema.region  ( "
            + "r_regionkey INTEGER NOT NULL,"
            + "r_name VARCHAR(25) NOT NULL,"
            + "r_comment VARCHAR(152),"
            + "PRIMARY KEY (r_regionkey) )";

    private final static String REGION_TABLE_DATA = "INSERT INTO statisticschema.region VALUES ("
            + "1,"
            + "'Basel',"
            + "'nice'"
            + "),"
            + "(2,"
            + "'Schaffhausen',"
            + "'amazing')";

    private final static ImmutableList<Object[]> REGION_TEST_DATA = ImmutableList.of(
            new Object[]{
                    1,
                    "Basel",
                    "nice"
            },
            new Object[]{
                    2,
                    "Schaffhausen",
                    "amazing"
            } );


    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    @AfterClass
    public static void end() {
        dropTables();
    }


    public static void dropTables() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( DROP_TABLES_NATION );
                statement.executeUpdate( DROP_TABLES_REGION );
                statement.executeUpdate( DROP_TABLES_NATION1 );
                statement.executeUpdate( DROP_SCHEMA );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }


    private static void addTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( SCHEMA );
                statement.executeUpdate( NATION_TABLE );
                statement.executeUpdate( NATION_TABLE_DATA );
                statement.executeUpdate( REGION_TABLE );
                statement.executeUpdate( REGION_TABLE_DATA );
                statement.executeUpdate( NATION_TABLE_FOR_DELETE );
                statement.executeUpdate( NATION_TABLE_DATA_FOR_DELETE );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }


    @Test
    public void testTables() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM statisticschema.nation" ),
                            NATION_TEST_DATA
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM statisticschema.region" ),
                            REGION_TEST_DATA
                    );
                    connection.commit();
                } finally {
                    connection.rollback();
                }
            }
        }
    }


    @Test
    public void testSimpleRowCount() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM statisticschema.nation" ),
                            NATION_TEST_DATA
                    );
                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM statisticschema.region" ),
                            REGION_TEST_DATA
                    );
                    waiter.await( 20, TimeUnit.SECONDS );
                    try {
                        CatalogTable catalogTableNation = Catalog.getInstance().getTable( "APP", "statisticschema", "nation" );
                        CatalogTable catalogTableRegion = Catalog.getInstance().getTable( "APP", "statisticschema", "region" );

                        Integer rowCountNation = StatisticsManager.getInstance().rowCountPerTable( catalogTableNation.id );
                        Integer rowCountRegion = StatisticsManager.getInstance().rowCountPerTable( catalogTableRegion.id );

                        Assert.assertEquals( Integer.valueOf( 3 ), rowCountNation );
                        Assert.assertEquals( Integer.valueOf( 2 ), rowCountRegion );
                    } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                        log.error( "Caught exception test", e );
                    }
                    connection.commit();
                } catch ( InterruptedException e ) {
                    log.error( "Caught exception test", e );
                } finally {
                    connection.rollback();
                }
            }
        }
    }


    @Test
    public void testDeleteRowCount() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    assertStatisticsConvertTo( 180, 3 );
                    statement.executeUpdate( "DELETE FROM statisticschema.nationdelete" );
                    connection.commit();

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM statisticschema.nationdelete" ),
                            ImmutableList.of()
                    );
                    assertStatisticsConvertTo( 320, 0 ); // normally the system is a lot faster, but in some edge-cases this seems necessary

                    connection.commit();
                } finally {
                    connection.rollback();
                }
            }
        }
    }


    private void assertStatisticsConvertTo( int maxSeconds, int target ) {
        try {
            boolean successfull = false;
            int count = 0;
            boolean inCatalog = true;
            while ( !successfull && count < maxSeconds ) {
                waiter.await( 1, TimeUnit.SECONDS );
                if ( Catalog.getInstance().getTables( new Pattern( "APP" ), new Pattern( "statisticschema" ), new Pattern( "nationdelete" ) ).size() != 1 ) {
                    count++;
                    inCatalog = false;
                    continue;
                }
                inCatalog = true;
                CatalogTable catalogTableNation = Catalog.getInstance().getTable( "APP", "statisticschema", "nationdelete" );
                Integer rowCount = StatisticsManager.getInstance().rowCountPerTable( catalogTableNation.id );
                // potentially table exists not yet in statistics but in catalog
                if ( rowCount != null && rowCount == target ) {
                    successfull = true;

                }
                count++;
            }

            if ( !successfull && inCatalog ) {
                Assert.fail( String.format( "RowCount did not diverge to the correct number: %d.", target ) );
            }

            if ( inCatalog ) {
                // collection was removed too fast, so the count was already removed -> returns null
                log.warn( "Collection was already removed from the catalog, therefore the count will be null, which is correct" );
            }

        } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException | InterruptedException e ) {
            log.error( "Caught exception test", e );
        }
    }

    /*

      statement.executeUpdate("DELETE FROM statisticSchema.region WHERE r_regionkey = 2");

                    TestHelper.checkResultSet(
                            statement.executeQuery( "SELECT * FROM statisticSchema.region" ),
                            ImmutableList.of( new Object[]{
        1,
                "Basel",
                "nice" } )
            );

     */

}
