/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.backup;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.transaction.TransactionManager;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class })
public class DependencyCircleTest {
    static TestHelper testHelper;
    BackupManager backupManager;

    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        //this.testHelper = TestHelper.getInstance();
        testHelper = TestHelper.getInstance();
        //deleteOldData();
        //this.backupManager = new BackupManager( testHelper.getTransactionManager() );
        //addTestData();
        addDependenyTestData();

    }


    @AfterClass
    public static void stop() {
        deleteDependencyTestData();
    }


    private static void addTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE schema1" );
                statement.executeUpdate( "CREATE TABLE schema1.table1( id INTEGER NOT NULL, PRIMARY KEY(id))" );
                statement.executeUpdate( "ALTER TABLE schema1.table1 ADD COLUMN name VARCHAR (255) NULL" );
                statement.executeUpdate( "ALTER TABLE schema1.table1 ADD UNIQUE INDEX index1 ON id ON STORE hsqldb" );
                statement.executeUpdate( "CREATE TABLE schema1.table2( id INTEGER NOT NULL, PRIMARY KEY(id) )" );
                statement.executeUpdate( "ALTER TABLE schema1.table2 ADD CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES schema1.table1(id) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "CREATE DOCUMENT SCHEMA private" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }


    private static void deleteOldData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "ALTER TABLE schema1.table2 DROP FOREIGN KEY fk_id" );
                    statement.executeUpdate( "ALTER TABLE schema1.table1 DROP INDEX index1" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                try {
                    statement.executeUpdate( "DROP TABLE schema1.table1" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                try {
                    statement.executeUpdate( "DROP TABLE schema1.table2" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                statement.executeUpdate( "DROP SCHEMA schema1" );
                statement.executeUpdate( "DROP SCHEMA private" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting old data", e );
        }
    }


    @Test
    public void testGatherData() {
        TransactionManager transactionManager = testHelper.getTransactionManager();
        BackupManager backupManager = BackupManager.getINSTANCE();

        backupManager.startDataGathering();

        Assert.assertEquals( 4, backupManager.getBackupInformationObject().getTables().get( 0 ).size());
    }





    private static void addDependenyTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE reli" );
                statement.executeUpdate( "CREATE NAMESPACE temp" );
                statement.executeUpdate( "CREATE NAMESPACE lol" );
                statement.executeUpdate( "create table reli.t1 (t1pk integer not null, t1fk integer not null)" );
                statement.executeUpdate( "create table reli.t2 (t2pk integer not null, t2fk integer not null)" );
                statement.executeUpdate( "create table reli.t3 (t2pk integer not null)" );
                statement.executeUpdate( "create table temp.t4 (t4pk integer not null,t4fk integer not null)" );
                statement.executeUpdate( "create table temp.t5 (t5pk integer not null,t5fk integer not null)" );
                statement.executeUpdate( "create table temp.t6 (t6pk integer not null, t6fk integer not null)" );
                statement.executeUpdate( "alter table reli.t1 add constraint test foreign key (t1fk) references temp.t6 (t6pk) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "alter table reli.t2 add constraint test foreign key (t2fk) references reli.t1 (t1pk) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "alter table temp.t4 add constraint test foreign key (t4fk) references reli.t1 (t1pk) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "alter table temp.t5 add constraint test foreign key (t5fk) references temp.t4 (t4pk) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                statement.executeUpdate( "alter table temp.t6 add constraint test foreign key (t6fk) references temp.t5 (t5pk) ON UPDATE RESTRICT ON DELETE RESTRICT" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }

    private static void deleteDependencyTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                /*
                try {
                    statement.executeUpdate( "ALTER TABLE schema1.table2 DROP FOREIGN KEY fk_id" );
                    statement.executeUpdate( "ALTER TABLE schema1.table1 DROP INDEX index1" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                try {
                    statement.executeUpdate( "DROP TABLE schema1.table1" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }
                try {
                    statement.executeUpdate( "DROP TABLE schema1.table2" );
                } catch ( SQLException e ) {
                    log.error( "Exception while deleting old data", e );
                }

                 */
                statement.executeUpdate( "DROP SCHEMA reli" );
                statement.executeUpdate( "DROP SCHEMA temp" );
                statement.executeUpdate( "DROP SCHEMA lol" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting old data", e );
        }
    }


    @Test
    public void testGetCatalogs() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            ResultSet resultSet = connection.getMetaData().getCatalogs();
            ResultSetMetaData rsmd = resultSet.getMetaData();

            // Check number of columns
            int totalColumns = rsmd.getColumnCount();
            Assert.assertEquals( "Wrong number of columns", 3, totalColumns );

            // Check column names
            Assert.assertEquals( "Wrong column name", "TABLE_CAT", rsmd.getColumnName( 1 ) );
            Assert.assertEquals( "Wrong column name", "OWNER", rsmd.getColumnName( 2 ) );
            Assert.assertEquals( "Wrong column name", "DEFAULT_SCHEMA", rsmd.getColumnName( 3 ) );

            // Check data
            final Object[] databaseApp = new Object[]{ "APP", "system", "public" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getCatalogs(),
                    ImmutableList.of( databaseApp ) );

        } catch ( SQLException e ) {
            log.error( "Exception while testing getCatalogs()", e );
        }
    }


}
