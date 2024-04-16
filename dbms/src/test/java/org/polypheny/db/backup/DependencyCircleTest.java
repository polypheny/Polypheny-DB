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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.transaction.TransactionManager;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class DependencyCircleTest {

    static TestHelper testHelper;
    BackupManager backupManager;


    @BeforeAll
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        //this.testHelper = TestHelper.getInstance();
        testHelper = TestHelper.getInstance();
        //deleteOldData();
        //this.backupManager = new BackupManager( testHelper.getTransactionManager() );
        //addTestData();
        //addDependenyTestData();

    }


    @AfterAll
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

        addBasicRelTestData();

        backupManager.startDataGathering();
        BackupInformationObject bupobj = backupManager.getBackupInformationObject();

        // go through all tables in the bupobj and add the table names to a string, which will be printed
        StringBuilder sb = new StringBuilder();
        ImmutableMap<Long, List<LogicalEntity>> tables = bupobj.getTables();
        for ( Long key : tables.keySet() ) {
            List<LogicalEntity> tableList = tables.get( key );
            for ( LogicalEntity entity : tableList ) {
                sb.append( entity.name ).append( "\n" );
            }
        }
        log.warn( sb.toString() );


        assertEquals( 1, tables.size(), "Wrong number of tables" );

        deleteBasicRelTestData();
    }


    private static void addDependenyTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE reli" );
                statement.executeUpdate( "CREATE NAMESPACE temp" );
                statement.executeUpdate( "CREATE NAMESPACE lol" );
                statement.executeUpdate( "create table reli.t1 (t1pk integer not null, t1fk integer not null, PRIMARY KEY(t1pk))" );
                statement.executeUpdate( "create table reli.t2 (t2pk integer not null, t2fk integer not null, PRIMARY KEY(t2pk))" );
                statement.executeUpdate( "create table reli.t3 (t2pk integer not null, PRIMARY KEY(t2pk))" );
                statement.executeUpdate( "create table temp.t4 (t4pk integer not null,t4fk integer not null, PRIMARY KEY(t4pk))" );
                statement.executeUpdate( "create table temp.t5 (t5pk integer not null,t5fk integer not null, PRIMARY KEY(t5pk))" );
                statement.executeUpdate( "create table temp.t6 (t6pk integer not null, t6fk integer not null, PRIMARY KEY(t6pk))" );
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

    private static void addBasicRelTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE reli" );
                statement.executeUpdate( "CREATE TABLE reli.album(albumId INTEGER NOT NULL, albumName VARCHAR(255), nbrSongs INTEGER,PRIMARY KEY (albumId))" );
                statement.executeUpdate( "INSERT INTO reli.album VALUES (1, 'Best Album Ever!', 10), (2, 'Pretty Decent Album...', 15), (3, 'Your Ears will Bleed!', 13)" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }

    private static void deleteBasicRelTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE reli.album" );
                statement.executeUpdate( "DROP SCHEMA reli" );
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
            assertEquals( 3, totalColumns, "Wrong number of columns" );

            // Check column names
            assertEquals( "TABLE_CAT", rsmd.getColumnName( 1 ), "Wrong column name" );
            assertEquals( "OWNER", rsmd.getColumnName( 2 ), "Wrong column name" );
            assertEquals( "DEFAULT_SCHEMA", rsmd.getColumnName( 3 ), "Wrong column name" );

            // Check data
            final Object[] databaseApp = new Object[]{ "APP", "system", "public" };

            TestHelper.checkResultSet(
                    connection.getMetaData().getCatalogs(),
                    ImmutableList.of( databaseApp ) );

        } catch ( SQLException e ) {
            log.error( "Exception while testing getCatalogs()", e );
        }
    }


    @Test
    public void testSimpleRelational() {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE reli2" );
                statement.executeUpdate( "CREATE TABLE reli2.t1 (t1pk INTEGER NOT NULL, t1fk INTEGER NOT NULL, PRIMARY KEY (t1pk))" );
                for ( int i = 0; i < 100; i++ ) {
                    statement.executeUpdate( String.format( "INSERT INTO reli2.t1 VALUES(%s,%s)", i, i * 2 ) );
                }
                connection.commit();

            } catch ( SQLException e ) {
                log.error( "Exception while adding test data", e );
            }

        } catch ( SQLException e ) {
            log.error( "Exception while testing getCatalogs()", e );
        }

        backupManager = BackupManager.getINSTANCE();
        backupManager.startDataGathering();

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( false ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE reli2.t1" );
                connection.commit();

            } catch ( SQLException e ) {
                log.error( "Exception while adding test data", e );
            }

        } catch ( SQLException e ) {
            log.error( "Exception while testing getCatalogs()", e );
        }

        backupManager.startInserting();
    }

}
