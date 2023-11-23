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
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
public class GeneralTest {

    @BeforeClass
    public static void start() {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        //deleteOldData();
        addTestData();
    }


    @AfterClass
    public static void stop() {
        deleteOldData();
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
