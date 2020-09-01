/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.jdbc;


import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class JdbcArraySelectTest {

    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private final static String ARRAYTEST_SQL = "CREATE TABLE arraytest( "
            + "id INTEGER NOT NULL, "
            + "intarray INTEGER ARRAY(1,2), "
            + "doublearray DOUBLE ARRAY(1,2), "
            + "varchararray VARCHAR(20) ARRAY(1,2), "
            + "booleanarray BOOLEAN ARRAY(1,2), "
            + "PRIMARY KEY (id) )";

    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private final static String ARRAYTEST_DATA_SQL = "INSERT INTO arraytest(id, intarray, doublearray, varchararray, booleanarray) VALUES ("
            + "1,"
            + "ARRAY[1,2],"
            + "ARRAY[2.0, 2.5],"
//            + "ARRAY['foo', 'bar'],"
            + "null,"
            + "ARRAY[TRUE, FALSE]"
            + ")";

    @SuppressWarnings({ "SqlNoDataSourceInspection" })
    private final static Object[] ARRAYTEST_DATA = new Object[]{
            1,
            new Object[] { 1, 2 },
            new Object[] { 2.0, 2.5 },
            null,
//            new Object[] { "foo", "bar" },
            new Object[] { true, false } };


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
//        addTestData();
    }

    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
//                statement.executeUpdate( "CREATE SCHEMA knn" );
                statement.executeUpdate( "CREATE TABLE arraytest( id INTEGER NOT NULL, myarray INTEGER ARRAY(1,2), PRIMARY KEY (id) )" );
                statement.executeUpdate( "INSERT INTO arraytest VALUES (1, ARRAY[1,1])" );
                statement.executeUpdate( "INSERT INTO arraytest VALUES (2, ARRAY[2,2])" );
                statement.executeUpdate( "INSERT INTO arraytest VALUES (3, ARRAY[0,3])" );
                connection.commit();
            }
        }
    }

    @Test
    public void testArrayTypes() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( ARRAYTEST_SQL );
                statement.executeUpdate( ARRAYTEST_DATA_SQL );

//                statement.executeUpdate( "CREATE TABLE intarraytest (id INTEGER NOT NULL, intarray INTEGER ARRAY(1,2), PRIMARY KEY (id) )" );
//                statement.executeUpdate( "INSERT INTO intarraytest VALUES (1, ARRAY[1,2])" );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id FROM arraytest" ),
                        ImmutableList.of( new Object[] { ARRAYTEST_DATA[0] } ) );

//                statement.executeUpdate( "DROP TABLE intarraytest" );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT intarray FROM arraytest" ),
                        ImmutableList.of( new Object[] { ARRAYTEST_DATA[1] } ) );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT doublearray FROM arraytest" ),
                        ImmutableList.of( new Object[] { ARRAYTEST_DATA[2] } ) );

                /*TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT varchararray FROM arraytest" ),
                        ImmutableList.of( new Object[] { ARRAYTEST_DATA[3] } ) );*/

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT booleanarray FROM arraytest" ),
                        ImmutableList.of( new Object[] { ARRAYTEST_DATA[4] } ) );

                statement.executeUpdate( "DROP TABLE arraytest" );
            }
        }
    }
}
