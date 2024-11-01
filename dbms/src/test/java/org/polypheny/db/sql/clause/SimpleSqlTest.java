/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.clause;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Tag("adapter")
public class SimpleSqlTest {

    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        insertData();
    }


    private static void insertData() {

    }


    @Test
    public void dropTable() {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))" ),
                ( c, s ) -> s.executeUpdate( "DROP TABLE TableA" ),
                ( c, s ) -> c.commit()
        );
    }

    @Test
    public void autoIncrement() throws SQLException {
        // skip this test if store.default.equals("file")
        String store = System.getProperty( "store.default" );
        if ( store != null && store.equals( "file" ) ) {
            return;
        }
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE IF EXISTS TableAutoInc" );
                statement.executeUpdate( "CREATE TABLE TableAutoInc(RID INTEGER NOT NULL AUTO_INCREMENT, YAC INTEGER NOT NULL, PRIMARY KEY (RID))" );
                statement.executeUpdate( "INSERT INTO TableAutoInc VALUES (1, 61), (2, 62)" );
                statement.executeUpdate( "INSERT INTO TableAutoInc(YAC) VALUES (63)" );
                statement.executeUpdate( "INSERT INTO TableAutoInc VALUES (100, 120)" );
                statement.executeUpdate( "INSERT INTO TableAutoInc(YAC) VALUES (121)" );
                statement.executeUpdate( "INSERT INTO TableAutoInc VALUES (4, 64)" );
                statement.executeUpdate( "INSERT INTO TableAutoInc(YAC) VALUES (122)" );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT RID, YAC FROM TableAutoInc" ),
                        ImmutableList.of(
                                new Object[]{1, 61},
                                new Object[]{2, 62},
                                new Object[]{3, 63},
                                new Object[]{100, 120},
                                new Object[]{101, 121},
                                new Object[]{4, 64},
                                new Object[]{102, 122}
                        )
                );
                statement.executeUpdate( "DROP TABLE TableAutoInc" );
                connection.commit();
            }

        }
    }


    @Test
    public void insert() throws SQLException {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (12, 'Name1', 60)" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (15, 'Name2', 24)" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (99, 'Name3', 11)" ),
                ( c, s ) -> s.executeUpdate( "DROP TABLE TableA" ),
                ( c, s ) -> c.commit()
        );

    }


    @Test
    public void select() throws SQLException {
        List<Object[]> data = List.of(
                new Object[]{ 12, "Name1", 60 },
                new Object[]{ 15, "Name2", 24 },
                new Object[]{ 99, "Name3", 11 }
        );
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (12, 'Name1', 60)" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (15, 'Name2', 24)" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (99, 'Name3', 11)" ),
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT * FROM TableA" ), data, true ),
                ( c, s ) -> s.executeUpdate( "UPDATE TableA SET AGE = 13 WHERE AGE = 12" ),
                ( c, s ) -> s.executeUpdate( "DROP TABLE TableA" ),
                ( c, s ) -> c.commit()
        );

    }


    @Test
    public void selectFromNestedSimple() {
        List<Object[]> data = List.of(
                new Object[]{ 1, "Name1" },
                new Object[]{ 1, "Name2" },
                new Object[]{ 1, "Name3" },
                new Object[]{ 1, "Name4" }
        );
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE TABLE Person(ID INTEGER NOT NULL, NAME VARCHAR(20), PRIMARY KEY (ID))" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (1, 'Name1'),(2, 'Name2')" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (3, 'Name3')" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (4, 'Name4')" ),
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT test, name FROM (SELECT COUNT(id) as test, name FROM Person GROUP BY name)" ), data, true ),
                ( c, s ) -> s.executeUpdate( "DROP TABLE Person" ),
                ( c, s ) -> c.commit()
        );

    }


    @Test
    public void likeTest() {
        List<Object[]> data = List.of(
                new Object[]{ 1 },
                new Object[]{ 2 },
                new Object[]{ 3 },
                new Object[]{ 4 }
        );
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE TABLE Person(ID INTEGER NOT NULL, NAME VARCHAR(20), PRIMARY KEY (ID))" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (1, 'Name1')" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (2, 'Name2')" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (3, 'Name3')" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO Person VALUES (4, 'Name4')" ),
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT test FROM (SELECT id AS test FROM Person where name like 'Name%')" ), data, true ),
                ( c, s ) -> s.executeUpdate( "DROP TABLE Person" ),
                ( c, s ) -> c.commit()
        );

    }


    @Test
    public void tooLargeNumberTest() {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "DROP TABLE IF EXISTS t" ),
                ( c, s ) -> s.executeUpdate( "CREATE TABLE t(i INTEGER NOT NULL, a DECIMAL(1) NOT NULL, PRIMARY KEY(i))" ),
                ( c, s ) -> Assertions.assertThrows( Exception.class, () -> s.executeUpdate( "INSERT INTO t(i, a) VALUES (0, 2555)" ) ),
                ( c, s ) -> Assertions.assertThrows( Exception.class, () -> {
                    PreparedStatement prepared = c.prepareStatement( "INSERT INTO t(i, a) VALUES (0, ?)" );
                    prepared.setInt( 1, 2555 );
                    prepared.execute();
                } ),
                ( c, s ) -> c.commit()
        );

    }


    @Test
    public void tooLargeVarCharTest() {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "DROP TABLE IF EXISTS t" ),
                ( c, s ) -> s.executeUpdate( "CREATE TABLE t(i INTEGER NOT NULL, a VARCHAR(1) NOT NULL, PRIMARY KEY(i))" ),
                ( c, s ) -> Assertions.assertThrows( Exception.class, () -> s.executeUpdate( "INSERT INTO t(i, a) VALUES (0, 'test')" ) ),
                ( c, s ) -> c.commit()
        );

    }

}
