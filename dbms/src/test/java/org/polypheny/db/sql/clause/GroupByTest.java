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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
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


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
public class GroupByTest {


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE TestTableA(Id INTEGER NOT NULL,Name VARCHAR(255), Primary key(Id))" );
                statement.executeUpdate( "INSERT INTO TestTableA VALUES(1,'Name1')" );
                statement.executeUpdate( "INSERT INTO TestTableA VALUES(2,'Name2')" );
                statement.executeUpdate( "INSERT INTO TestTableA VALUES(3,'Name3')" );
                statement.executeUpdate( "INSERT INTO TestTableA VALUES(4,'Name4')" );
                statement.executeUpdate( "INSERT INTO TestTableA VALUES(5,'Name5')" );

                statement.executeUpdate( "CREATE TABLE TestTableB(Id INTEGER NOT NULL,Row_Code VARCHAR(255) NOT NULL,Frequencies INTEGER, Primary key(Id,Row_Code))" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES(1,'A',86)" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES(1,'B',86)" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES(1,'C',90)" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES(2,'A',89)" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES(2,'C',92)" );
                statement.executeUpdate( "INSERT INTO TestTableB VALUES(3,'C',80)" );

                statement.executeUpdate( "CREATE TABLE TestTableC(Id INTEGER NOT NULL,Name VARCHAR(255),location VARCHAR(255), Primary key(Id))" );
                statement.executeUpdate( "INSERT INTO TestTableC VALUES(1,'Name1','loc1')" );
                statement.executeUpdate( "INSERT INTO TestTableC VALUES(2,'Name2','loc2')" );
                statement.executeUpdate( "INSERT INTO TestTableC VALUES(3,'Name3','loc3')" );

                statement.executeUpdate( "CREATE TABLE TestTableD(Row_Code VARCHAR(255) NOT NULL,differentname VARCHAR(255),spec VARCHAR(255), Primary key(Row_Code))" );
                statement.executeUpdate( "INSERT INTO TestTableD VALUES('A','names1','spec1')" );
                statement.executeUpdate( "INSERT INTO TestTableD VALUES('B','names2','spec2')" );
                statement.executeUpdate( "INSERT INTO TestTableD VALUES('C','names3','spec3')" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TestTableA" );
                statement.executeUpdate( "DROP TABLE TestTableB" );
                statement.executeUpdate( "DROP TABLE TestTableC" );
                statement.executeUpdate( "DROP TABLE TestTableD" );
            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void groupByTest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Name1", 443 },
                        new Object[]{ "Name2", 443 },
                        new Object[]{ "Name3", 443 },
                        new Object[]{ "Name4", 443 },
                        new Object[]{ "Name5", 443 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.Name, sum (P.Frequencies) FROM TestTableA S, TestTableB P WHERE P.Frequencies > 84 GROUP BY S.Name ORDER BY S.Name" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void groupByWithInnerSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Name1" },
                        new Object[]{ 2, "Name2" },
                        new Object[]{ 2, "Name2" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT s.id, s.name FROM TestTableC s, TestTableB t WHERE s.id = t.id AND Frequencies > (SELECT AVG (Frequencies) FROM TestTableB WHERE row_code = 'C' GROUP BY row_code='C')" ),
                        expectedResult,
                        true
                );
            }
        }
    }

}
