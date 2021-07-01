/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;


@Slf4j
public class GroupByTest {


    @BeforeClass
    public static void start() throws SQLException {
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

                statement.executeUpdate("CREATE TABLE TestTableB(Id INTEGER NOT NULL,Row_Code VARCHAR(255) NOT NULL,Frequency INTEGER, Primary key(Id,Row_Code))" );
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


                statement.executeUpdate("CREATE TABLE TestTableD(Row_Code VARCHAR(255) NOT NULL,differentname VARCHAR(255),spec VARCHAR(255), Primary key(Row_Code))" );
                statement.executeUpdate( "INSERT INTO TestTableD VALUES('A','names1','spec1')" );
                statement.executeUpdate( "INSERT INTO TestTableD VALUES('B','names2','spec2')" );
                statement.executeUpdate( "INSERT INTO TestTableD VALUES('C','names3','spec3')" );

                connection.commit();
            }
        }
    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TestTableA" );
                statement.executeUpdate( "DROP TABLE TestTableB" );
                statement.executeUpdate( "DROP TABLE TestTableC" );
                statement.executeUpdate( "DROP TABLE TestTableD" );

                //   statement.executeUpdate( "DROP TABLE trigotestinteger" );
            }
        }
    }

    // --------------- Tests ---------------

    @Test
    public void groupbytest() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {


                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Name1", 452},
                        new Object[]{ "Name5", 452},
                        new Object[]{ "Name3", 452},
                        new Object[]{ "Name4", 452},
                        new Object[]{ "Name2", 452}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT S.Name, sum (P.Frequency) FROM TestTableA S, TestTableB P WHERE P.Frequency > 84 GROUP BY S.Name" ),
                        expectedResult
                );




            }

        }

    }


    @Test
    public void groupbywithInnerSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();

            try ( Statement statement = connection.createStatement() ) {


                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1, "Name1"},
                        new Object[]{ 2, "Name2"},
                        new Object[]{ 2, "Name2"}
                );

                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT s.id, s.name FROM TestTableC s, TestTableB t WHERE s.id = t.id AND Frequency >  (SELECT AVG (Frequency) FROM TestTableB WHERE row_code = 'C' GROUP BY row_code='C')\n" ),
                        expectedResult
                );




            }

        }

    }


}
