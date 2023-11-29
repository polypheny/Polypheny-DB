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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.excluded.CottontailExcluded;
import org.polypheny.db.excluded.FileExcluded;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.entity.PolyValue;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Category({ AdapterTestSuite.class })
public class SelectTest {


    @BeforeClass
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
                statement.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (12, 'Name1', 60)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (15, 'Name2', 24)" );
                statement.executeUpdate( "INSERT INTO TableA VALUES (99, 'Name3', 11)" );

                statement.executeUpdate( "CREATE TABLE TableB(  ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (15, 'Name2', 24)" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (25, 'Name4', 40)" );
                statement.executeUpdate( "INSERT INTO TableB VALUES (98, 'Name3', 20)" );

                statement.executeUpdate( "CREATE TABLE TableC(  ID INTEGER NOT NULL, category VARCHAR(2), Tid BIGINT, PRIMARY KEY (ID) )" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (15, 'AB', 8200)" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (25, 'AB', 8201)" );
                statement.executeUpdate( "INSERT INTO TableC VALUES (98, 'AC', 8203)" );

                connection.commit();
            }
        }

    }


    @AfterClass
    public static void stop() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE TableA" );
                statement.executeUpdate( "DROP TABLE TableB" );
                statement.executeUpdate( "DROP TABLE TableC" );

            }
            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    @Category({ FileExcluded.class, CottontailExcluded.class })
    public void nestedSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // All with Inner Select
                // ALL is only supported if expand = false
                /*List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 12, "Name1" },
                        new Object[]{ 15, "Name2" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM  TableA WHERE TableA.age > ALL (SELECT age FROM TableB WHERE TableB.name = 'Name3')" ),
                        expectedResult,
                        true
                );

                // SOME with Inner Select
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 99, "Name3" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM  TableA WHERE  TableA.age < SOME (SELECT age FROM TableB WHERE TableB.name = 'Name3')" ),
                        expectedResult
                );

                // ANY with inner select
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 12, "Name1" },
                        new Object[]{ 15, "Name2" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM  TableA WHERE TableA.age > ANY (SELECT age FROM TableB WHERE TableB.name = 'Name3')" ),
                        expectedResult,
                        true
                );*/

                //NOT IN (SUB-QUERY)
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 12, "Name1" },
                        new Object[]{ 15, "Name2" },
                        new Object[]{ 99, "Name3" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM TableA WHERE TableA.age NOT IN (SELECT age FROM TableB WHERE TableB.name = 'Name3')" ),
                        expectedResult,
                        true
                );

                // IN(SUB-QUERY)
                expectedResult = ImmutableList.of(
                        new Object[]{ 12, "Name1" },
                        new Object[]{ 99, "Name3" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM TableA WHERE TableA.age NOT IN (SELECT age FROM TableB WHERE TableB.name = 'Name2')" ),
                        expectedResult,
                        true
                );

                // IN(value)
                expectedResult = ImmutableList.of(
                        new Object[]{ 12, "Name1" },
                        new Object[]{ 99, "Name3" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM TableA WHERE TableA.age IN (60,11,50)" ),
                        expectedResult,
                        true
                );

                // NOT IN(value)
                expectedResult = ImmutableList.of(
                        new Object[]{ 12, "Name1" },
                        new Object[]{ 15, "Name2" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT TableA.id, TableA.name FROM TableA WHERE TableA.age NOT IN (11)" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void existsSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 98 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT id FROM TABLEC WHERE category='AC' AND EXISTS (SELECT * FROM TABLEB WHERE age > 19 AND TABLEB.id = TABLEC.id)" ),
                        expectedResult
                );
            }
        }
    }


    @Test // todo dl move
    public void getRowsTest() {
        Transaction trx = TestHelper.getInstance().getTransaction();
        org.polypheny.db.transaction.Statement statement = trx.createStatement();

        AlgBuilder builder = AlgBuilder.create( statement );

        AlgNode scan = builder.scan( "public", "TableC" ).build();
        PolyImplementation impl = statement.getQueryProcessor().prepareQuery( AlgRoot.of( scan, Kind.SELECT ), false );

        ResultIterator iter = impl.execute( statement, 2 );
        List<List<PolyValue>> first = iter.getNextBatch();
        List<List<PolyValue>> others = iter.getNextBatch();

        assertEquals( 2, first.size() );
        assertEquals( 1, others.size() );
        try {
            iter.close();
            trx.commit();
        } catch ( TransactionException | Exception e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void databaseTest() throws SQLException {
        String sql = "SELECT * FROM APP.public.TableC";

        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 15, "AB", 8200 },
                        new Object[]{ 25, "AB", 8201 },
                        new Object[]{ 98, "AC", 8203 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( sql ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void insertUpdateTest() throws SQLException {
        String ddl = "CREATE TABLE my_table (column1 INT NOT NULL, column2 VARCHAR(255), column3 INT, PRIMARY KEY (column1))";
        String insert = "INSERT INTO my_table (column1, column2, column3) VALUES (1, 'v1', 100), (2, 'v2', 200), (3, 'v3', 300)";
        String update = "UPDATE my_table SET column2 = 'foo' WHERE column1 = 1 AND column3 = 100";

        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( ddl );
                statement.executeUpdate( insert );
                statement.executeUpdate( update );

            }
        }
    }

}
