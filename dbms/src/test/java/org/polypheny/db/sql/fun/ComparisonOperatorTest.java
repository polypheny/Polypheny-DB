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

package org.polypheny.db.sql.fun;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;


@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
@Slf4j
@Tag("adapter")
@Tag("fileExcluded")
public class ComparisonOperatorTest {


    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    private static void addTestData() throws SQLException {
        try ( TestHelper.JdbcConnection jdbcConnection = new TestHelper.JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE ComparisonOperatorTestTable (id integer not null, comparisonColumn INT, strColumn VARCHAR(20), primary key(id) )" );
                statement.executeUpdate( "INSERT INTO ComparisonOperatorTestTable (id, comparisonColumn, strColumn) VALUES "
                        + "(1, 8, 'Hans'), (2, 10, 'Alice'), (3, 12, 'Rebecca'), (4, 16, 'Bob'), (5, 22, 'Nina'), (6, 24, 'Ben'), (7, null, 'Test%Value')" );

                connection.commit();
            }
        }
    }


    @AfterAll
    public static void stop() throws SQLException {
        try ( TestHelper.JdbcConnection jdbcConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                // DROP TABLEs
                statement.executeUpdate( "DROP TABLE ComparisonOperatorTestTable" );
            }

            connection.commit();
        }
    }

    // --------------- Tests ---------------


    @Test
    public void testEqualsOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 } // Assuming the table has one row where the comparisonColumn equals 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn = 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Tag("mongodbExcluded")
    public void testNotEqualOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 5 } // Assuming the table has five rows where the comparisonColumn is not 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn <> 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Tag("mongodbExcluded")
    public void testNotEqualAlternativeOperator() throws SQLException {
        // This test should only be run in environments where '!=' is supported
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 5 } // Assuming the same setup as the previous test
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn != 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testGreaterThanOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 4 } // Assuming the table has four rows where the comparisonColumn is greater than 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn > 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testGreaterThanOrEqualOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 5 } // Assuming the table has five rows where the comparisonColumn is greater than or equal to 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn >= 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testLessThanOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 } // Assuming two rows where comparisonColumn is less than 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn < 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testLessThanOrEqualOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2 } // Assuming two rows where comparisonColumn is less than or equal to 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn <= 10" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testIsNullOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 } // Assuming one row where comparisonColumn is NULL
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn IS NULL" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testIsNotNullOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 6 } // Assuming six rows where comparisonColumn is NOT NULL
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn IS NOT NULL" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testIsDistinctFromOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 5 } // Assuming one value is NULL and four other distinct non-NULL values from 10
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn IS DISTINCT FROM 10 AND comparisonColumn IS NOT NULL" ), // should we include null in distinct or not
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testIsNotDistinctFromOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2 } // Assuming there are 2 values equal to 10 and one NULL, which is considered equal to another NULL
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn IS NOT DISTINCT FROM 10 OR comparisonColumn IS NULL" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testBetweenOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 3 } // Assuming there are 3 values between 10 and 20 inclusive
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn BETWEEN 10 AND 20" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testNotBetweenOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 3 } // Assuming there is 1 value less than 10 and 2 values greater than 20, making 3 that are NOT between 10 and 20
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn NOT BETWEEN 10 AND 20" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testLikeOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2 } // Assuming two strings match the pattern 'B%' (Ben and Bob)
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE strColumn LIKE 'B%'" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testNotLikeOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 5 } // Assuming three strings do not match the pattern 'B%'
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE strColumn NOT LIKE 'B%'" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testLikeOperatorWithEscapeCharacter() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 } // Assuming one string exactly matches 'Test|%Value' where '|' is used as an escape character for '%'
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE strColumn LIKE 'Test|%Value' ESCAPE '|'" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testInOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2 } // Assuming two values are in the list (12, 24)
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn IN (12, 24)" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Tag("mongodbExcluded")
    @Tag("cottontailExcluded")
    public void testNotInOperator() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 3 } // Assuming three values are not in the list (e.g., 10, 12, 22)
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn NOT IN (10, 12, 22)" ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void testInWithSubQuery() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn IN (SELECT comparisonColumn FROM ComparisonOperatorTestTable WHERE strColumn = 'Alice')"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Disabled("RexSubQuery Bug")
    public void testNotInWithSubQuery() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 0 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn NOT IN (SELECT comparisonColumn FROM ComparisonOperatorTestTable WHERE strColumn LIKE 'Rebecca')"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Disabled("RexSubQuery Bug")
    public void testSomeWithSubQuery() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn > SOME (SELECT comparisonColumn FROM ComparisonOperatorTestTable WHERE strColumn = 'Alice')"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Disabled("RexSubQuery Bug")
    public void testAnyWithSubQuery() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn < ANY (SELECT comparisonColumn FROM ComparisonOperatorTestTable WHERE strColumn = 'Bob')"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Disabled("RexSubQuery Bug")
    public void testAllWithSubQuery() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 1 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE comparisonColumn >= ALL (SELECT comparisonColumn FROM ComparisonOperatorTestTable WHERE strColumn = 'Alice')"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    @Tag("mongodbExcluded")
    public void testExistsWithSubQuery() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 7 } // Assuming the sub-query returns at least one row, indicating the EXISTS condition is true
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE EXISTS (SELECT 1 FROM ComparisonOperatorTestTable WHERE strColumn = 'Rebecca')"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void complexLogicalTestOne() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 3 } // Assuming 3 rows meet the complex condition
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE (comparisonColumn < 15 OR strColumn LIKE 'A%') AND comparisonColumn IS NOT NULL"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


    @Test
    public void complexLogicalTestTwo() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ 2 } // Assuming 2 rows meet the complex condition
                );
                TestHelper.checkResultSet(
                        statement.executeQuery(
                                "SELECT COUNT(*) FROM ComparisonOperatorTestTable WHERE (comparisonColumn > 20 OR comparisonColumn IS NULL) AND strColumn NOT LIKE 'B%'"
                        ),
                        expectedResult,
                        true
                );
            }
        }
    }


}
