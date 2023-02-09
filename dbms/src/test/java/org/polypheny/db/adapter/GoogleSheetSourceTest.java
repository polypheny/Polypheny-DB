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

package org.polypheny.db.adapter;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@Ignore // for debug purpose
public class GoogleSheetSourceTest {

    @BeforeClass
    public static void start() throws SQLException {
        TestHelper.getInstance();

        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                Map<String, String> settings = new HashMap<>();
                settings.put( "maxStringLength", "255" );
                settings.put( "querySize", "1000" );
                settings.put( "sheetsURL", "https://docs.google.com/spreadsheets/d/1-int7xwx0UyyigB4FLGMOxaCiuHXSNhi09fYSuAIX2Q/edit#gid=0" );
                settings.put( "mode", "remote" );
                settings.put( "resetRefreshToken", "No" );
                Gson gson = new Gson();
                statement.executeUpdate( "ALTER ADAPTERS ADD \"googlesheetunit\" USING 'GoogleSheets' AS 'Source' WITH '" + gson.toJson( settings ) + "'" );
            }
        }
    }


    @AfterClass
    public static void end() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "ALTER ADAPTERS DROP googlesheetunit" );
            }
        }

    }


    @Test
    public void existsSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Oracle", "Product Manager", 127000 },
                        new Object[]{ "eBay", "Software Engineer", 100000 },
                        new Object[]{ "Amazon", "Product Manager", 310000 },
                        new Object[]{ "Apple", "Software Engineering Manager", 372000 },
                        new Object[]{ "Microsoft", "Software Engineer", 157000 }

                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT \"Company\", \"Title\", \"Yearly Compensation\" from salaries LIMIT 5" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void existsSecondSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "GP", "F", 18, "U", "GT3", "A", 4, 4, "at_home", "teacher", "course", "mother", 2, 2, 0, "yes", "no", "no", "no", "yes", "yes", "no", "no", 4, 3, 4, 3, 6, 5, 6, 6 },
                        new Object[]{ "GP", "F", 17, "U", "GT3", "T", 1, 1, "at_home", "other", "course", "father", 1, 2, 0, "no", "yes", "no", "no", "no", "yes", "yes", "no", 5, 3, 3, 3, 4, 5, 5, 6 },
                        new Object[]{ "GP", "F", 15, "U", "LE3", "T", 1, 1, "at_home", "other", "other", "mother", 1, 2, 3, "yes", "no", "yes", "no", "yes", "yes", "yes", "no", 4, 3, 2, 3, 10, 7, 8, 10 }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT * from student_data LIMIT 3" ),
                        expectedResult
                );
            }
        }
    }


    @Test
    public void existsThirdSelect() throws SQLException {
        try ( TestHelper.JdbcConnection polyphenyDbConnection = new TestHelper.JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                List<Object[]> expectedResult = ImmutableList.of(
                        new Object[]{ "Alexandra", "Female" },
                        new Object[]{ "Andrew", "Male" },
                        new Object[]{ "Anna", "Female" }
                );
                TestHelper.checkResultSet(
                        statement.executeQuery( "SELECT \"Student Name\", \"Gender\" from student_formatting" ),
                        expectedResult
                );
            }
        }
    }

}



