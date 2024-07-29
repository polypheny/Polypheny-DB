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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class CountTest {

    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        insertData();
    }


    @AfterAll
    public static void end() {
        deleteData();
    }


    private static void insertData() {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE TABLE TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (10, 'Name1', 60)" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (12, 'Name2', 60)" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO TableA VALUES (13, 'Name2', 60)" ),
                ( c, s ) -> c.commit()
        );
    }


    private static void deleteData() {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "DROP TABLE TableA" )
        );
    }


    @Test
    public void simpleCountTest() {
        List<Object[]> data = new ArrayList<>();
        data.add( new Object[]{ 3 } );
        TestHelper.executeSql(
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT COUNT(*) FROM TableA" ), data, true )
        );
    }


    @Test
    public void simpleCountGroupTest() {
        List<Object[]> data = new ArrayList<>();
        data.add( new Object[]{ 60, 3 } );
        TestHelper.executeSql(
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT AGE, COUNT(*) FROM TableA GROUP BY AGE" ), data, true )
        );
    }


    @Test
    public void simpleCountGroupWhereTest() {
        List<Object[]> data = new ArrayList<>();
        data.add( new Object[]{ 60, 2 } );
        TestHelper.executeSql(
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT AGE, COUNT(*) FROM TableA WHERE name not like 'Name1' GROUP BY AGE" ), data, true )
        );
    }


}
