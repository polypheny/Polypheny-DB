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

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class CastTest {

    @BeforeAll
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void floatToIntTest() {
        List<Object[]> data = List.of(
                new Object[][]{ new Object[]{ 1 } }
        );

        TestHelper.executeSql(
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT CAST(1.1 as INTEGER)" ), data, true, true ),
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT CAST('1.1' as INTEGER)" ), data, true, true ),
                ( c, s ) -> c.commit()
        );
    }


    @Test
    public void nullAsTest() {
        List<Object[]> data = List.of(
                new Object[][]{ new Object[]{ null } }
        );

        TestHelper.executeSql(
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT CAST(null as INTEGER)" ), data, true ),
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT CAST(null as VARCHAR)" ), data, true ),
                ( c, s ) -> c.commit()
        );
    }


}
