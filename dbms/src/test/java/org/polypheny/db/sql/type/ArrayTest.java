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

package org.polypheny.db.sql.type;

import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;

public class ArrayTest {

    @Test
    public void NoTypeTest() throws SQLException {
        List<Object[]> data = List.of(
                new Object[][]{ new Object[]{ 1, 2, 3 } }
        );
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "DROP TABLE IF EXISTS t" ),
                ( c, s ) -> s.executeUpdate( "CREATE TABLE t(i INTEGER NOT NULL, a ARRAY, PRIMARY KEY(i))" ),
                ( c, s ) -> s.executeUpdate( "INSERT INTO t(i, a) VALUES (0, [1, 2, 3])" ),
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT a FROM t" ), data, true ),
                ( c, s ) -> s.executeUpdate( "DROP TABLE IF EXISTS t" ),
                ( c, s ) -> c.commit()
        );

    }

}
