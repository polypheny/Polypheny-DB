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
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class ArrayTest {

    @Test
    public void NoTypeTest() throws SQLException {
        try ( JdbcConnection con = new JdbcConnection( true ) ) {
            try ( Statement s = con.getConn().createStatement() ) {
                s.executeUpdate( "DROP TABLE IF EXISTS t" );
                Assertions.assertThrows(
                        PrismInterfaceServiceException.class,
                        () -> s.executeUpdate( "CREATE TABLE t(i INTEGER NOT NULL, a ARRAY, PRIMARY KEY(i))" ),
                        "Array type must specify a collection type"
                );
            }
        }
    }

}
