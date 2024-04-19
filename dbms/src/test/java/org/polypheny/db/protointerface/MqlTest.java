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

package org.polypheny.db.protointerface;


import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.Result.ResultType;

public class MqlTest {

    private static final String MQL_LANGUAGE_NAME = "mongo";
    private static final String TEST_QUERY = "db.students.find();";


    @Test
    public void connectionUnwrapTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
        }
    }


    @Test
    public void simpleMqlTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            try ( Statement statement = connection.createStatement() ) {
                statement.execute( "DROP NAMESPACE IF EXISTS mqltest" );
                statement.execute( "CREATE DOCUMENT NAMESPACE mqltest" );
            }
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createProtoStatement();
            Result result = polyStatement.execute( "mqltest", MQL_LANGUAGE_NAME, TEST_QUERY );
            assertEquals( ResultType.DOCUMENT, result.getResultType() );
        }
    }

}
