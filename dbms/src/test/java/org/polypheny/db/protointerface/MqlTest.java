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

package org.polypheny.db.protointerface;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.Result.ResultType;
import org.polypheny.jdbc.multimodel.ScalarResult;

@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class MqlTest {

    private static final String MQL_LANGUAGE_NAME = "mongo";
    private static final String TEST_DATA = "db.students.insertOne({name: \"John Doe\", age: 20, subjects: [\"Math\", \"Physics\", \"Chemistry\"], address: {street: \"123 Main St\", city: \"Anytown\", state: \"CA\", postalCode: \"12345\"}, graduationYear: 2023});";
    private static final String TEST_QUERY = "db.students.find();";


    @Test
    public void connectionUnwrapTest() {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    @Test
    public void simpleMqlTest() throws ClassNotFoundException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createProtoStatement();
            Result result = polyStatement.execute( "public", MQL_LANGUAGE_NAME, TEST_QUERY );
            assertEquals( ResultType.DOCUMENT, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }
}
