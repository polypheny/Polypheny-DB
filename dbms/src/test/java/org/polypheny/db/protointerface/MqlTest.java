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
import java.sql.SQLException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.excluded.CassandraExcluded;
import org.polypheny.jdbc.PolyphenyConnection;
import org.polypheny.jdbc.multimodel.ProtoStatement;
import org.polypheny.jdbc.multimodel.ProtoStatement.ResultType;

@Category({ AdapterTestSuite.class, CassandraExcluded.class })
public class MqlTest {

    private static final String MQL_LANGUAGE_NAME = "mongo";
    private static final String TEST_DATA = "db.students.insertOne({name: \"John Doe\", age: 20, subjects: [\"Math\", \"Physics\", \"Chemistry\"], address: {street: \"123 Main St\", city: \"Anytown\", state: \"CA\", postalCode: \"12345\"}, graduationYear: 2023});";
    private static final String TEST_QUERY = "db.students.find();";


    @Test
    public void connectionUnwrapTest() {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyphenyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void simpleMqlTest() {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyphenyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            ProtoStatement protoStatement = connection.unwrap( PolyphenyConnection.class ).createProtoStatement();
            protoStatement.execute( "public", MQL_LANGUAGE_NAME, "use test" );
            ResultType resultType = protoStatement.execute( "test", MQL_LANGUAGE_NAME, TEST_DATA );
            assertEquals( resultType, ResultType.SCALAR );
            assertEquals( 1, protoStatement.getScalarResult() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

}
