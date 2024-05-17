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

package org.polypheny.db.prisminterface;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.types.PolyDocument;

public class MqlTest {

    @BeforeAll
    public static void setup() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            polyStatement.execute( "public", "sql", "DROP NAMESPACE IF EXISTS mongotest" );
            polyStatement.execute( "public", "sql", "CREATE DOCUMENT NAMESPACE mongotest" );
            polyStatement.execute( "mongotest", "mongo", "db.createCollection(test_collection)" );
            polyStatement.execute( "mongotest", "mongo", "db.test_collection.insert({ \"_id\": 1, \"name\": \"Alice\" })" );
            polyStatement.execute( "mongotest", "mongo", "db.test_collection.insert({ \"_id\": 2, \"name\": \"Bob\" })" );
        }
    }


    @Test
    public void mongoSelectTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            DocumentResult result = polyStatement.execute( "mongotest", "mongo", "db.test_collection.find({})" ).unwrap( DocumentResult.class );
            Iterator<PolyDocument> documents = result.iterator();

            assertTrue( documents.hasNext() );
            PolyDocument document = documents.next();

            assertEquals( 1, document.get( "_id" ).asInt() );
            assertEquals( "Alice", document.get( "name" ).asString() );

            assertTrue( documents.hasNext() );
            document = documents.next();

            assertEquals( 2, document.get( "_id" ).asInt() );
            assertEquals( "Bob", document.get( "name" ).asString() );

            assertFalse( documents.hasNext() );
        }
    }

}
