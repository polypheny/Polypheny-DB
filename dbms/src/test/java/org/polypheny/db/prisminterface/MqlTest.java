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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.types.PolyDocument;

public class MqlTest {

    @BeforeAll
    public static void setup() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();

            polyStatement.execute( "public", "sql", "DROP NAMESPACE IF EXISTS prism_mql_test" );
            polyStatement.execute( "public", "sql", "CREATE DOCUMENT NAMESPACE prism_mql_test" );
            polyStatement.execute( "prism_mql_test", "mongo", "db.createCollection(\"test_collection\")" );
            polyStatement.execute( "prism_mql_test", "mongo", "db.test_collection.insert({ \"rank\": 1, \"name\": \"Alice\" })" );
            polyStatement.execute( "prism_mql_test", "mongo", "db.test_collection.insert({ \"rank\": 2, \"name\": \"Bob\" })" );
        }
    }


    @AfterAll
    public static void tearDown() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            connection.unwrap( PolyConnection.class ).createPolyStatement()
                    .execute( "public", "sql", "DROP NAMESPACE IF EXISTS prism_mql_test" );
        }
    }


    @Test
    public void mongoSelectTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            DocumentResult result = polyStatement.execute( "prism_mql_test", "mongo", "db.test_collection.find({})" ).unwrap( DocumentResult.class );
            Map<Integer, String> documents = new HashMap<>();
            for ( PolyDocument document : result ) {
                assertNull( documents.put( document.get( "rank" ).asInt(), document.get( "name" ).asString() ) );
            }
            assertEquals( Map.of( 1, "Alice", 2, "Bob" ), documents );
        }
    }


    @Test
    public void rejectsAutomaticallyGeneratedCollectionCreation() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            assertThrows( PrismInterfaceServiceException.class,
                    () -> polyStatement.execute( "prism_mql_test", "mongo", "db.missing_collection.find()" ) );
        }
    }

}
