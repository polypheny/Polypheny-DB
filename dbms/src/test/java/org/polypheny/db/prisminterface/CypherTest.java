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
import org.polypheny.jdbc.multimodel.GraphResult;
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.types.PolyGraphElement;
import org.polypheny.jdbc.types.PolyPath;

public class CypherTest {

    @BeforeAll
    public static void setup() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            polyStatement.execute( "public", "sql", "DROP NAMESPACE IF EXISTS cyphertest" );
            polyStatement.execute( "public", "sql", "CREATE GRAPH NAMESPACE cyphertest" );
            polyStatement.execute( "cyphertest", "cypher", "CREATE (:Person {id: 1, name: 'Alice'})" );
            polyStatement.execute( "cyphertest", "cypher", "CREATE (:Person {id: 2, name: 'Bob'})" );
            polyStatement.execute( "cyphertest", "cypher", "CREATE (:Person {id: 3, name: 'Charlie'})" );
            polyStatement.execute( "cyphertest", "cypher", "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)" );
            polyStatement.execute( "cyphertest", "cypher", "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)" );
        }
    }


    @Test
    public void cypherSelectNodesTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            GraphResult result = polyStatement.execute( "cyphertest", "cypher", "MATCH (n:Person) RETURN n ORDER BY n.id" ).unwrap( GraphResult.class );
            Iterator<PolyGraphElement> elements = result.iterator();

            assertTrue( elements.hasNext() );
            PolyGraphElement element = elements.next();
            assertEquals( "Alice", element.get( "name" ).asString() );
            assertEquals( 1, element.getLabels().size() );
            assertEquals( "Person", element.getLabels().get( 0 ) );

            assertTrue( elements.hasNext() );
            element = elements.next();
            assertEquals( "Bob", element.get( "name" ).asString() );
            assertEquals( 1, element.getLabels().size() );
            assertEquals( "Person", element.getLabels().get( 0 ) );

            assertTrue( elements.hasNext() );
            element = elements.next();
            assertEquals( "Charlie", element.get( "name" ).asString() );
            assertEquals( 1, element.getLabels().size() );
            assertEquals( "Person", element.getLabels().get( 0 ) );

            assertFalse( elements.hasNext() );
        }
    }


    @Test
    public void cypherSelectEdgesTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            GraphResult result = polyStatement.execute( "cyphertest", "cypher", "MATCH ()-[r:KNOWS]->() RETURN r" ).unwrap( GraphResult.class );
            Iterator<PolyGraphElement> elements = result.iterator();

            assertTrue( elements.hasNext() );
            PolyGraphElement element = elements.next();
            assertEquals( "KNOWS", element.getLabels().get( 0 ) );

            assertTrue( elements.hasNext() );
            element = elements.next();
            assertEquals( "KNOWS", element.getLabels().get( 0 ) );

            assertFalse( elements.hasNext() );
        }
    }


    @Test
    public void cypherSelectPathsTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            GraphResult result = polyStatement.execute( "cyphertest", "cypher", "MATCH (a:Person), (b:Person), p=(a)-[:KNOWS]->(a) RETURN p" ).unwrap( GraphResult.class );
            Iterator<PolyGraphElement> elements = result.iterator();

            assertTrue( elements.hasNext() );
            PolyGraphElement element = elements.next();
            PolyPath path = element.unwrap( PolyPath.class );
            assertFalse( elements.hasNext() );
        }
    }

    @Test
    public void cypherRelationalTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            RelationalResult result = polyStatement.execute( "cyphertest", "cypher", "MATCH (n:Person {name: 'Alice'}) RETURN n.name, n.id" ).unwrap( RelationalResult.class );
            Iterator<PolyRow> rows = result.iterator();

            assertTrue( rows.hasNext() );
            PolyRow row = rows.next();
            assertEquals( "Alice", row.get( "n.name" ).asString() );
            assertEquals( "1", row.get( "n.id" ).asString() );

            assertFalse( rows.hasNext() );
        }
    }


}
