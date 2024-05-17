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
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalColumnMetadata;
import org.polypheny.jdbc.multimodel.RelationalMetadata;
import org.polypheny.jdbc.multimodel.RelationalResult;

public class SqlTest {

    @BeforeAll
    public static void setup() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            polyStatement.execute( "public", "sql", "DROP NAMESPACE IF EXISTS sqltest" );
            polyStatement.execute( "public", "sql", "CREATE RELATIONAL NAMESPACE sqltest" );
            polyStatement.execute( "sqltest", "sql", "CREATE TABLE sqltest.test_table (id INT PRIMARY KEY, name VARCHAR(50))" );
            polyStatement.execute( "sqltest", "sql", "INSERT INTO sqltest.test_table (id, name) VALUES (1, 'Alice')" );
            polyStatement.execute( "sqltest", "sql", "INSERT INTO sqltest.test_table (id, name) VALUES (2, 'Bob')" );
        }
    }


    @Test
    public void sqlSelectTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            RelationalResult result = polyStatement.execute( "sqltest", "sql", "SELECT * FROM test_table" ).unwrap( RelationalResult.class );
            Iterator<PolyRow> rows = result.unwrap( RelationalResult.class ).iterator();
            assertTrue( rows.hasNext() );
            PolyRow row = rows.next();

            assertEquals( 1, row.get( "id" ).asInt() );
            assertEquals( "Alice", row.get( "name" ).asString() );

            assertEquals( 1, row.get( 0 ).asInt() );
            assertEquals( "Alice", row.get( 1 ).asString() );

            assertTrue( rows.hasNext() );
            row = rows.next();

            assertEquals( 2, row.get( "id" ).asInt() );
            assertEquals( "Bob", row.get( "name" ).asString() );

            assertEquals( 2, row.get( 0 ).asInt() );
            assertEquals( "Bob", row.get( 1 ).asString() );
        }
    }


    @Test
    public void sqlSelectMetadataTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            RelationalMetadata relationalMetadata = polyStatement.execute( "sqltest", "sql", "SELECT * FROM test_table" ).unwrap( RelationalResult.class ).getMetadata();
            assertEquals( 2, relationalMetadata.getColumnCount() );

            RelationalColumnMetadata relationalColumnMetadata1 = relationalMetadata.getColumnMeta( 0 );
            assertEquals( 0, relationalColumnMetadata1.getColumnIndex() );
            assertFalse( relationalColumnMetadata1.isNullable() ); // false as this is the primary key
            assertEquals( 10, relationalColumnMetadata1.getLength() );
            assertEquals( "id", relationalColumnMetadata1.getColumnLabel() );
            assertEquals( "id", relationalColumnMetadata1.getColumnName() );
            assertEquals( 10, relationalColumnMetadata1.getPrecision() );
            assertEquals( "INTEGER", relationalColumnMetadata1.getProtocolTypeName() );
            assertEquals( 0, relationalColumnMetadata1.getScale() );

            RelationalColumnMetadata relationalColumnMetadata2 = relationalMetadata.getColumnMeta( 1 );
            assertEquals( 1, relationalColumnMetadata2.getColumnIndex() );
            assertTrue( relationalColumnMetadata2.isNullable() );
            assertEquals( 50, relationalColumnMetadata2.getLength() );
            assertEquals( "name", relationalColumnMetadata2.getColumnLabel() );
            assertEquals( "name", relationalColumnMetadata2.getColumnName() );
            assertEquals( 50, relationalColumnMetadata2.getPrecision() );
            assertEquals( "VARCHAR", relationalColumnMetadata2.getProtocolTypeName() );
            assertEquals( -2147483648, relationalColumnMetadata2.getScale() );
        }
    }

}
