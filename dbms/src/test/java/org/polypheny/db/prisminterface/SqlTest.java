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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;

public class SqlTest {

    @BeforeAll
    public static void setup() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection(); Statement statement = connection.createStatement() ) {
            statement.execute( "DROP NAMESPACE IF EXISTS prism_sql_test" );
            statement.execute( "CREATE RELATIONAL NAMESPACE prism_sql_test" );
            statement.execute( "CREATE TABLE prism_sql_test.test_table (id INTEGER NOT NULL, name VARCHAR(50), PRIMARY KEY (id))" );
            statement.execute( "INSERT INTO prism_sql_test.test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob')" );
        }
    }


    @AfterAll
    public static void tearDown() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection(); Statement statement = connection.createStatement() ) {
            statement.execute( "DROP NAMESPACE IF EXISTS prism_sql_test" );
        }
    }


    @Test
    public void sqlSelectTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection(); Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery( "SELECT id, name FROM prism_sql_test.test_table ORDER BY id" ) ) {
            assertTrue( rows.next() );
            assertEquals( 1, rows.getInt( "id" ) );
            assertEquals( "Alice", rows.getString( "name" ) );
            assertEquals( 1, rows.getInt( 1 ) );
            assertEquals( "Alice", rows.getString( 2 ) );

            assertTrue( rows.next() );
            assertEquals( 2, rows.getInt( "id" ) );
            assertEquals( "Bob", rows.getString( "name" ) );
            assertEquals( 2, rows.getInt( 1 ) );
            assertEquals( "Bob", rows.getString( 2 ) );
            assertFalse( rows.next() );
        }
    }


    @Test
    public void sqlSelectMetadataTest() throws SQLException {
        try ( Connection connection = new JdbcConnection( true ).getConnection(); Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery( "SELECT id, name FROM prism_sql_test.test_table" ) ) {
            ResultSetMetaData metadata = rows.getMetaData();
            assertEquals( 2, metadata.getColumnCount() );

            assertEquals( ResultSetMetaData.columnNoNulls, metadata.isNullable( 1 ) );
            assertEquals( 10, metadata.getColumnDisplaySize( 1 ) );
            assertEquals( "id", metadata.getColumnLabel( 1 ) );
            assertEquals( "id", metadata.getColumnName( 1 ) );
            assertEquals( 10, metadata.getPrecision( 1 ) );
            assertEquals( Types.INTEGER, metadata.getColumnType( 1 ) );

            assertEquals( ResultSetMetaData.columnNullable, metadata.isNullable( 2 ) );
            assertEquals( 50, metadata.getColumnDisplaySize( 2 ) );
            assertEquals( "name", metadata.getColumnLabel( 2 ) );
            assertEquals( "name", metadata.getColumnName( 2 ) );
            assertEquals( 50, metadata.getPrecision( 2 ) );
            assertEquals( Types.VARCHAR, metadata.getColumnType( 2 ) );
        }
    }

}
