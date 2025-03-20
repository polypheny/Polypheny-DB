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

package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.mql.MqlTestTemplate;

public class DocumentIdentifierTests extends MqlTestTemplate {

    @BeforeAll
    public static void setup() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                statement.execute( "CREATE DOCUMENT NAMESPACE mvccTest CONCURRENCY MVCC" );
                connection.commit();
            }
        }
    }


    @BeforeEach
    public void initCollection() {
        initCollection( namespace );
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                statement.execute( "CREATE DOCUMENT NAMESPACE IF NOT EXISTS mvccTest CONCURRENCY MVCC" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @AfterEach
    public void dropCollection() {
        dropCollection( namespace );
        dropCollection( "mvccTest", "mvccTest" );
    }


    @Test
    public void insertOneDocumentNoConflict() {

        execute( "db.mvccTest.insertOne({\"a\":\"first\", \"b\":\"second\" })", "mvccTest" );

        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"b\":\"second\"" ) );
        assertFalse( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void updateOneAddOrUpdateFieldNoConflict() {

        execute( "db.mvccTest.insertOne({\"a\":\"first\", \"b\":\"second\" })", "mvccTest" );
        execute( "db.mvccTest.updateOne({ \"a\":\"first\" }, { $set: { \"c\":\"third\" } })", "mvccTest" );

        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"b\":\"second\"" ) );
        assertTrue( document.contains( "\"c\":\"third\"" ) );
        assertFalse( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void updateOneRemoveFieldNoConflict() {

        execute( "db.mvccTest.insertOne({\"a\":\"first\", \"b\":\"second\" })", "mvccTest" );
        execute( "db.mvccTest.updateOne({ \"a\":\"first\" }, { $unset: { \"b\": null } });\n", "mvccTest" );

        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertFalse( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void insertSingleDocumentWithConflict() {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.mvccTest.insert({\"_eid\":\"first\", \"b\":\"second\" })", "mvccTest" )
        );
        assertTrue( exception.getMessage().contains( "The name of the field _eid" ) );
    }


    @Test
    public void updateOneAddOrUpdateIdentifierFieldConflict() {
        execute( "db.mvccTest.insertOne({\"a\":\"first\", \"b\":\"second\" })", "mvccTest" );
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.mvccTest.updateOne({ \"a\":\"first\" }, { $set: { \"_eid\":-32 } })", "mvccTest" )
        );
        assertTrue( exception.getMessage().contains( "The name of the field _eid" ) );
    }


    @Test
    public void updateOneRemoveIdentifierFieldConflict() {
        execute( "db.mvccTest.insertOne({\"a\":\"first\", \"b\":\"second\" })", "mvccTest" );
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.mvccTest.updateOne({ \"a\":\"first\" }, { $unset: { \"_eid\": null } });", "mvccTest" )
        );
        assertTrue( exception.getMessage().contains( "The name of the field _eid" ) );
    }


    @Test
    public void insertOneDocumentWithConflicts() {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.mvccTest.insertOne({\"_eid\":\"first\", \"b\":\"second\" })", "mvccTest" )
        );
        assertTrue( exception.getMessage().contains( "The name of the field _eid" ) );
    }


    @Test
    public void insertSingleDocumentNoConflicts() {

        execute( "db.mvccTest.insert({\"a\":\"first\", \"b\":\"second\" })", "mvccTest" );

        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"b\":\"second\"" ) );
        assertFalse( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void insertManyDocumentNoConflicts() {

        execute( "db.mvccTest.insertMany([{ \"a\": \"first\", \"b\": \"second\" }, { \"a\": \"third\", \"b\": \"fourth\" }, { \"a\": \"fifth\", \"b\": \"sixth\" }])", "mvccTest" );

        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 3, data.length );
        for ( String document : data ) {
            assertTrue( document.contains( "\"a\":" ) );
            assertTrue( document.contains( "\"b\":" ) );
            assertFalse( document.contains( "\"_eid\":" ) );
        }
    }


    @Test
    public void insertManyDocumentWithConflicts() {

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.mvccTest.insertMany([{ \"_eid\": \"first\", \"b\": \"second\" }, { \"a\": \"third\", \"b\": \"fourth\" }, { \"a\": \"fifth\", \"b\": \"sixth\" }])", "mvccTest" )
        );
        assertTrue( exception.getMessage().contains( "The name of the field _eid" ) );
    }


    @Test
    public void updateManyAddOrUpdateFieldsDuplicatesNoConflict() {
        execute( "db.mvccTest.insertMany([{\"a\":\"first\", \"b\":\"second\" }, {\"a\":\"first\", \"b\":\"second\" }, {\"a\":\"second\", \"b\":\"third\" }])", "mvccTest" );
        String[] debug = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        execute( "db.mvccTest.updateMany({ \"a\":\"first\" }, { $set: { \"c\":\"thisIsNew\" } })", "mvccTest" ).getData();
        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 3, data.length );

        int countUpdated = 0;
        for ( String document : data ) {
            if ( document.contains( "\"a\":\"first\"" ) ) {
                assertTrue( document.contains( "\"c\":\"thisIsNew\"" ) );
                countUpdated++;
            } else {
                assertTrue( document.contains( "\"b\":\"third\"" ) );
            }
        }
        assertEquals( 2, countUpdated );
        assertFalse( data[0].contains( "\"_eid\":" ) );
    }

    @Test
    public void updateManyAddOrUpdateFieldsNoDuplicatesNoConflict() {

        execute( "db.mvccTest.insertMany([{\"a\":\"first\", \"b\":\"second\" }, {\"a\":\"second\", \"b\":\"third\" }, {\"a\":\"fourth\", \"b\":\"fifth\" }])", "mvccTest" );
        execute( "db.mvccTest.updateMany({ \"a\":\"first\" }, { $set: { \"c\":\"thisIsNew\" } })", "mvccTest" );
        String[] data = execute( "db.mvccTest.find({})", "mvccTest" ).getData();
        assertEquals( 3, data.length );

        int countUpdated = 0;
        for ( String document : data ) {
            if ( document.contains( "\"a\":\"first\"" ) ) {
                assertTrue( document.contains( "\"c\":\"thisIsNew\"" ) );
                countUpdated++;
            }
        }
        assertEquals( 1, countUpdated );
        assertFalse( data[0].contains( "\"_eid\":" ) );
    }


    @Test
    public void updateManyRemoveIdentifierFieldConflict() {

        execute( "db.mvccTest.insertMany([{\"a\":\"first\", \"b\":\"second\"}, {\"a\":\"first\", \"b\":\"third\"}, {\"a\":\"second\", \"b\":\"fourth\"}])", "mvccTest" );
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.mvccTest.updateMany({ \"a\":\"first\" }, { $unset: { \"_eid\": null } })", "mvccTest" )
        );
        assertTrue( exception.getMessage().contains( "The name of the field _eid" ) );
    }

}
