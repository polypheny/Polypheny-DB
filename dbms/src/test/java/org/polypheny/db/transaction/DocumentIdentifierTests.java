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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.polypheny.db.mql.MqlTestTemplate;

public class DocumentIdentifierTests extends MqlTestTemplate {

    @Test
    public void insertOneDocumentNoConflict() {
        execute( "db.test.insertOne({\"a\":\"first\", \"b\":\"second\" })" );

        String[] data = execute( "db.test.find({})" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"b\":\"second\"" ) );
        assertTrue( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void updateOneAddOrUpdateFieldNoConflict() {
        execute( "db.test.insertOne({\"a\":\"first\", \"b\":\"second\" })" );
        execute( "db.test.updateOne({ \"a\":\"first\" }, { $set: { \"c\":\"third\" } })" );

        String[] data = execute( "db.test.find({})" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"b\":\"second\"" ) );
        assertTrue( document.contains( "\"c\":\"third\"" ) );
        assertTrue( document.contains( "\"_eid\":" ) );
    }


    @Test
    // TODO David: find out why this does not work
    public void updateOneRemoveFieldNoConflict() {
        execute( "db.test.insertOne({\"a\":\"first\", \"b\":\"second\" })" );
        execute( "db.test.updateOne({ \"a\":\"first\" }, { $unset: { \"b\": null } });\n" );

        String[] data = execute( "db.test.find({})" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void insertSingleDocumentWithConflict() {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.test.insert({\"_eid\":\"first\", \"b\":\"second\" })" )
        );
        assertTrue( exception.getMessage().contains( "The field _eid is reserved" ) );
    }


    @Test
    public void updateOneAddOrUpdateIdentifierFieldConflict() {
        execute( "db.test.insertOne({\"a\":\"first\", \"b\":\"second\" })" );
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.test.updateOne({ \"a\":\"first\" }, { $set: { \"_eid\":-32 } })" )
        );
        assertTrue( exception.getMessage().contains( "The field _eid is reserved" ) );
    }


    @Test
    // TODO David: find out why this does not work
    public void updateOneRemoveIdentifierFieldConflict() {
        execute( "db.test.insertOne({\"a\":\"first\", \"b\":\"second\" })" );
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.test.updateOne({ \"a\":\"first\" }, { $unset: { \"_eid\": null } });" )
        );
        assertTrue( exception.getMessage().contains( "The field _eid is reserved" ) );
    }


    @Test
    public void insertOneDocumentWithConflicts() {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.test.insertOne({\"_eid\":\"first\", \"b\":\"second\" })" )
        );
        assertTrue( exception.getMessage().contains( "The field _eid is reserved" ) );
    }


    @Test
    public void insertSingleDocumentNoConflicts() {
        execute( "db.test.insert({\"a\":\"first\", \"b\":\"second\" })" );

        String[] data = execute( "db.test.find({})" ).getData();
        assertEquals( 1, data.length );
        String document = data[0];
        assertTrue( document.contains( "\"a\":\"first\"" ) );
        assertTrue( document.contains( "\"b\":\"second\"" ) );
        assertTrue( document.contains( "\"_eid\":" ) );
    }


    @Test
    public void insertManyDocumentNoConflicts() {
        execute( "db.test.insertMany([{ \"a\": \"first\", \"b\": \"second\" }, { \"a\": \"third\", \"b\": \"fourth\" }, { \"a\": \"fifth\", \"b\": \"sixth\" }])" );

        String[] data = execute( "db.test.find({})" ).getData();
        assertEquals( 3, data.length );
        for ( String document : data ) {
            assertTrue( document.contains( "\"a\":" ) );
            assertTrue( document.contains( "\"b\":" ) );
            assertTrue( document.contains( "\"_eid\":" ) );
        }
    }


    @Test
    public void insertManyDocumentWithConflicts() {
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.test.insertMany([{ \"_eid\": \"first\", \"b\": \"second\" }, { \"a\": \"third\", \"b\": \"fourth\" }, { \"a\": \"fifth\", \"b\": \"sixth\" }])" )
        );
        assertTrue( exception.getMessage().contains( "The field _eid is reserved" ) );
    }


    @Test
    public void updateManyAddOrUpdateFieldNoConflict() {
        execute( "db.test.insertMany([{\"a\":\"first\", \"b\":\"second\" }, {\"a\":\"first\", \"b\":\"second\" }, {\"a\":\"second\", \"b\":\"third\" }])" );
        execute( "db.test.updateMany({ \"a\":\"first\" }, { $set: { \"c\":\"third\" } })" );
        String[] data = execute( "db.test.find({})" ).getData();
        assertEquals( 3, data.length );

        int countUpdated = 0;
        for ( String document : data ) {
            if ( document.contains( "\"a\":\"first\"" ) ) {
                assertTrue( document.contains( "\"c\":\"third\"" ) );
                countUpdated++;
            } else {
                assertTrue( document.contains( "\"b\":\"third\"" ) );
            }
        }
        assertEquals( 2, countUpdated );
        assertTrue( data[0].contains( "\"_eid\":" ) );
    }


    @Test
    public void updateManyRemoveIdentifierFieldConflict() {
        execute( "db.test.insertMany([{\"a\":\"first\", \"b\":\"second\"}, {\"a\":\"first\", \"b\":\"third\"}, {\"a\":\"second\", \"b\":\"fourth\"}])" );
        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> execute( "db.test.updateMany({ \"a\":\"first\" }, { $unset: { \"_eid\": null } })" )
        );
        assertTrue( exception.getMessage().contains( "The field _eid is reserved" ) );
    }

}
