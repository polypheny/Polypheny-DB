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

package org.polypheny.db.cql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cql.EntityIndex;
import org.polypheny.db.cql.FieldIndex;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.utils.helper.CqlTestHelper;


public class IndexTest extends CqlTestHelper {

    @Test
    public void testCreateColumnIndex() throws UnknownIndexException {
        FieldIndex index = FieldIndex.createIndex( "test", "testtable", "tbigint" );
        assertEquals( index.fullyQualifiedName, "test.testtable.tbigint" );
    }


    @Test
    public void testCreateColumnIndexThrowsUnknownIndexException() throws UnknownIndexException {
        NoSuchElementException thrown = assertThrows( NoSuchElementException.class, () -> {
            FieldIndex.createIndex( "hello", "world", "!" );
        } );
    }


    @Test
    public void testCreateTableIndex() throws UnknownIndexException {
        EntityIndex index = EntityIndex.createIndex( "test", "testtable" );
        assertEquals( index.fullyQualifiedName, "test.testtable" );
    }


    @Test
    public void testCreateTableIndexThrowsUnknownIndexException() throws UnknownIndexException {
        NoSuchElementException thrown = assertThrows( NoSuchElementException.class, () -> {
            EntityIndex.createIndex( "hello", "world" );
        } );
    }

}
