/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;


class JsonEnumeratorTest {

    @BeforeAll
    static void setup() {
        TestHelper.getInstance();
    }


    @Test
    void flatArrayElementsAreReturnedAsDocuments() throws Exception {
        Path json = Files.createTempFile( "polypheny-flat-array", ".json" );
        Files.writeString( json, "[1,\"two\",[3,4]]" );

        JsonEnumerator enumerator = new JsonEnumerator( json.toUri().toURL() );
        try {
            assertTrue( enumerator.moveNext() );
            assertEquals( 1L, getDataValue( enumerator ).asLong().value );

            assertTrue( enumerator.moveNext() );
            assertEquals( "two", getDataValue( enumerator ).asString().value );

            assertTrue( enumerator.moveNext() );
            PolyList<PolyValue> list = getDataValue( enumerator ).asList();
            assertEquals( 3L, list.get( 0 ).asLong().value );
            assertEquals( 4L, list.get( 1 ).asLong().value );

            assertFalse( enumerator.moveNext() );
        } finally {
            enumerator.close();
            Files.deleteIfExists( json );
        }
    }


    private PolyValue getDataValue( JsonEnumerator enumerator ) {
        PolyDocument document = enumerator.current()[0].asDocument();
        return document.get( PolyString.of( DocumentType.DOCUMENT_DATA ) );
    }

}
