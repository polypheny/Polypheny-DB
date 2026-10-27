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

package org.polypheny.db.catalog.persistance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;


class FilePersisterTest {

    @Test
    void completesEachReplacementBeforeReturning( @TempDir Path directory ) throws Exception {
        Path file = directory.resolve( "catalog.poly" );
        FilePersister persister = new FilePersister( file.toFile() );
        persister.write( "first catalog" );
        assertEquals( "first catalog", persister.read() );
        persister.write( "second catalog" );
        assertEquals( "second catalog", persister.read() );
        try ( var files = Files.list( directory ) ) {
            assertEquals( 1, files.count() );
        }
    }


    @Test
    void reportsReplacementFailureAndCleansTemporaryFile( @TempDir Path directory ) throws Exception {
        Path target = Files.createDirectory( directory.resolve( "catalog.poly" ) );
        Path sentinel = target.resolve( "existing" );
        Files.writeString( sentinel, "keep" );
        FilePersister persister = new FilePersister( target.toFile() );
        assertThrows( GenericRuntimeException.class, () -> persister.write( "replacement" ) );
        assertEquals( "keep", Files.readString( sentinel ) );
        try ( var files = Files.list( directory ) ) {
            assertEquals( 1, files.count() );
        }
    }

}
