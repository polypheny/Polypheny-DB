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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.catalog.persistance.DurableFile.PublishedFileException;


class DurableFileTest {

    @TempDir
    Path directory;


    @Test
    void synchronizationFailureReportsAlreadyPublishedReplacement() throws IOException {
        Path target = directory.resolve( "catalog.poly" );
        byte[] replacement = { 4, 5, 6 };
        Files.write( target, new byte[]{ 1, 2, 3 } );
        IOException syncFailure = new IOException( "Injected directory synchronization failure" );

        PublishedFileException failure = assertThrows( PublishedFileException.class, () -> DurableFile.replace( target, replacement, ignored -> {
            assertArrayEquals( replacement, Files.readAllBytes( target ) );
            throw syncFailure;
        } ) );

        assertSame( syncFailure, failure.getCause() );
        assertArrayEquals( replacement, Files.readAllBytes( target ) );
    }

}
