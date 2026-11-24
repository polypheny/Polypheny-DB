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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;


/** Writes a complete replacement before publishing it at the destination path. */
public final class DurableFile {

    private DurableFile() {
    }


    public static void replace( Path target, byte[] content ) throws IOException {
        replace( target, content, DurableFile::syncDirectory );
    }


    static void replace( Path target, byte[] content, DirectorySync sync ) throws IOException {
        Path directory = target.getParent();
        Path temporary = Files.createTempFile( directory, target.getFileName().toString(), ".tmp" );
        try {
            try ( FileChannel file = FileChannel.open( temporary, StandardOpenOption.WRITE ) ) {
                ByteBuffer buffer = ByteBuffer.wrap( content );
                while ( buffer.hasRemaining() ) {
                    file.write( buffer );
                }
                file.force( true );
            }

            Files.move( temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING );
            try {
                sync.force( directory );
            } catch ( IOException e ) {
                throw new PublishedFileException( target, e );
            }
        } finally {
            Files.deleteIfExists( temporary );
        }
    }


    private static void syncDirectory( Path directory ) throws IOException {
        // Directory channels are available on the POSIX file systems supported by Java.
        if ( Files.getFileStore( directory ).supportsFileAttributeView( "posix" ) ) {
            try ( FileChannel parent = FileChannel.open( directory, StandardOpenOption.READ ) ) {
                parent.force( true );
            }
        }
    }


    @FunctionalInterface
    interface DirectorySync {

        void force( Path directory ) throws IOException;

    }


    /** The replacement can be read in this process, but its directory entry might not survive a crash. */
    public static final class PublishedFileException extends IOException {

        private PublishedFileException( Path target, IOException cause ) {
            super( "Replaced " + target + " but could not synchronize its directory", cause );
        }

    }

}
