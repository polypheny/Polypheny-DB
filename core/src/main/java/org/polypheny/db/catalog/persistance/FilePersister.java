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

package org.polypheny.db.catalog.persistance;

import com.drew.lang.Charsets;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.PolyphenyHomeDirManager;

public class FilePersister implements Persister {

    private final File backup;


    public FilePersister() {
        this( initBackupFile() );
    }


    FilePersister( File backup ) {
        this.backup = backup;
    }


    private static File initBackupFile() {
        if ( PolyphenyHomeDirManager.getInstance().getHomeFile( "catalog" ).isEmpty() ) {
            PolyphenyHomeDirManager.getInstance().registerNewFolder( "catalog" );
        }
        Optional<File> folder = PolyphenyHomeDirManager.getInstance().getHomeFile( "catalog" );
        if ( !folder.map( File::isDirectory ).orElse( false ) ) {
            throw new GenericRuntimeException( "There is an error with the catalog folder in the .polypheny folder." );
        }
        return PolyphenyHomeDirManager.getInstance().registerNewFile( "catalog/catalog.poly" );
    }


    /**
     * Writes synchronously, so failures propagate to the committing transaction.
     * Writes to a separate file and forces its contents to disk.
     * Atomically replaces the previous catalog file, protecting it from a partially completed write.
     * This lets PolyCatalog.commit() persist first, then publish the snapshot and advance the rollback checkpoint.
     * It protects the whole catalog, including mapping rules and their associated logical names.
     */
    @Override
    public synchronized void write( String data ) {
        // mapping update is reported as successful
        // only after the catalog has been written successfully
        Path temporary = null;
        try {
            Path target = backup.toPath();
            temporary = Files.createTempFile( target.getParent(), "catalog-", ".tmp" );
            Files.writeString( temporary, data, Charsets.ISO_8859_1 );
            try ( FileChannel channel = FileChannel.open( temporary, StandardOpenOption.WRITE ) ) {
                channel.force( true );
            }
            // Fail rather than truncate the previous catalog if atomic replacement is unavailable.
            Files.move( temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Could not persist the catalog.", e );
        } finally {
            if ( temporary != null ) {
                try {
                    Files.deleteIfExists( temporary );
                } catch ( IOException ignored ) {
                    // The previous catalog remains intact; a temporary file can be cleaned up later.
                }
            }
        }
    }


    @Override
    public synchronized String read() {
        StringBuilder data = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader( new FileReader( backup, Charsets.ISO_8859_1 ) );
            int c;
            while ( ((c = reader.read()) != -1) ) {
                data.append( (char) c );
            }
            reader.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }
        return data.toString();
    }

}
