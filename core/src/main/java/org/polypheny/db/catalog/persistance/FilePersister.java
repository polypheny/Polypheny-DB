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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.PolyphenyHomeDirManager;

public class FilePersister implements Persister {

    private final Path backup;


    public FilePersister() {
        this.backup = initBackupFile().toPath();
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


    public Path getDirectory() {
        return backup.getParent();
    }


    @Override
    public synchronized void write( String data ) {
        try {
            DurableFile.replace( backup, data.getBytes( StandardCharsets.ISO_8859_1 ) );
        } catch ( DurableFile.PublishedFileException e ) {
            throw new CatalogPublicationException( e );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Cannot persist catalog.", e );
        }
    }


    @Override
    public synchronized String read() {
        try {
            return Files.readString( backup, StandardCharsets.ISO_8859_1 );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Cannot read catalog.", e );
        }
    }

    /** The catalog is visible at its final path, but crash durability could not be confirmed. */
    public static final class CatalogPublicationException extends GenericRuntimeException {

        public CatalogPublicationException( IOException cause ) {
            super( "The catalog change was published, but its durability could not be confirmed.", cause );
        }

    }

}
