/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.webui;


import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;


/**
 * Static manager that remembers files that were created during a transaction and can delete them on request.
 * It will delete all files on shutdown as well.
 */
@Slf4j
public final class TemporalFileManager {

    private TemporalFileManager() {
        //intentionally empty
    }


    static ConcurrentHashMap<String, Set<File>> temporaryFiles = new ConcurrentHashMap<>();


    static {
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            for ( Set<File> files : temporaryFiles.values() ) {
                for ( File file : files ) {
                    file.delete();
                }
            }
        } ) );
    }


    /**
     * Add a file to the manager
     * The file will only be added to the manager if it exists
     *
     * @param xid Transaction id
     * @param file File that was created during a transaction
     */
    public static boolean addFile( final String xid, final File file ) {
        if ( file.exists() ) {
            if ( !temporaryFiles.containsKey( xid ) ) {
                temporaryFiles.put( xid, new HashSet<>() );
            }
            temporaryFiles.get( xid ).add( file );
        }
        return file.exists();
    }


    /**
     * See {@link #addFile}
     */
    public static boolean addPath( final String xid, final Path path ) {
        File f = new File( path.toString() );
        return addFile( xid, f );
    }


    public static void deleteFilesOfTransaction( final String xid ) {
        if ( !temporaryFiles.containsKey( xid ) ) {
            return;
        }
        for ( File file : temporaryFiles.get( xid ) ) {
            file.delete();
        }
        temporaryFiles.remove( xid );
    }

}
