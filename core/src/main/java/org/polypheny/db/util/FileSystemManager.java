/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.util;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * The FileSystemManager should handle all dynamically needed resources and sort them
 */
public class FileSystemManager {

    static FileSystemManager fileSystemManager = null;
    final File root = new File( System.getProperty( "user.home" ), ".polypheny" );
    final List<File> dirs = new ArrayList<>();


    public static FileSystemManager getInstance() {
        if ( fileSystemManager == null ) {
            fileSystemManager = new FileSystemManager();
        }
        return fileSystemManager;
    }


    private FileSystemManager() {
        if ( !root.exists() ) {
            root.mkdir();
        }
    }


    /**
     * Registers a new data folder
     *
     * @param path the path of the new folder
     * @return the file object for the directory
     */
    public File registerDataFolder( String path ) {
        File file = new File( this.root, path );
        if ( !file.exists() ) {
            file.mkdirs();
            dirs.add( file );
        }

        return file;
    }


    public File registerNewFile( String pathToFile ) {
        return registerNewFile( root, pathToFile );
    }


    /**
     * places a new file in a specific path, if no path is specified, it is places in the root path
     *
     * @param path       path to the folder in which the file should be placed
     * @param pathToFile the file and its parent paths
     * @return the file object of the file itself
     */
    public File registerNewFile( File path, String pathToFile ) {
        File file = new File( path, pathToFile
                .replace( "//", "/" )
                .replace( "/", "\\" )
                .replace( "//", "\\" ) );
        if ( !file.exists() ) {
            try {
                file.mkdirs();
                file.createNewFile();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
        return file;
    }

}
