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
    File root = new File( System.getProperty( "user.home" ) != null ? System.getProperty( "user.home" ) : ".", ".polypheny" );
    final List<File> dirs = new ArrayList<>();
    final List<File> deleteOnExit = new ArrayList<>();


    public static FileSystemManager getInstance() {
        if ( fileSystemManager == null ) {
            fileSystemManager = new FileSystemManager();
        }
        return fileSystemManager;
    }


    private FileSystemManager() {
        if ( !tryCreatingFolder( root ) ) {
            root = new File( "." );
            if ( !tryCreatingFolder( root ) ) {
                throw new RuntimeException( "Could not create root directory: .polypheny neither in:" + System.getProperty( "user.home" ) + " nor \".\"" );
            }
        }

        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            for ( File file : deleteOnExit ) {
                if ( file.exists() ) {
                    recursiveDeleteFolder( file );
                }
            }
        } ) );
    }


    private boolean tryCreatingFolder( File file ) {
        if ( file.isFile() ) {
            return false;
        }

        boolean couldCreate = true;
        if ( !root.exists() ) {
            couldCreate = root.mkdirs();
        }
        return couldCreate && root.canWrite();
    }


    /**
     * Registers a new folder
     *
     * @param path the path of the new folder
     * @return the file object for the directory
     */
    public File registerNewFolder( File root, String path ) {
        File file = root;
        if ( !file.setWritable( true ) ) {
            throw new RuntimeException( "Directory " + path + " is not writable" );
        }

        if ( path.contains( "/" ) ) {
            String[] splits = path.split( "/" );
            for ( String split : splits ) {
                file = registerNewFolder( file, split );
            }
        } else {
            file = new File( root, path );
        }

        if ( !file.exists() ) {

            if ( !file.mkdirs() ) {
                throw new RuntimeException( "Could not create directory: " + path + " in parent folder: " + root.getAbsolutePath() );
            }
            dirs.add( file );
        }

        return file;
    }


    public File registerNewFile( String pathToFile ) {
        return registerNewFile( root, pathToFile );
    }


    public boolean checkIfExists( String path ) {
        File file = new File( this.root, path );
        return file.exists();
    }


    public boolean moveFolder( String oldPath, String newPath ) {
        if ( checkIfExists( newPath ) ) {
            throw new RuntimeException( "Target folder does already exist." );
        }
        File file = new File( this.root, oldPath );
        return file.renameTo( new File( this.root, newPath ) );
    }


    public boolean recursiveDeleteFolder( String path ) {
        File folder = new File( this.root, path );
        if ( folder.exists() ) {
            return recursiveDeleteFolder( folder );
        }
        return true;
    }


    public void recursiveDeleteFolderOnExit( String path ) {
        File folder = new File( this.root, path );
        if ( !folder.exists() ) {
            throw new RuntimeException( "There is no directory with this name: " + folder.getPath() );
        }
        deleteOnExit.add( folder );
    }


    private boolean recursiveDeleteFolder( File folder ) {
        File[] allContents = folder.listFiles();
        if ( allContents != null ) {
            for ( File file : allContents ) {
                recursiveDeleteFolder( file );
            }
        }
        return folder.delete();
    }


    /**
     * places a new file in a specific path, if no path is specified, it is places in the root path
     *
     * @param path path to the folder in which the file should be placed
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


    public File registerNewFolder( String folder ) {
        return registerNewFolder( this.root, folder );
    }

}
