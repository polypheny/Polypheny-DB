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

package org.polypheny.db.notebooks;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Base64;
import org.polypheny.db.notebooks.model.response.NotebookModel;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
class JupyterMapFS {

    private final File rootPath;


    JupyterMapFS() {
        PolyphenyHomeDirManager phm = PolyphenyHomeDirManager.getInstance();
        rootPath = phm.registerNewFolder( "data/jupyter" );
        phm.registerNewFolder( "data/jupyter/notebooks" );
    }


    private static void validateName( String name ) throws JupyterMapFSException {
        if ( !name.matches( "^[a-zA-Z0-9_. -]+$" ) || name.contains( ".." ) || name.isBlank() ) {
            throw new JupyterMapFSException( "Invalid filename" );
        }
    }


    private static String validateAndNormalizePath( String path ) throws JupyterMapFSException {
        if ( path.isEmpty() ) {
            throw new JupyterMapFSException( "Empty path is invalid" );
        }

        if ( !path.matches( "^[a-zA-Z0-9_/.\\\\ -]+$" ) ) {
            throw new JupyterMapFSException( "Invalid character in path" );
        }

        if ( path.contains( "//" ) || path.contains( "/./" ) ) {
            throw new JupyterMapFSException( "Paths cannot contain // or /./" );
        }

        if ( path.contains( ".." ) ) {
            throw new JupyterMapFSException( "Sequence '..' is illegal in paths" );
        }

        if ( path.endsWith( "/" ) ) {
            path = path.substring( 0, path.length() - 1 );
        }

        if ( !(path.equals( "notebooks" ) || path.startsWith( "notebooks/" ) || path.startsWith( "notebooks\\" )) ) {
            throw new JupyterMapFSException( "Paths must equal notebooks, or start with notebooks/ or notebooks\\" );
        }

        return path;
    }


    private File walkToFile( String path ) throws JupyterMapFSException {
        path = validateAndNormalizePath( path );
        String[] parts = path.split( "/" );

        File cur = rootPath;
        outer:
        for ( String next : parts ) {
            // Very strict, as these should have already been filtered out before
            if ( next.isEmpty() || next.equals( ".." ) || next.equals( "." ) ) {
                throw new AssertionError( "invalid path supplied to walkToPath" );
            }

            if ( !cur.isDirectory() ) {
                throw new JupyterMapFSException( 404, "Not a directory: %s" + cur );
            }

            for ( File entry : Objects.requireNonNull( cur.listFiles() ) ) {
                if ( entry.getName().equals( next ) ) {
                    cur = entry;
                    continue outer;
                }
            }
            throw new JupyterMapFSException( 404, "No such file or directory: " + next );
        }

        if ( !cur.getAbsolutePath().equals( rootPath.getAbsolutePath() + File.separator + path.replace( "/", File.separator ) ) ) {
            // Not a JupyterMapFSException because this should not happen
            throw new AssertionError( "error while walking the filesystem" );
        }

        return cur;
    }


    private File createNewFile( String path, String name ) throws JupyterMapFSException {
        validateName( name );
        File f = walkToFile( path );
        if ( !f.isDirectory() ) {
            throw new JupyterMapFSException( "Cannot create file: '" + path + "' is not a directory" );
        }
        validateAndNormalizePath( f.getAbsolutePath().replaceFirst(
                Pattern.quote( rootPath.getAbsolutePath() + File.separator ), "" )
                + File.separator + name );
        return new File( f.getAbsolutePath() + File.separator + name );
    }


    String getContents( String path, boolean showContent, String format ) throws JupyterMapFSException {
        File f = walkToFile( path );

        if ( f.isDirectory() ) {
            return jsonDirectory( f, path, showContent );
        }

        return jsonFile( f, path, showContent, format );
    }


    Optional<String> createFile( String parentPath, String name, ByteBuffer content ) throws JupyterMapFSException {
        validateName( name );
        try {
            File n = createNewFile( parentPath, name );
            if ( n.createNewFile() ) {
                try ( FileOutputStream w = new FileOutputStream( n ) ) {
                    while ( content.hasRemaining() ) {
                        w.write( content.get() );
                    }
                }
                return Optional.of( getContents( parentPath + "/" + name, false, null ) );
            }
            return Optional.empty();
        } catch ( IOException e ) {
            log.error( "Jupyter: Unexpected error while creating file '" + name + "' in '" + parentPath + "':", e );
            throw new JupyterMapFSException( "Internal server error" );
        }
    }


    Optional<String> createFile( String parentPath, String name, String content ) throws JupyterMapFSException {
        return createFile( parentPath, name, StandardCharsets.UTF_8.encode( content ) );
    }


    Optional<String> createDirectory( String parentPath, String name ) throws JupyterMapFSException {
        validateName( name );
        try {
            File n = createNewFile( parentPath, name );
            if ( n.mkdir() ) {
                return Optional.of( getContents( parentPath + "/" + name, false, null ) );
            }
            return Optional.empty();
        } catch ( IOException e ) {
            log.error( "Jupyter: Unexpected error while creating directory '" + name + "' in '" + parentPath + "':", e );
            throw new JupyterMapFSException( "Internal server error" );
        }
    }


    String moveFile( String src, String dst ) throws JupyterMapFSException {
        try {
            dst = validateAndNormalizePath( dst );

            File fsrc = walkToFile( src );
            File fdst = new File( rootPath.getAbsolutePath() + File.separator + dst.replace( "/", File.separator ) );

            Files.move( Path.of( fsrc.getAbsolutePath() ), Path.of( fdst.getAbsolutePath() ) );
            return getContents( dst, false, null );
        } catch ( FileAlreadyExistsException ignore ) {
            // Jupyter Server returns 409 in this case
            throw new JupyterMapFSException( 409, "Cannot move file: destination file already exists" );
        } catch ( JupyterMapFSException e ) {
            throw e;
        } catch ( IOException e ) {
            log.error( "Jupyter: Unexpected error while moving '" + src + "' to '" + dst + "':", e );
            throw new JupyterMapFSException( "Internal server error" );
        }
    }


    String putFile( String filePath, byte[] content ) throws JupyterMapFSException {
        File f = walkToFile( filePath );
        try ( FileOutputStream w = new FileOutputStream( f ) ) {
            w.write( content );
            return getContents( filePath, false, null );
        } catch ( FileNotFoundException e ) {
            throw new JupyterMapFSException( 404, "No such file or directory" );
        } catch ( JupyterMapFSException e ) {
            throw e;
        } catch ( IOException e ) {
            log.error( "Jupyter: Unexpected error while saving file '" + filePath + "':", e );
            throw new JupyterMapFSException( "Internal server error" );
        }
    }


    void deleteFile( String filePath ) throws JupyterMapFSException {
        File f = walkToFile( filePath );
        if ( !f.delete() ) {
            throw new JupyterMapFSException( "Failed to delete " + filePath );
        }
    }


    boolean hasFile( String path ) {
        try {
            walkToFile( path );
            return true;
        } catch ( IOException ignore ) {
            return false;
        }
    }


    byte[] getContent( String path ) throws JupyterMapFSException {
        File f = walkToFile( path );
        if ( f.isDirectory() ) {
            throw new JupyterMapFSException( "Cannot return the content of directories" );
        }
        try ( FileInputStream in = new FileInputStream( f ) ) {
            return in.readAllBytes();
        } catch ( FileNotFoundException e ) {
            throw new JupyterMapFSException( 404, "No such file or directory" );
        } catch ( IOException e ) {
            log.error( "Jupyter: Unexpected error while retrieving file '" + path + "':", e );
            throw new JupyterMapFSException( "Internal server error" );
        }
    }


    private String formatJsonKv( String key, String value ) {
        if ( value != null ) {
            value = value.replace( "\"", "\\\"" );
            return formatJsonRaw( key, "\"" + value + "\"" );
        } else {
            return formatJsonRaw( key, "null" );
        }
    }


    private String formatJsonRaw( String key, String rawValue ) {
        key = key.replace( "\"", "\\\"" );
        return String.format( "\"%s\": %s", key, rawValue );
    }


    private String jsonEntry(
            File f, String name, String path, String content, String format, String mimetype, String size,
            String writable, String type ) throws JupyterMapFSException {
        String creationTime;
        String lastModified;
        try {
            BasicFileAttributes a = Files.readAttributes( f.toPath(), BasicFileAttributes.class );
            creationTime = a.creationTime().toString();
            lastModified = a.lastModifiedTime().toString();
        } catch ( IOException e ) {
            log.error( "Failed to retrieve attributes: ", e );
            throw new JupyterMapFSException( "Internal server error" );
        }
        return "{"
                + formatJsonKv( "name", name ) + ", "
                + formatJsonKv( "path", path ) + ", "
                + formatJsonKv( "last_modified", lastModified ) + ", "
                + formatJsonKv( "created", creationTime ) + ", "
                + formatJsonRaw( "content", content ) + ", "
                + formatJsonKv( "format", format ) + ", "
                + formatJsonKv( "mimetype", mimetype ) + ", "
                + formatJsonRaw( "size", size ) + ", "
                + formatJsonRaw( "writable", writable ) + ", "
                + formatJsonKv( "type", type.toLowerCase() )
                + "}";
    }


    private String jsonDirectory( File f, String path, boolean showContent ) throws JupyterMapFSException {
        String contents = null;
        if ( showContent ) {
            File[] entries = f.listFiles();
            if ( entries == null ) {
                throw new JupyterMapFSException( "Failed to read directory entries" );
            } else {
                List<String> jsonEntries = new ArrayList<>();
                for ( File entry : entries ) {
                    if ( entry.isDirectory() ) {
                        jsonEntries.add( jsonDirectory( entry, path + "/" + entry.getName(), false ) );
                    } else {
                        jsonEntries.add( jsonFile( entry, path + "/" + entry.getName(), false, null ) );
                    }
                }
                contents = "[" + String.join( ", ", jsonEntries ) + "]";
            }
        }

        return jsonEntry(
                f,
                f.getName(),
                path,
                contents,
                "json",
                null,
                null,
                String.valueOf( f.canWrite() ),
                "directory"
        );

    }


    private Optional<String> getNotebook( byte[] content ) {
        try {
            NotebookModel model = new Gson().fromJson( StandardCharsets.UTF_8.decode( ByteBuffer.wrap( content ) ).toString(), NotebookModel.class );
            if ( model == null ) {
                return Optional.empty();
            }
            return Optional.of( new Gson().toJson( model ) );
        } catch ( JsonSyntaxException ignore ) {
            return Optional.empty();
        }
    }


    private String jsonFile( File f, String path, boolean showContent, String format ) throws JupyterMapFSException {
        final String content;
        try ( FileInputStream in = new FileInputStream( f ) ) {
            byte[] raw = in.readAllBytes();
            Optional<String> notebook = getNotebook( raw );
            if ( notebook.isPresent() ) {
                return jsonEntry(
                        f,
                        f.getName(),
                        path,
                        showContent ? notebook.get() : null,
                        showContent ? "json" : null,
                        null,
                        String.valueOf( notebook.get().length() ),
                        String.valueOf( f.canWrite() ),
                        "notebook"
                );
            }
            if ( format != null && format.equals( "base64" ) ) {
                content = "\"" + Base64.encodeBytes( raw ) + "\"";
            } else {
                content = new Gson().toJson( StandardCharsets.UTF_8.decode( ByteBuffer.wrap( raw ) ).toString() );
            }
        } catch ( IOException e ) {
            log.error( "Jupyter: Unexpected error while retrieving file '" + path + "':", e );
            throw new JupyterMapFSException( "Internal server error" );
        }

        return jsonEntry(
                f,
                f.getName(),
                path,
                showContent ? content : null,
                showContent ? format != null ? format : "text" : null,
                MimetypeFromExtension.guessMimetype( f.getName() ),
                String.valueOf( content.length() ),
                String.valueOf( f.canWrite() ),
                "file"
        );

    }


    static class JupyterMapFSException extends IOException {

        @Getter
        private final int code;
        @Getter
        private final String message;


        JupyterMapFSException( String message ) {
            this.code = 500;
            this.message = message;
        }


        JupyterMapFSException( int code, String message ) {
            this.code = code;
            this.message = message;
        }

    }

}
