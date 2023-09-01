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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.javalin.http.Context;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.notebooks.JupyterMapFS.JupyterMapFSException;
import org.polypheny.db.notebooks.model.language.JupyterKernelLanguage;
import org.polypheny.db.notebooks.model.language.JupyterLanguageFactory;
import org.polypheny.db.notebooks.model.request.ContentsCreateRequest;
import org.polypheny.db.notebooks.model.request.ContentsRenameRequest;
import org.polypheny.db.notebooks.model.request.ContentsSaveRequest;
import org.polypheny.db.notebooks.model.response.NotebookContentModel;
import org.polypheny.db.notebooks.model.response.NotebookModel;

@Slf4j
public class JupyterContentServer {

    private final JupyterMapFS fs = new JupyterMapFS();
    private final Gson gson = new Gson();


    public void contents( final Context ctx ) {
        String content = ctx.queryParam( "content" );
        String format = ctx.queryParam( "format" );
        String path = ctx.pathParam( "path" );

        try {
            String response = fs.getContents( path, content == null || content.equals( "1" ), format );
            ctx.contentType( "application/json" );
            ctx.status( 200 );
            ctx.result( response );
        } catch ( JupyterMapFSException e ) {
            error( ctx, e );
        }
    }


    /**
     * Return the Base64 encoded contents of a file (without metadata) stored in the jupyter server.
     * This is used by the Markdown parser in the UI to retrieve local files (usually images).
     */
    public void file( final Context ctx ) {
        String path = ctx.pathParam( "path" );
        try {
            byte[] content = fs.getContent( path.replaceFirst( "/file", "" ) );

            ctx.status( 200 );
            ctx.contentType( MimetypeFromExtension.guessMimetype( path ) );
            ctx.result( content );
        } catch ( JupyterMapFSException e ) {
            error( ctx, e );
        }
    }


    /**
     * Exports all query cells of the notebook in the specified path to the specified target language.
     */
    public void export( final Context ctx ) {
        String path = ctx.pathParam( "path" );
        String language = ctx.queryParam( "language" );
        JupyterKernelLanguage exporter = JupyterLanguageFactory.getKernelLanguage( language );
        if ( exporter == null ) {
            ctx.status( 404 ).result( "Unknown language: " + language );
            return;
        }
        try {
            String response = fs.getContents( path, true, null );
            NotebookContentModel content = gson.fromJson( response, NotebookContentModel.class );
            NotebookModel nb = content.getContent();
            if ( nb == null ) {
                ctx.status( 404 ).result( "Target must be a notebook" );
                return;
            }
            nb.exportCells( exporter );
            ctx.json( content );
        } catch ( JupyterMapFSException e ) {
            error( ctx, e );
        }
    }


    private void duplicateFile( final Context ctx, String parentPath, String copyFrom ) {
        if ( !fs.hasFile( copyFrom ) ) {
            error( ctx, 404, "No such file or directory" );
            return;
        }

        ByteBuffer contents;
        try {
            contents = ByteBuffer.wrap( fs.getContent( copyFrom ) );
        } catch ( JupyterMapFSException e ) {
            error( ctx, e );
            return;
        }

        String[] parts = copyFrom.split( "/" );
        String origName = parts[parts.length - 1];

        Optional<String> file;
        String[] nameParts = origName.split( "\\." );
        String origFirstPart = nameParts[0];
        for ( int i = 1; i < 100; i++ ) {
            nameParts[0] = origFirstPart + "-Copy" + i;
            String newFilename = String.join( ".", nameParts );
            try {
                file = fs.createFile( parentPath, newFilename, contents );
                if ( file.isPresent() ) {
                    ctx.status( 201 );
                    ctx.result( file.get() );
                    break;
                }
            } catch ( JupyterMapFSException e ) {
                error( ctx, e );
                return;
            }
        }
    }


    // ============= POST =============
    public void createFile( final Context ctx ) {
        String parentPath = ctx.pathParam( "parentPath" );
        String body = ctx.body();
        ContentsCreateRequest request = gson.fromJson( body, ContentsCreateRequest.class );

        if ( request.getCopyFrom() == null ) {
            String content = null;
            // New file
            for ( int i = 0; i < 100; i++ ) {
                String name;
                Optional<String> file;
                try {
                    switch ( request.getType() ) {
                        case "notebook":
                            // TODO: Language dependent
                            name = i == 0 ? "Untitled.ipynb" : String.format( "Untitled%d.ipynb", i );
                            String defaultContent = "{\"cells\": [], \"metadata\": {}, \"nbformat\": 4, \"nbformat_minor\": 5}";
                            file = fs.createFile( parentPath, name, defaultContent );
                            break;
                        case "file":
                            name = (i == 0 ? "untitled" : String.format( "untitled%d", i )) + (request.getExt() == null ? "" : request.getExt());
                            file = fs.createFile( parentPath, name, "" );
                            break;
                        case "directory":
                            name = i == 0 ? "Untitled Folder" : String.format( "Untitled Folder %d", i );
                            file = fs.createDirectory( parentPath, name );
                            break;
                        default:
                            error( ctx, 400, "Type " + request.getType() + " is invalid" );
                            return;
                    }
                    if ( file.isPresent() ) {
                        content = file.get();
                        break;
                    }
                } catch ( JupyterMapFSException e ) {
                    error( ctx, e );
                    return;
                }
            }

            if ( content == null ) {
                error( ctx, 500, "Internal server error" );
                return;
            }

            ctx.status( 201 );
            ctx.result( content );
        } else {
            duplicateFile( ctx, parentPath, request.getCopyFrom() );
        }
    }


    // ============= PATCH =============
    public void moveFile( final Context ctx ) {
        String filePath = ctx.pathParam( "filePath" );
        String body = ctx.body();

        ContentsRenameRequest request = gson.fromJson( body, ContentsRenameRequest.class );

        if ( !fs.hasFile( filePath ) ) {
            error( ctx, 404, "No such file or directory" );
        } else {
            try {
                String content = fs.moveFile( filePath, request.getPath() );
                ctx.status( 200 );
                ctx.result( content );
            } catch ( JupyterMapFSException e ) {
                error( ctx, e );
            }
        }
    }


    // ============= PUT =============
    public void uploadFile( final Context ctx ) {
        String filePath = ctx.pathParam( "filePath" );
        String body = ctx.body();

        String type;
        try {
            JsonObject o = gson.fromJson( body, JsonObject.class );
            JsonElement el = o.get( "type" );
            type = el.getAsString();
        } catch ( ClassCastException | IllegalStateException ignore ) {
            error( ctx, 400, "Bad request" );
            return;
        }

        final byte[] content;
        if ( type.equals( "notebook" ) ) {
            Type contentType = new TypeToken<ContentsSaveRequest<NotebookModel>>() {
            }.getType();
            ContentsSaveRequest<NotebookModel> request = gson.fromJson( body, contentType );
            content = gson.toJson( request.getContent() ).getBytes();
        } else {
            Type contentType = new TypeToken<ContentsSaveRequest<String>>() {
            }.getType();
            ContentsSaveRequest<String> request = gson.fromJson( body, contentType );
            String format = request.getFormat();
            if ( request.getContent() == null ) {
                content = null;
            } else {
                if ( format.equals( "base64" ) ) {
                    content = Base64.getDecoder().decode( request.getContent() );
                } else if ( format.equals( "text" ) ) {
                    content = request.getContent().getBytes();
                } else {
                    error( ctx, 400, "Unknown format " + format );
                    return;
                }
            }
        }

        if ( !fs.hasFile( filePath ) ) {
            String[] parts = filePath.split( "/" );
            String name = parts[parts.length - 1];
            String path = String.join( "/", Arrays.asList( parts ).subList( 0, parts.length - 1 ) );

            ByteBuffer defaultContent = ByteBuffer.wrap( new byte[]{} );
            try {
                Optional<String> result;
                switch ( type ) {
                    case "notebook":
                        defaultContent = StandardCharsets.UTF_8.encode( "{\"cells\": [], \"metadata\": {}, \"nbformat\": 4, \"nbformat_minor\": 5}" );
                        // fallthrough
                    case "file":
                        result = fs.createFile( path, name, content == null ? defaultContent : ByteBuffer.wrap( content ) );
                        break;
                    case "directory":
                        result = fs.createDirectory( path, name );
                        break;
                    default:
                        error( ctx, 400, "Unknown type " + type );
                        return;
                }
                if ( result.isPresent() ) {
                    ctx.status( 201 );
                    ctx.result( result.get() );
                } else {
                    error( ctx, 500, "Failed to create file" );
                }
            } catch ( JupyterMapFSException e ) {
                error( ctx, e );
            }
        } else {
            try {
                String result = fs.putFile( filePath, content );
                ctx.status( 200 );
                ctx.result( result );
            } catch ( JupyterMapFSException e ) {
                error( ctx, e );
            }
        }
    }


    // ============= DELETE =============
    public void deleteFile( final Context ctx ) {
        String filePath = ctx.pathParam( "filePath" );

        try {
            fs.deleteFile( filePath );
        } catch ( JupyterMapFSException e ) {
            log.info( e.getMessage() );
        }

        // 204 is the only documented return code for this endpoint, so we return it even when there was an error.
        ctx.status( 204 );
        ctx.result( "" );
    }


    private void error( Context ctx, JupyterMapFSException e ) {
        error( ctx, e.getCode(), e.getMessage() );
    }


    private void error( Context ctx, int code, String message ) {
        ctx.status( code );
        ctx.result( String.format( "{\"message\": %s, \"reason\": null}", gson.toJson( message ) ) );
    }

}
