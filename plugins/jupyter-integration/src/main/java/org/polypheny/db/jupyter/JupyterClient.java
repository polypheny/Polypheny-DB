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

package org.polypheny.db.jupyter;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.javalin.http.HttpCode;
import java.io.IOException;
import java.net.CookieManager;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.jupyter.model.JupyterSessionManager;

@Slf4j
public class JupyterClient {

    private final String host;
    private final HttpClient client;
    private final WebSocket.Builder webSocketBuilder;
    private final String token;
    private final Gson gson = new Gson();
    private final JupyterSessionManager sessionManager = JupyterSessionManager.getInstance();


    public JupyterClient( String token, String host, int port ) {
        this.token = token;
        this.host = host + ":" + port;
        this.client = HttpClient.newBuilder()
                .cookieHandler( new CookieManager() )
                .build();
        this.webSocketBuilder = client.newWebSocketBuilder();
    }


    public boolean testConnection() {
        try {
            HttpResponse<String> response = sendGET( "status" );
            return response.statusCode() == 200;
        } catch ( JupyterServerException e ) {
            return false;
        }
    }


    public HttpResponse<String> getSession( String sessionId ) throws JupyterServerException {
        HttpResponse<String> response = sendGET( "sessions/" + sessionId );
        if ( response.statusCode() == 200 ) {
            updateSession( gson.fromJson( response.body(), JsonObject.class ) );
        } else {
            // if not successful, the jupyter server sends a html page that we do not want to forward to the UI
            throw new JupyterServerException( "Requested session not found", 404 );
        }
        return response;
    }


    public HttpResponse<String> getSessions() throws JupyterServerException {
        HttpResponse<String> response = sendGET( "sessions" );
        if ( response.statusCode() == 200 ) {
            updateSessions( gson.fromJson( response.body(), JsonArray.class ) );
        }
        log.warn( sessionManager.getOverview() );
        return response;
    }


    public HttpResponse<String> createSession( String body ) throws JupyterServerException {
        HttpResponse<String> response = sendPOST( "sessions", body );
        if ( response.statusCode() == 201 ) {
            JsonObject json = gson.fromJson( response.body(), JsonObject.class );
            updateSession( json );
        }
        return response;
    }


    public HttpResponse<String> createSession( String kernelName, String fileName, String filePath ) throws JupyterServerException {
        JsonObject kernel = new JsonObject();
        kernel.addProperty( "name", kernelName );
        JsonObject body = new JsonObject();
        body.add( "kernel", kernel );
        body.addProperty( "name", fileName );
        body.addProperty( "path", filePath );
        body.addProperty( "type", "notebook" );

        return createSession( body.toString() );
    }


    public HttpResponse<String> patchSession( String sessionId, String body ) throws JupyterServerException {
        HttpResponse<String> response = sendPATCH( "sessions/" + sessionId, body );
        if ( response.statusCode() == 200 ) {
            updateSession( gson.fromJson( response.body(), JsonObject.class ) );
        }
        return response;
    }


    public HttpResponse<String> renameSession( String sessionId, String fileName, String filePath ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "name", fileName );
        body.addProperty( "path", filePath );

        return patchSession( sessionId, body.toString() );
    }


    public HttpResponse<String> setKernelOfSession( String sessionId, String kernelId ) throws JupyterServerException {
        JsonObject kernel = new JsonObject();
        kernel.addProperty( "id", kernelId );
        JsonObject body = new JsonObject();
        body.add( "kernel", kernel );

        return patchSession( sessionId, body.toString() );
    }


    public HttpResponse<String> deleteSession( String sessionId ) throws JupyterServerException {
        HttpResponse<String> response = sendDELETE( "sessions/" + sessionId );
        if ( response.statusCode() == 204 ) {
            sessionManager.invalidateSession( sessionId );
        }
        return response;
    }


    public HttpResponse<String> getRunningKernels() throws JupyterServerException {
        HttpResponse<String> response = sendGET( "kernels" );
        if ( response.statusCode() == 200 ) {
            updateKernels( gson.fromJson( response.body(), JsonArray.class ) );
        }
        return response;
    }


    public HttpResponse<String> getKernelspecs() throws JupyterServerException {
        HttpResponse<String> response = sendGET( "kernelspecs" );
        if ( response.statusCode() == 200 ) {
            updateKernelSpecs( gson.fromJson( response.body(), JsonObject.class ) );
        }
        return response;
    }


    public HttpResponse<String> interruptKernel( String kernelId ) throws JupyterServerException {
        return sendPOST( "kernels/" + kernelId + "/interrupt", "" );
    }


    public HttpResponse<String> restartKernel( String kernelId ) throws JupyterServerException {
        return sendPOST( "kernels/" + kernelId + "/restart", "" );
    }


    public HttpResponse<String> getContents( String path ) throws JupyterServerException {
        return sendGET( "contents/" + path );
    }


    public HttpResponse<String> createFile( String parentPath, String body ) throws JupyterServerException {
        return sendPOST( "contents/" + parentPath, body );
    }


    public HttpResponse<String> createNotebook( String parentPath ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "type", "notebook" );

        return createFile( parentPath, body.toString() );
    }


    public HttpResponse<String> createDirectory( String parentPath ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "type", "directory" );

        return createFile( parentPath, body.toString() );
    }


    public HttpResponse<String> createFileWithExtension( String parentPath, String extension ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "type", "file" );
        body.addProperty( "ext", extension );
        return createFile( parentPath, body.toString() );
    }


    public HttpResponse<String> copyFile( String destParentPath, String srcFilePath ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "copy_from", srcFilePath );
        return createFile( destParentPath, body.toString() );
    }


    public HttpResponse<String> putFile( String filePath, String body ) throws JupyterServerException {
        return sendPUT( "contents/" + filePath, body );
    }


    /**
     * Format can be json, text or base64
     */
    public HttpResponse<String> saveFile( String filePath, String content, String format, String type ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "content", content );
        body.addProperty( "format", format );
        body.addProperty( "type", type );

        return putFile( filePath, body.toString() );
    }


    /**
     * Format can be json, text or base64
     */
    public HttpResponse<String> uploadFile( String filePath, String fileName, String content, String format, String type ) throws JupyterServerException {
        JsonObject body = new JsonObject();
        body.addProperty( "content", content );
        body.addProperty( "format", format );
        body.addProperty( "type", type );
        body.addProperty( "name", fileName );

        return putFile( filePath, body.toString() );
    }


    public HttpResponse<String> deleteFile( String filePath ) throws JupyterServerException {
        return sendDELETE( "contents/" + filePath );
    }


    /**
     * Move and/or rename an existing file or directory.
     */

    public HttpResponse<String> moveFile( String srcFilePath, String body ) throws JupyterServerException {
        return sendPATCH( "contents/" + srcFilePath, body );
    }


    private HttpResponse<String> sendGET( String resource ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, SECONDS ) )
                .GET()
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "GET failed: Jupyter Server is unavailable", HttpCode.SERVICE_UNAVAILABLE );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "GET failed: Thread was interrupted", HttpCode.SERVICE_UNAVAILABLE );
        }
    }


    private HttpResponse<String> sendPOST( String resource, String body ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, SECONDS ) )
                .POST( HttpRequest.BodyPublishers.ofString( body ) )
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "POST failed: Jupyter Server is unavailable", HttpCode.SERVICE_UNAVAILABLE );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "POST failed: Thread was interrupted", HttpCode.SERVICE_UNAVAILABLE );
        }
    }


    private HttpResponse<String> sendPUT( String resource, String body ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, SECONDS ) )
                .PUT( HttpRequest.BodyPublishers.ofString( body ) )
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "PUT failed: Jupyter Server is unavailable", HttpCode.SERVICE_UNAVAILABLE );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "PUT failed: Thread was interrupted", HttpCode.SERVICE_UNAVAILABLE );
        }
    }


    private HttpResponse<String> sendPATCH( String resource, String body ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, SECONDS ) )
                .method( "PATCH", HttpRequest.BodyPublishers.ofString( body ) )
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "PATCH failed: Jupyter Server is unavailable", HttpCode.SERVICE_UNAVAILABLE );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "PATCH failed: Thread was interrupted", HttpCode.SERVICE_UNAVAILABLE );
        }
    }


    private HttpResponse<String> sendDELETE( String resource ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, SECONDS ) )
                .DELETE()
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "DELETE failed: Jupyter Server is unavailable", HttpCode.SERVICE_UNAVAILABLE );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "DELETE failed: Thread was interrupted", HttpCode.SERVICE_UNAVAILABLE );
        }
    }


    /**
     * Updates the sessions stored in the JupyterSessionManager according to the specified JsonArray.
     * Existing session that do not occur in sessions are removed.
     */
    private void updateSessions( JsonArray sessions ) {
        Set<String> validSessionIds = new HashSet<>();
        for ( JsonElement session : sessions ) {
            validSessionIds.add( session.getAsJsonObject().get( "id" ).getAsString() );
        }
        sessionManager.retainValidSessions( validSessionIds );

        for ( JsonElement session : sessions ) {
            updateSession( session.getAsJsonObject() );
        }
    }


    private void updateSession( JsonObject session ) {
        JsonObject kernel = session.getAsJsonObject( "kernel" );
        String kernelId = kernel.get( "id" ).getAsString();
        String name = kernel.get( "name" ).getAsString();
        sessionManager.addKernel( kernelId, name, webSocketBuilder, host );

        String sessionId = session.get( "id" ).getAsString();
        String fileName = session.get( "name" ).getAsString();
        String filePath = session.get( "path" ).getAsString();
        sessionManager.updateSession( sessionId, fileName, filePath, kernelId );

    }


    private void updateKernels( JsonArray kernels ) {
        Set<String> validKernelIds = new HashSet<>();
        for ( JsonElement kernel : kernels ) {
            String id = kernel.getAsJsonObject().get( "id" ).getAsString();
            sessionManager.addKernel( id, kernel.getAsJsonObject().get( "name" ).getAsString(), webSocketBuilder, host );
            validKernelIds.add( id );
        }
        sessionManager.retainValidKernels( validKernelIds );
    }


    private void updateKernelSpecs( JsonObject kernelSpecs ) {
        sessionManager.setDefaultKernel( kernelSpecs.get( "default" ).getAsString() );
        for ( Entry<String, JsonElement> entry : kernelSpecs.get( "kernelspecs" ).getAsJsonObject().entrySet() ) {
            JsonObject spec = entry.getValue().getAsJsonObject().get( "spec" ).getAsJsonObject();
            sessionManager.addKernelSpec( entry.getKey(),
                    spec.get( "display_name" ).getAsString(),
                    spec.get( "language" ).getAsString() );
        }

    }


    private URI getUriFromPath( String path ) {
        try {
            return new URI( "http", host, "/api/" + path, null, null );
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( e );
        }
    }


    public static class JupyterServerException extends Exception {

        @Getter
        private final String msg;
        @Getter
        private final HttpCode status;


        public JupyterServerException( String msg, HttpCode status ) {
            this.msg = msg;
            this.status = Objects.requireNonNullElse( status, HttpCode.INTERNAL_SERVER_ERROR );
        }


        public JupyterServerException( String msg, int status ) {
            this.msg = msg;
            this.status = Objects.requireNonNullElse( HttpCode.Companion.forStatus( status ), HttpCode.INTERNAL_SERVER_ERROR );
        }

    }


}
