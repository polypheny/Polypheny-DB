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
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.jupyter.model.JupyterSessionManager;

@Slf4j
public class JupyterClient {

    public static final String HOST = "localhost:12345";
    private HttpClient client;
    private WebSocket.Builder webSocketBuilder;
    private String token;
    private final Gson gson = new Gson();
    private final JupyterSessionManager sessionManager = JupyterSessionManager.getInstance();


    public JupyterClient( String token ) {
        this.token = token;
        this.client = HttpClient.newBuilder()
                .cookieHandler( new CookieManager() )
                .build();
        this.webSocketBuilder = client.newWebSocketBuilder();
    }


    public boolean testConnection() {
        HttpResponse<String> response = sendGET( "status" );
        return response != null && response.statusCode() == 200;
    }


    public void getSession( String sessionId ) {
        HttpResponse<String> response = sendGET( "sessions/" + sessionId );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in getSession" );
            return;
        }
        updateSession( gson.fromJson( response.body(), JsonObject.class ) );
        logResponse( response );
    }


    public void getSessions() {
        HttpResponse<String> response = sendGET( "sessions" );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in getSessions" );
            return;
        }
        updateSessions( gson.fromJson( response.body(), JsonArray.class ) );
        logResponse( response );
    }


    public String createSession( String kernelName, String fileName, String filePath ) {
        JsonObject kernel = new JsonObject();
        kernel.addProperty( "name", kernelName );
        JsonObject body = new JsonObject();
        body.add( "kernel", kernel );
        body.addProperty( "name", fileName );
        body.addProperty( "path", filePath );
        body.addProperty( "type", "notebook" );

        HttpResponse<String> response = sendPOST( "sessions", body.toString() );
        if ( response == null || response.statusCode() != 201 ) {
            log.error( "Error in createSession" );
            return "ERROR";
        }
        JsonObject json = gson.fromJson( response.body(), JsonObject.class );
        updateSession( json );
        logResponse( response );
        return json.get( "id" ).getAsString();
    }


    public void renameSession( String sessionId, String fileName, String filePath ) {
        JsonObject body = new JsonObject();
        body.addProperty( "name", fileName );
        body.addProperty( "path", filePath );

        HttpResponse<String> response = sendPATCH( "sessions/" + sessionId, body.toString() );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in renameSession" );
            return;
        }
        updateSession( gson.fromJson( response.body(), JsonObject.class ) );
        logResponse( response );
    }


    public void setKernelOfSession( String sessionId, String kernelId ) {
        JsonObject kernel = new JsonObject();
        kernel.addProperty( "id", kernelId );
        JsonObject body = new JsonObject();
        body.add( "kernel", kernel );

        HttpResponse<String> response = sendPATCH( "sessions/" + sessionId, body.toString() );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in setKernelOfSession" );
            return;
        }
        updateSession( gson.fromJson( response.body(), JsonObject.class ) );
        logResponse( response );
    }


    public void deleteSession( String sessionId ) {
        HttpResponse<String> response = sendDELETE( "sessions/" + sessionId );
        if ( response == null || response.statusCode() != 204 ) {
            log.error( "Error in deleteSession" );
            return;
        }
        logResponse( response );
    }


    public void getRunningKernels() {
        HttpResponse<String> response = sendGET( "kernels" );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in getRunningKernels" );
            return;
        }
        updateKernels( gson.fromJson( response.body(), JsonArray.class ) );
        logResponse( response );
    }


    public void getKernelspecs() {
        HttpResponse<String> response = sendGET( "kernelspecs" );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in getKernelSpecs" );
            return;
        }
        updateKernelSpecs( gson.fromJson( response.body(), JsonObject.class ) );
        logResponse( response );
    }

    public void startKernel (String kernelName) {
        JsonObject body = new JsonObject();
        body.addProperty( "name", kernelName );

        HttpResponse<String> response = sendPOST( "kernels", body.toString() );
        if ( response == null || response.statusCode() != 201 ) {
            log.error( "Error in startKernel" );
            return;
        }
        JsonObject json = gson.fromJson( response.body(), JsonObject.class );
        sessionManager.addKernel( json.get( "id" ).getAsString(), json.get( "name" ).getAsString(), webSocketBuilder );
        logResponse( response );
    }


    public void deleteKernel( String kernelId ) {
        HttpResponse<String> response = sendDELETE( "kernels/" + kernelId );
        if ( response == null || response.statusCode() != 204 ) {
            log.error( "Error in deleteKernel" );
            return;
        }
        logResponse( response );
    }


    public void interruptKernel( String kernelId ) {

        HttpResponse<String> response = sendPOST( "kernels/" + kernelId + "/interrupt", "" );
        if ( response == null || response.statusCode() != 204 ) {
            log.error( "Error in interruptKernel" );
            return;
        }
        logResponse( response );
    }


    public void restartKernel( String kernelId ) {

        HttpResponse<String> response = sendPOST( "kernels/" + kernelId + "/restart", "" );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in restartKernel" );
            return;
        }
        logResponse( response );
    }

    /**
     * Format determines how file content should be returned.
     * It can be text or base64.
     */
    public void getContents( String path, String format ) {
        HttpResponse<String> response = sendGET( "contents/" + path );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in getContents" );
            return;
        }
        logResponse( response );
    }

    public void getContents( String path) {
        getContents( path, "text" );
    }


    public void createNotebook( String parentPath ) {
        JsonObject body = new JsonObject();
        body.addProperty( "type", "notebook" );

        HttpResponse<String> response = sendPOST( "contents/" + parentPath, body.toString() );
        if ( response == null || response.statusCode() != 201 ) {
            log.error( "Error in createNotebook" );
            return;
        }
        logResponse( response );
    }


    public void createDirectory( String parentPath ) {
        JsonObject body = new JsonObject();
        body.addProperty( "type", "directory" );

        HttpResponse<String> response = sendPOST( "contents/" + parentPath, body.toString() );
        if ( response == null || response.statusCode() != 201 ) {
            log.error( "Error in createDirectory" );
            return;
        }
        logResponse( response );
    }


    public void createFile( String parentPath, String extension ) {
        JsonObject body = new JsonObject();
        body.addProperty( "type", "file" );
        body.addProperty( "ext", extension );

        HttpResponse<String> response = sendPOST( "contents/" + parentPath, body.toString() );
        if ( response == null || response.statusCode() != 201 ) {
            log.error( "Error in createFile" );
            return;
        }
        logResponse( response );
    }


    /**
     * Format can be json, text or base64
     */
    public void saveFile( String filePath, String content, String format, String type ) {
        JsonObject body = new JsonObject();
        body.addProperty( "content", content );
        body.addProperty( "format", format );
        body.addProperty( "type", type );

        HttpResponse<String> response = sendPUT( "contents/" + filePath, body.toString() );
        if ( response == null || (response.statusCode() != 200 && response.statusCode() != 201) ) {
            log.error( "Error in saveFile" );
            return;
        }
        logResponse( response );
    }

    /**
     * Format can be json, text or base64
     */
    public void uploadFile( String filePath, String fileName, String content, String format, String type ) {
        // TODO: check if path is necessary
        JsonObject body = new JsonObject();
        body.addProperty( "content", content );
        body.addProperty( "format", format );
        body.addProperty( "type", type );
        body.addProperty( "name", fileName );

        HttpResponse<String> response = sendPUT( "contents/" + filePath, body.toString() );
        if ( response == null || (response.statusCode() != 200 && response.statusCode() != 201) ) {
            log.error( "Error in uploadFile" );
            return;
        }
        logResponse( response );
    }


    public void deleteFile( String filePath ) {
        HttpResponse<String> response = sendDELETE( "contents/" + filePath );
        if ( response == null || response.statusCode() != 204 ) {
            log.error( "Error in deleteFile" );
            return;
        }
        logResponse( response );
    }


    public void copyFile( String destParentPath, String srcFilePath ) {
        JsonObject body = new JsonObject();
        body.addProperty( "copy_from", srcFilePath );

        HttpResponse<String> response = sendPOST( "contents/" + destParentPath, body.toString() );
        if ( response == null || response.statusCode() != 201 ) {
            log.error( "Error in copyFile" );
            return;
        }
        logResponse( response );
    }


    /**
     * Move and rename an existing file or directory.
     */
    public void moveFile( String destFilePath, String srcFilePath ) {
        JsonObject body = new JsonObject();
        body.addProperty( "path", destFilePath );

        HttpResponse<String> response = sendPATCH( "contents/" + srcFilePath, body.toString() );
        if ( response == null || response.statusCode() != 200 ) {
            log.error( "Error in moveFile" );
            return;
        }
        logResponse( response );
    }


    private HttpResponse<String> sendGET( String resource ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri( getUriFromPath(resource) )
                    .timeout( Duration.of( 10, SECONDS ) )
                    .GET()
                    .header( "Authorization", "token " + token )
                    .build();
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            log.warn( "Caught something in sendGET: {}", e.getMessage() );
            return null;
        }
    }


    private HttpResponse<String> sendPOST( String resource, String body ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri( getUriFromPath(resource) )
                    .timeout( Duration.of( 10, SECONDS ) )
                    .POST( HttpRequest.BodyPublishers.ofString( body ) )
                    .header( "Authorization", "token " + token )
                    .build();
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            log.warn( "Caught something in sendPOST: {}", e.getMessage() );
            return null;
        }
    }


    private HttpResponse<String> sendPUT( String resource, String body ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri( getUriFromPath(resource) )
                    .timeout( Duration.of( 10, SECONDS ) )
                    .PUT( HttpRequest.BodyPublishers.ofString( body ) )
                    .header( "Authorization", "token " + token )
                    .build();
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            log.warn( "Caught something in sendPOST: {}", e.getMessage() );
            return null;
        }
    }


    private HttpResponse<String> sendPATCH( String resource, String body ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri( getUriFromPath(resource) )
                    .timeout( Duration.of( 10, SECONDS ) )
                    .method( "PATCH", HttpRequest.BodyPublishers.ofString( body ) )
                    .header( "Authorization", "token " + token )
                    .build();
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            log.warn( "Caught something in sendPATCH: {}", e.getMessage() );
            return null;
        }
    }


    private HttpResponse<String> sendDELETE( String resource ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri( getUriFromPath(resource) )
                    .timeout( Duration.of( 10, SECONDS ) )
                    .DELETE()
                    .header( "Authorization", "token " + token )
                    .build();
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            log.warn( "Caught something in sendGET: {}", e.getMessage() );
            return null;
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
        sessionManager.addKernel( kernelId, name, webSocketBuilder );

        String sessionId = session.get( "id" ).getAsString();
        String fileName = session.get( "name" ).getAsString();
        String filePath = session.get( "path" ).getAsString();
        sessionManager.updateSession( sessionId, fileName, filePath, kernelId );

    }


    private void updateKernels( JsonArray kernels ) {
        Set<String> validKernelIds = new HashSet<>();
        for ( JsonElement kernel : kernels ) {
            String id = kernel.getAsJsonObject().get( "id" ).getAsString();
            sessionManager.addKernel( id, kernel.getAsJsonObject().get( "name" ).getAsString(), webSocketBuilder );
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

    private URI getUriFromPath(String path) {
        try {
            return new URI( "http", HOST, "/api/" + path, null, null );
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( e );
        }
    }


    private void logResponse( HttpResponse<String> response ) {
        log.warn( response.toString() );
        log.warn( "Status: {}", response.statusCode() );
        log.warn( "Headers: {}", response.headers() );
        log.warn( "Body: {}\n", response.body() );
    }


}
