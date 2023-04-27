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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.jupyter.model.JupyterKernel;
import org.polypheny.db.jupyter.model.JupyterSessionManager;

@Slf4j
public class JupyterClient {

    public static final String URL = "http://localhost:8888/api/";
    public static final String WS_URL = "ws://localhost:8888/api/";
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


    public String getContents( String path ) {
        HttpResponse<String> response = sendGET( "contents/" + path );
        if ( response == null || response.statusCode() != 200 ) {
            return "ERROR";
        }
        logResponse( response );
        return response.body();
    }


    public String getSession( String sessionId ) {
        HttpResponse<String> response = sendGET( "sessions/" + sessionId );
        if ( response == null || response.statusCode() != 200 ) {
            return "ERROR";
        }
        logResponse( response );
        return response.body();
    }


    public String getSessions() {
        HttpResponse<String> response = sendGET( "sessions" );
        if ( response == null || response.statusCode() != 200 ) {
            return "ERROR";
        }
        updateSessions( gson.fromJson( response.body(), JsonArray.class ) );
        logResponse( response );
        return response.body();
    }


    public String getRunningKernels() {
        HttpResponse<String> response = sendGET( "kernels" );
        if ( response == null || response.statusCode() != 200 ) {
            return "ERROR";
        }
        updateKernels( gson.fromJson( response.body(), JsonArray.class ) );
        logResponse( response );
        return response.body();
    }


    public String getKernelspecs() {
        HttpResponse<String> response = sendGET( "kernelspecs" );
        if ( response == null || response.statusCode() != 200 ) {
            return "ERROR";
        }
        updateKernelSpecs( gson.fromJson( response.body(), JsonObject.class ) );
        logResponse( response );
        return response.body();
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
            return "ERROR";
        }
        JsonObject json = gson.fromJson( response.body(), JsonObject.class );
        updateSession( json );
        logResponse( response );
        return json.get( "id" ).getAsString();

    }


    private HttpResponse<String> sendGET( String resource ) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri( URI.create( URL + resource ) )
                    .timeout( Duration.of( 5, SECONDS ) )
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
                    .uri( URI.create( URL + resource ) )
                    .timeout( Duration.of( 5, SECONDS ) )
                    .POST( HttpRequest.BodyPublishers.ofString( body ) )
                    .header( "Authorization", "token " + token )
                    .build();
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException | InterruptedException e ) {
            log.warn( "Caught something in sendPOST: {}", e.getMessage() );
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


    private void logResponse( HttpResponse<String> response ) {
        log.warn( "Status: {}", response.statusCode() );
        log.warn( "Headers: {}", response.headers() );
        log.warn( "Body: {}", response.body() );
        log.warn( "as string: {}", response );
        log.warn( "URI: {}", response.uri() );
    }


    public void close() {
        log.warn( "closing jupyter client" );

    }


}
