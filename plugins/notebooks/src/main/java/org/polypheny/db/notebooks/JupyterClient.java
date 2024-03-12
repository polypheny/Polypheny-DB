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

package org.polypheny.db.notebooks;

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
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpStatus.Code;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.notebooks.model.JupyterSessionManager;

@Slf4j
public class JupyterClient {

    private final String host;
    private final HttpClient client;
    private final WebSocket.Builder webSocketBuilder;
    private final String token;
    private final Gson gson = new Gson();
    private final JupyterSessionManager sessionManager = JupyterSessionManager.getInstance();


    /**
     * Create a new JupyterClient instance for sending REST requests to the jupyter server,
     * adhering to the <a href="https://jupyter-server.readthedocs.io/en/latest/developers/rest-api.html">API</a>.
     *
     * @param token the token needed for authentication with the server
     * @param host the address of the docker container
     * @param port the port of the server
     */
    public JupyterClient( String token, String host, int port ) {
        this.token = token;
        this.host = host + ":" + port;
        this.client = HttpClient.newBuilder()
                .cookieHandler( new CookieManager() )
                .build();
        this.webSocketBuilder = client.newWebSocketBuilder();
    }


    /**
     * Can be used to test the connection to the jupyter server.
     *
     * @return true if the server can be reached, false otherwise
     */
    public boolean testConnection() {
        try {
            HttpResponse<String> response = sendGET( "status" );
            return response.statusCode() == 200;
        } catch ( JupyterServerException e ) {
            return false;
        }
    }


    /**
     * Get the current status/activity of the server.
     *
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> getStatus() throws JupyterServerException {
        return sendGET( "status" );
    }


    /**
     * Get information about a specific session and its associated kernel.
     *
     * @param sessionId the ID of the requested session
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
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


    /**
     * Get a list of all running sessions with information about each session and its associated kernel.
     *
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> getSessions() throws JupyterServerException {
        HttpResponse<String> response = sendGET( "sessions" );
        if ( response.statusCode() == 200 ) {
            updateSessions( gson.fromJson( response.body(), JsonArray.class ) );
        }
        return response;
    }


    /**
     * Create a new session, or return an existing session if a session of the same name already exists.
     *
     * @param body serialized JSON object as specified by the jupyter server API
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> createSession( String body ) throws JupyterServerException {
        HttpResponse<String> response = sendPOST( "sessions", body );
        if ( response.statusCode() == 201 ) {
            JsonObject json = gson.fromJson( response.body(), JsonObject.class );
            updateSession( json );
        }
        return response;
    }


    /**
     * Create a new session, or return an existing session if a session of the same name already exists.
     *
     * @param kernelName the unique name of the kerne to be used
     * @param fileName name of the session
     * @param filePath path to the session
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
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


    /**
     * Can be used to change any information of the given session
     *
     * @param sessionId ID of the session to be changed
     * @param body serialized JSON object as specified by the jupyter server API
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> patchSession( String sessionId, String body ) throws JupyterServerException {
        HttpResponse<String> response = sendPATCH( "sessions/" + sessionId, body );
        if ( response.statusCode() == 200 ) {
            updateSession( gson.fromJson( response.body(), JsonObject.class ) );
        }
        return response;
    }


    /**
     * Delete the specified session.
     *
     * @param sessionId ID of the session to be deleted
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> deleteSession( String sessionId ) throws JupyterServerException {
        HttpResponse<String> response = sendDELETE( "sessions/" + sessionId );
        if ( response.statusCode() == 204 ) {
            sessionManager.invalidateSession( sessionId );
        }
        return response;
    }


    /**
     * Return a list containing information about each running kernel.
     *
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> getRunningKernels() throws JupyterServerException {
        HttpResponse<String> response = sendGET( "kernels" );
        if ( response.statusCode() == 200 ) {
            updateKernels( gson.fromJson( response.body(), JsonArray.class ) );
        }
        return response;
    }


    /**
     * Return a list containing the specifications of each available (installed) kernel.
     *
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> getKernelspecs() throws JupyterServerException {
        HttpResponse<String> response = sendGET( "kernelspecs" );
        if ( response.statusCode() == 200 ) {
            updateKernelSpecs( gson.fromJson( response.body(), JsonObject.class ) );
        }
        return response;
    }


    /**
     * Interrupt the specified kernel.
     *
     * @param kernelId the ID of a running kernel to be interrupted
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> interruptKernel( String kernelId ) throws JupyterServerException {
        return sendPOST( "kernels/" + kernelId + "/interrupt", "" );
    }


    /**
     * Restart the specified kernel.
     *
     * @param kernelId the ID of a running kernel to be restarted
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    public HttpResponse<String> restartKernel( String kernelId ) throws JupyterServerException {
        return sendPOST( "kernels/" + kernelId + "/restart", "" );
    }


    /**
     * Send a GET request to the specified endpoint.
     *
     * @param resource the endpoint the GET request is sent to
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    private HttpResponse<String> sendGET( String resource ) throws JupyterServerException {
        return sendGET( resource, null );
    }


    /**
     * Send a GET request to the specified endpoint.
     *
     * @param resource the endpoint the GET request is sent to
     * @param queryParams query parameters as string, correctly formatted {@code (key1=value1&key2=value2...)}
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    private HttpResponse<String> sendGET( String resource, String queryParams ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource, queryParams ) )
                .timeout( Duration.of( 10, ChronoUnit.SECONDS ) )
                .GET()
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "GET failed: Jupyter Server is unavailable", HttpStatus.SERVICE_UNAVAILABLE_503 );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "GET failed: Thread was interrupted", HttpStatus.SERVICE_UNAVAILABLE_503 );
        }
    }


    /**
     * Send a POST request to the specified endpoint.
     *
     * @param resource the endpoint the request is sent to
     * @param body the body of the request
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    private HttpResponse<String> sendPOST( String resource, String body ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, ChronoUnit.SECONDS ) )
                .POST( HttpRequest.BodyPublishers.ofString( body ) )
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "POST failed: Jupyter Server is unavailable", HttpStatus.SERVICE_UNAVAILABLE_503 );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "POST failed: Thread was interrupted", HttpStatus.SERVICE_UNAVAILABLE_503 );
        }
    }


    /**
     * Send a PUT request to the specified endpoint.
     *
     * @param resource the endpoint the request is sent to
     * @param body the body of the request
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    private HttpResponse<String> sendPUT( String resource, String body ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, ChronoUnit.SECONDS ) )
                .PUT( HttpRequest.BodyPublishers.ofString( body ) )
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "PUT failed: Jupyter Server is unavailable", HttpStatus.SERVICE_UNAVAILABLE_503 );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "PUT failed: Thread was interrupted", HttpStatus.SERVICE_UNAVAILABLE_503 );
        }
    }


    /**
     * Send a PATCH request to the specified endpoint.
     *
     * @param resource the endpoint the request is sent to
     * @param body the body of the request
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    private HttpResponse<String> sendPATCH( String resource, String body ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, ChronoUnit.SECONDS ) )
                .method( "PATCH", HttpRequest.BodyPublishers.ofString( body ) )
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "PATCH failed: Jupyter Server is unavailable", HttpStatus.SERVICE_UNAVAILABLE_503 );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "PATCH failed: Thread was interrupted", HttpStatus.SERVICE_UNAVAILABLE_503 );
        }
    }


    /**
     * Send a DELETE request to the specified endpoint.
     *
     * @param resource the endpoint the request is sent to
     * @return the HttpResponse from the jupyter server
     * @throws JupyterServerException if the request fails
     */
    private HttpResponse<String> sendDELETE( String resource ) throws JupyterServerException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri( getUriFromPath( resource ) )
                .timeout( Duration.of( 10, ChronoUnit.SECONDS ) )
                .DELETE()
                .header( "Authorization", "token " + token )
                .build();
        try {
            return client.send( request, BodyHandlers.ofString() );
        } catch ( IOException e ) {
            throw new JupyterServerException( "DELETE failed: Jupyter Server is unavailable", HttpStatus.SERVICE_UNAVAILABLE_503 );
        } catch ( InterruptedException e ) {
            throw new JupyterServerException( "DELETE failed: Thread was interrupted", HttpStatus.SERVICE_UNAVAILABLE_503 );
        }
    }


    /**
     * Updates the sessions stored in the JupyterSessionManager according to the specified JsonArray.
     * Existing session that do not occur in sessions are removed.
     *
     * @param sessions the JsonArray containing the session information in the same format as returned by GET sessions
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


    /**
     * Updates the corresponding session stored in the JupyterSessionManager according to the specified JsonObject.
     *
     * @param session the JsonObject containing the session information in the same format as returned by GET session
     */
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


    /**
     * Updates the kernels stored in the JupyterSessionManager according to the specified JsonArray.
     * Existing kernels that do not occur in kernels are removed.
     *
     * @param kernels the JsonArray containing the kernel information in the same format as returned by GET kernels
     */
    private void updateKernels( JsonArray kernels ) {
        Set<String> validKernelIds = new HashSet<>();
        for ( JsonElement kernelElem : kernels ) {
            JsonObject kernel = kernelElem.getAsJsonObject();
            String id = kernel.get( "id" ).getAsString();
            String name = kernel.get( "name" ).getAsString();
            sessionManager.addKernel( id, name, webSocketBuilder, host );
            validKernelIds.add( id );
        }
        sessionManager.retainValidKernels( validKernelIds );
    }


    /**
     * Updates the kernel specs stored in the JupyterSessionManager according to the specified JsonObject.
     *
     * @param kernelSpecs the JsonObject containing the kernel information in the same format as returned by GET kernelspecs
     */
    private void updateKernelSpecs( JsonObject kernelSpecs ) {
        sessionManager.setDefaultKernel( kernelSpecs.get( "default" ).getAsString() );
        for ( Entry<String, JsonElement> entry : kernelSpecs.get( "kernelspecs" ).getAsJsonObject().entrySet() ) {
            JsonObject spec = entry.getValue().getAsJsonObject().get( "spec" ).getAsJsonObject();
            sessionManager.addKernelSpec( entry.getKey(),
                    spec.get( "display_name" ).getAsString(),
                    spec.get( "language" ).getAsString() );
        }

    }


    /**
     * Returns a URI object from the given endpoint.
     *
     * @param path a jupyter server endpoint without the "/api/" prefix
     * @return the URI for the request
     * @throws RuntimeException if no valid URI can be built from the given endpoint.
     */
    private URI getUriFromPath( String path ) {
        return getUriFromPath( path, null );
    }


    /**
     * Returns a URI object from the given endpoint and query parameters.
     *
     * @param path a jupyter server endpoint without the "/api/" prefix
     * @param queryParams the String containing the query parameters, or null if none are present.
     * @return the URI for the request
     * @throws RuntimeException if no valid URI can be built from the given endpoint and params.
     */
    private URI getUriFromPath( String path, String queryParams ) {
        try {
            return new URI( "http", host, "/api/" + path, queryParams, null );
        } catch ( URISyntaxException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    /**
     * A class representing any exception that might occur when a request is sent to the jupyter server.
     */
    @Getter
    public static class JupyterServerException extends Exception {

        private final String msg;
        private final Code status;


        public JupyterServerException( String msg, Code status ) {
            this.msg = msg;
            this.status = Objects.requireNonNullElse( status, Code.INTERNAL_SERVER_ERROR );
        }


        public JupyterServerException( String msg, int status ) {
            this.msg = msg;
            this.status = Objects.requireNonNullElse( HttpStatus.getCode( status ), Code.INTERNAL_SERVER_ERROR );
        }

    }


}
