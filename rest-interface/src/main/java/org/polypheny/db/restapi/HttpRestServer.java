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

package org.polypheny.db.restapi;


import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.restapi.exception.ParserException;
import org.polypheny.db.restapi.exception.RestException;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.restapi.models.requests.ResourceDeleteRequest;
import org.polypheny.db.restapi.models.requests.ResourceGetRequest;
import org.polypheny.db.restapi.models.requests.ResourcePatchRequest;
import org.polypheny.db.restapi.models.requests.ResourcePostRequest;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;
import spark.Request;
import spark.Response;
import spark.Service;


@Slf4j
public class HttpRestServer extends QueryInterface {

    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_NAME = "REST Interface";
    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_DESCRIPTION = "REST-based query interface.";
    @SuppressWarnings("WeakerAccess")
    public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new QueryInterfaceSettingInteger( "port", false, true, false, 8089 )
    );

    private final Gson gson = new Gson();

    private final RequestParser requestParser;
    private final int port;

    private Service restServer;


    public HttpRestServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings );
        this.requestParser = new RequestParser( transactionManager, authenticator, "pa", "APP" );
        this.port = Integer.parseInt( settings.get( "port" ) );
        if ( !Util.checkIfPortIsAvailable( port ) ) {
            // Port is already in use
            throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
        }
    }


    @Override
    public void run() {
        restServer = Service.ignite();
        restServer.port( port );

        Rest rest = new Rest( transactionManager, "pa", "APP" );
        restRoutes( restServer, rest );

        log.info( "REST API Server started." );
    }


    private void restRoutes( Service restServer, Rest rest ) {
        restServer.path( "/restapi/v1", () -> {
            restServer.before( "/*", ( q, a ) -> {
                log.debug( "Checking authentication of request with id: {}.", q.session().id() );
                try {
                    CatalogUser catalogUser = this.requestParser.parseBasicAuthentication( q );
                } catch ( UnauthorizedAccessException e ) {
                    restServer.halt( 401, e.getMessage() );
                }
            } );
            restServer.get( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.GET, q, a, q.params( ":resName" ) ), gson::toJson );
            restServer.post( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.POST, q, a, q.params( ":resName" ) ), gson::toJson );
            restServer.delete( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.DELETE, q, a, q.params( ":resName" ) ), gson::toJson );
            restServer.patch( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.PATCH, q, a, q.params( ":resName" ) ), gson::toJson );
        } );
    }


    private Map<String, Object> processResourceRequest( Rest rest, RequestType type, Request request, Response response, String resourceName ) {
        try {
            switch ( type ) {
                case DELETE:
                    ResourceDeleteRequest resourceDeleteRequest = requestParser.parseDeleteResourceRequest( request, resourceName );
                    return rest.processDeleteResource( resourceDeleteRequest, request, response );
                case GET:
                    ResourceGetRequest resourceGetRequest = requestParser.parseGetResourceRequest( request, resourceName );
                    return rest.processGetResource( resourceGetRequest, request, response );
                case PATCH:
                    ResourcePatchRequest resourcePatchRequest = requestParser.parsePatchResourceRequest( request, resourceName, gson );
                    return rest.processPatchResource( resourcePatchRequest, request, response );
                case POST:
                    ResourcePostRequest resourcePostRequest = requestParser.parsePostResourceRequest( request, resourceName, gson );
                    return rest.processPostResource( resourcePostRequest, request, response );
            }
        } catch ( ParserException e ) {
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "system", "parser" );
            bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
            bodyReturn.put( "error_code", e.getErrorCode().code );
            bodyReturn.put( "error", e.getErrorCode().name );
            bodyReturn.put( "error_description", e.getErrorCode().description );
            bodyReturn.put( "violating_input", e.getViolatingInput() );
            return bodyReturn;
        } catch ( RestException e ) {
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "system", "rest" );
            bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
            bodyReturn.put( "error_code", e.getErrorCode().code );
            bodyReturn.put( "error", e.getErrorCode().name );
            bodyReturn.put( "error_description", e.getErrorCode().description );
            return bodyReturn;
        }

        log.error( "processResourceRequest should never reach this point in the code!" );
        throw new RuntimeException( "processResourceRequest should never reach this point in the code!" );
    }


    @Override
    public List<QueryInterfaceSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        restServer.stop();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // There is no modifiable setting for this query interface
    }

    @Override
    public String getInterfaceType() {
        return INTERFACE_NAME;
    }
}
