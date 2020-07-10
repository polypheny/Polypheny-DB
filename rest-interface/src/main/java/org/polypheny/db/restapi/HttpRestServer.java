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


import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.restapi.exception.ParserException;
import org.polypheny.db.restapi.exception.RestException;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.restapi.models.requests.DeleteValueRequest;
import org.polypheny.db.restapi.models.requests.GetResourceRequest;
import org.polypheny.db.restapi.models.requests.InsertValueRequest;
import org.polypheny.db.restapi.models.requests.UpdateResourceRequest;
import org.polypheny.db.transaction.TransactionManager;
import spark.Service;


@Slf4j
public class HttpRestServer extends QueryInterface {

    private final Gson gson = new Gson();

    private final int port;

    private final RequestParser requestParser;

    public HttpRestServer( TransactionManager transactionManager, Authenticator authenticator, final int port ) {
        super( transactionManager, authenticator );
        this.port = port;
        this.requestParser = new RequestParser( transactionManager, authenticator, "pa", "APP" );
    }


    @Override
    public void run() {

        Service restServer = Service.ignite();
        restServer.port(this.port);

        Rest rest = new Rest( transactionManager, "pa", "APP" );
        restRoutes( restServer, rest );

        log.info( "REST API Server started." );
    }


    private void restRoutes( Service restServer, Rest rest ) {
        restServer.path( "/restapi/v1", () -> {
//            RequestInfo requestInfo;
            restServer.before( "/*", (q, a) -> {
                log.debug( "Checking authentication of request with id: {}.", q.session().id() );
//                RequestInfo requestInfo = new RequestInfo();
                try {
                    CatalogUser catalogUser = this.requestParser.parseBasicAuthentication( q );
//                    requestInfo.setAuthenticatedUser( catalogUser );
                } catch ( UnauthorizedAccessException e ) {
                    restServer.halt( 401, e.getMessage() );
                }
//                boolean authenticated = this.checkBasicAuthentication( q.headers("Authorization") );
//                CatalogUser catalogUser = this.basicAuthentication( q.headers("Authorization") );
//                if ( catalogUser == null ) {
//                    log.debug( "Unauthenticated request with id: {}. Blocking with 401.", q.session().id() );
//                    restServer.halt( 401, "Not authorized.");
//                }
//                requestInfo.setAuthenticatedUser( catalogUser );
            } );
//            restServer.get( "/res", restOld::getTableList, gson::toJson );
            restServer.get( "/res/:resName", (q, a) -> {
                try {
                    GetResourceRequest getResourceRequest = requestParser.parseGetResourceRequest( q, q.params( ":resName" ) );
                    return rest.processGetResource( getResourceRequest, q, a );
                } catch ( ParserException e ) {
                    a.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "parser" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    bodyReturn.put( "violating_input", e.getViolatingInput() );
                    return bodyReturn;
                } catch ( RestException e ) {
                    a.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "rest" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    return bodyReturn;
                } catch ( Exception e ) {
                    a.status( 500 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "wtf", "something's fishy" );
                    bodyReturn.put( "exception", e.getLocalizedMessage() );
                    bodyReturn.put( "stacktrace", e.getStackTrace() );
                    return bodyReturn;
                }
            }, gson::toJson );
            restServer.post( "/res/:resName", (q, a) -> {
                try {
                    InsertValueRequest insertValueRequest = requestParser.parsePutResourceRequest( q, q.params( ":resName" ), gson );
                    return rest.processPostResource( insertValueRequest, q, a );
                } catch ( ParserException e ) {
                    a.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "parser" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    bodyReturn.put( "violating_input", e.getViolatingInput() );
                    return bodyReturn;
                } catch ( RestException e ) {
                    a.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "rest" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    return bodyReturn;
                }
            }, gson::toJson );
            restServer.delete( "/res/:resName", (q, a) -> {
               try {
                   DeleteValueRequest deleteValueRequest = requestParser.parseDeleteResourceRequest( q, q.params( ":resName" ) );
                   return rest.processDeleteResource( deleteValueRequest, q, a );
               } catch ( ParserException e ) {
                   a.status( 400 );
                   Map<String, Object> bodyReturn = new HashMap<>();
                   bodyReturn.put( "system", "parser" );
                   bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                   bodyReturn.put( "error_code", e.getErrorCode().code );
                   bodyReturn.put( "error", e.getErrorCode().name );
                   bodyReturn.put( "error_description", e.getErrorCode().description );
                   bodyReturn.put( "violating_input", e.getViolatingInput() );
                   return bodyReturn;
               } catch ( RestException e ) {
                   a.status( 400 );
                   Map<String, Object> bodyReturn = new HashMap<>();
                   bodyReturn.put( "system", "rest" );
                   bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                   bodyReturn.put( "error_code", e.getErrorCode().code );
                   bodyReturn.put( "error", e.getErrorCode().name );
                   bodyReturn.put( "error_description", e.getErrorCode().description );
                   return bodyReturn;
               }
            }, gson::toJson );
            restServer.patch( "/res/:resName", (q, a) -> {
                try {
                    UpdateResourceRequest updateResourceRequest = requestParser.parsePatchResourceRequest( q, q.params( ":resName" ), gson );
                    return rest.processPatchResource( updateResourceRequest, q, a );
                } catch ( ParserException e ) {
                    a.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "parser" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    bodyReturn.put( "violating_input", e.getViolatingInput() );
                    return bodyReturn;
                } catch ( RestException e ) {
                    a.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "rest" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    return bodyReturn;
                }
            }, gson::toJson );
        } );
    }


    private CatalogUser basicAuthentication( String basicAuthHeader ) {
        if ( basicAuthHeader == null ) {
            return null;
        }

        String[] decoded = this.decodeAuthorizationHeader( basicAuthHeader );

        try {
            return this.authenticator.authenticate( decoded[0], decoded[1] );
        } catch ( AuthenticationException e ) {
//            e.printStackTrace();
            return null;
        }
    }

    private boolean checkBasicAuthentication( String basicAuthHeader ) {
        if ( basicAuthHeader == null ) {
            return false;
        }

        String[] decoded = this.decodeAuthorizationHeader( basicAuthHeader );

        try {
            this.authenticator.authenticate( decoded[0], decoded[1] );
            return true;
        } catch ( AuthenticationException e ) {
//            e.printStackTrace();
            return false;
        }

    }


    private String[] decodeAuthorizationHeader( String header ) {
        final String encodedHeader = StringUtils.substringAfter( header, "Basic" );
        final String decodedHeader = new String( Base64.decodeBase64( encodedHeader ) );
        return StringUtils.splitPreserveAllTokens( decodedHeader, ":" );
    }
}
