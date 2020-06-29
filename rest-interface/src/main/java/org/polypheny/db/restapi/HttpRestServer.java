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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.restapi.models.requests.InsertValueRequest;
import org.polypheny.db.restapi.models.requests.ResourceRequest;
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
            restServer.before( "/*", (q, a) -> {
                q.queryMap();
//                log.info( "Received api call." );

                log.debug( "Checking authentication of request with id: {}.", q.session().id() );
                boolean authenticated = this.checkBasicAuthentication( q.headers("Authorization") );
                if ( ! authenticated ) {
                    log.debug( "Unauthenticated request with id: {}. Blocking with 401.", q.session().id() );
                    restServer.halt( 401, "Not authorized.");
                }
            } );
            restServer.get( "/res", rest::getTableList, gson::toJson );
//            restServer.get( "/res", rest::getTableList, gson::toJson );
            restServer.get( "/res/:resName", (q, a) -> {
                ResourceRequest resourceRequest = requestParser.parseResourceRequest( q.params( ":resName" ), q.queryMap() );

                return rest.getResourceTable( resourceRequest, q, a );
            }, gson::toJson );
            restServer.post( "/res/:resName", (q, a) -> {
                InsertValueRequest insertValueRequest = requestParser.parseInsertValuePost( q.params(":resName"), q.queryMap(), q.body(), gson );

                return rest.postInsertValue( insertValueRequest, q, a );
            }, gson::toJson );
//            restServer.get( "/res/:resName", rest::getTable, gson::toJson );

//            restServer.path( "/res", () -> {
//                restServer.get( "/", rest::testMethod, gson::toJson );
//                restServer.get( "/:resName", rest::testMethod, gson::toJson );
//                restServer.post( "/:resName", null );
//            } );
        } );
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
