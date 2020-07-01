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
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.restapi.RequestParser.Filters;
import org.polypheny.db.restapi.RequestParser.ProjectionAndAggregation;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.restapi.models.requests.RequestInfo;
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
                RequestInfo requestInfo = new RequestInfo();
                try {
                    CatalogUser catalogUser = this.requestParser.parseBasicAuthentication( q );
                    requestInfo.setAuthenticatedUser( catalogUser );
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
                RequestInfo requestInfo = new RequestInfo();
                requestInfo.setTables( requestParser.parseTables( q.params( ":resName" ) ) );
                Map<String, CatalogColumn> nameMapping = requestParser.generateNameMapping( requestInfo.getTables() );
                requestInfo.initialNameMapping( nameMapping );
                ProjectionAndAggregation projectionsAndAggregates = requestParser.parseProjectionsAndAggregations( q );
                requestInfo.setProjection( projectionsAndAggregates.projection );
                requestInfo.setAggregateFunctions( projectionsAndAggregates.aggregateFunctions );

                Map<String, CatalogColumn> nameAndAliasMapping = requestInfo.getNameAndAliasMapping();

                requestInfo.setGroupings( requestParser.parseGroupings( q, nameAndAliasMapping ) );

                requestInfo.setLimit( requestParser.parseLimit( q ) );
                requestInfo.setOffset( requestParser.parseOffset( q ) );
                requestInfo.setSort( requestParser.parseSorting( q, nameAndAliasMapping ) );

                Filters filters = requestParser.parseFilters( q, nameAndAliasMapping );
                requestInfo.setLiteralFilters( filters.literalFilters );
                requestInfo.setColumnFilters( filters.columnFilters );


                return rest.processGetResource( requestInfo, q, a );

//                ResourceRequest resourceRequest = requestParserOld.parseResourceRequest( q.params( ":resName" ), q.queryMap() );
//                return rest.getResourceTable( resourceRequest, q, a );
            }, gson::toJson );
            restServer.post( "/res/:resName", (q, a) -> {
                RequestInfo requestInfo = new RequestInfo();
                requestInfo.setTables( requestParser.parseTables( q.params( ":resName" ) ) );
                Map<String, CatalogColumn> nameMapping = requestParser.generateNameMapping( requestInfo.getTables() );
                requestInfo.initialNameMapping( nameMapping );
                Map<String, CatalogColumn> nameAndAliasMapping = requestInfo.getNameAndAliasMapping();
                requestInfo.setValues( requestParser.parseValues( q, nameAndAliasMapping, gson ) );
                return rest.processPutResource( requestInfo, q, a );

//                InsertValueRequest insertValueRequest = requestParserOld.parseInsertValuePost( q.params(":resName"), q.queryMap(), q.body(), gson );
//                return rest.postInsertValue( insertValueRequest, q, a );
            }, gson::toJson );
            restServer.delete( "/res/:resName", (q, a) -> {
                RequestInfo requestInfo = new RequestInfo();
                requestInfo.setTables( requestParser.parseTables( q.params( ":resName" ) ) );
                Map<String, CatalogColumn> nameMapping = requestParser.generateNameMapping( requestInfo.getTables() );
                requestInfo.initialNameMapping( nameMapping );
                Map<String, CatalogColumn> nameAndAliasMapping = requestInfo.getNameAndAliasMapping();
                Filters filters = requestParser.parseFilters( q, nameAndAliasMapping );
                requestInfo.setLiteralFilters( filters.literalFilters );
                requestInfo.setColumnFilters( filters.columnFilters );

                return rest.processDeleteResource( requestInfo, q, a );
//                DeleteValueRequest deleteValueRequest = requestParserOld.parseDeleteValueRequest( q.params(":resName"), q.queryMap() );
//                return rest.deleteValues( deleteValueRequest, q, a );
            }, gson::toJson );
            restServer.patch( "/res/:resName", (q, a) -> {
                RequestInfo requestInfo = new RequestInfo();
                requestInfo.setTables( requestParser.parseTables( q.params( ":resName" ) ) );
                Map<String, CatalogColumn> nameMapping = requestParser.generateNameMapping( requestInfo.getTables() );
                requestInfo.initialNameMapping( nameMapping );
                Map<String, CatalogColumn> nameAndAliasMapping = requestInfo.getNameAndAliasMapping();
                Filters filters = requestParser.parseFilters( q, nameAndAliasMapping );
                requestInfo.setLiteralFilters( filters.literalFilters );
                requestInfo.setColumnFilters( filters.columnFilters );
                requestInfo.setValues( requestParser.parseValues( q, nameAndAliasMapping, gson ) );

                return rest.processPatchResource( requestInfo, q, a );
//                UpdateResourceRequest updateResourceRequest = requestParserOld.parseUpdateResourceRequest( q.params(":resName"), q.queryMap(), q.body(), gson );
//                return rest.updateResource( updateResourceRequest, q, a );
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
