/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.cql.server;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.cql.Cql2RelConverter;
import org.polypheny.db.cql.CqlQuery;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.cql.parser.ParseException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationKeyValue;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.sql.Kind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import spark.Request;
import spark.Response;
import spark.Service;


@Slf4j
public class HttpCqlInterface extends QueryInterface {

    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_NAME = "CQL Interface";
    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_DESCRIPTION = "HTTP-based query interface for the Contextual Query Language (CQL).";
    @SuppressWarnings("WeakerAccess")
    public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new QueryInterfaceSettingInteger( "port", false, true, false, 8087 )
    );

    private final Gson gson = new Gson();

    private final int port;
    private final String uniqueName;
    private final String databaseName = "APP";

    // Monitoring
    private final AtomicLong requestCounter = new AtomicLong();
    private final MonitoringPage monitoringPage;

    private Service server;


    public HttpCqlInterface( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings, false, false );
        this.uniqueName = uniqueName;
        this.port = Integer.parseInt( settings.get( "port" ) );
        if ( !Util.checkIfPortIsAvailable( port ) ) {
            // Port is already in use
            throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
        }
        // Add information page
        monitoringPage = new MonitoringPage();
    }


    private void routes( Service server ) {
        server.path( "/", () -> {
            server.before( "/*", ( q, a ) -> {
                log.debug( "Checking authentication of request with id: {}.", q.session().id() );
                try {
                    CatalogUser catalogUser = parseBasicAuthentication( q );
                } catch ( UnauthorizedAccessException e ) {
                    server.halt( 401, e.getMessage() );
                }
            } );
            server.post( "/", this::processRequest );
        } );
    }


    @Override
    public void run() {
        server = Service.ignite();
        server.port( port );
        routes( server );
        log.info( "{} started and is listening on port {}.", INTERFACE_NAME, port );
    }


    String processRequest( Request request, Response response ) {
        try {
            String cqlQueryStr = request.body();
            if ( cqlQueryStr.equals( "" ) ) {
                throw new RuntimeException( "CQL query is an empty string!" );
            }
            CqlParser cqlParser = new CqlParser( cqlQueryStr, databaseName );
            CqlQuery cqlQuery = cqlParser.parse();

            log.debug( "Starting to process CQL resource request. Session ID: {}.", request.session().id() );
            requestCounter.incrementAndGet();
            Transaction transaction = getTransaction();
            Statement statement = transaction.createStatement();
            RelBuilder relBuilder = RelBuilder.create( statement );
            JavaTypeFactory typeFactory = transaction.getTypeFactory();
            RexBuilder rexBuilder = new RexBuilder( typeFactory );

            Cql2RelConverter cql2RelConverter = new Cql2RelConverter( cqlQuery );

            RelRoot relRoot = cql2RelConverter.convert2Rel( relBuilder, rexBuilder );

            return executeAndTransformRelAlg( relRoot, statement, response );
        } catch ( ParseException e ) {
            log.error( "ParseException", e );
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "Exception", e.getMessage() );
            return gson.toJson( bodyReturn );
        } catch ( RuntimeException e ) {
            log.error( "RuntimeException", e );
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "Exception", e.getMessage() );
            return gson.toJson( bodyReturn );
        } catch ( Error e ) {
            log.error( "Error", e );
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "Exception", e.getMessage() );
            return gson.toJson( bodyReturn );
        }
    }


    public String executeAndTransformRelAlg( RelRoot relRoot, final Statement statement, final Response res ) {
        Result result;
        try {
            // Prepare
            PolyphenyDbSignature signature = statement.getQueryProcessor().prepareQuery( relRoot );
            log.debug( "RelRoot was prepared." );

            @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( statement.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            result = new Result( relRoot.kind, iterator, signature.rowType, signature.columns );
            result.transform();
            long executionTime = result.getExecutionTime();
            if ( !relRoot.kind.belongsTo( Kind.DML ) ) {
                signature.getExecutionTimeMonitor().setExecutionTime( executionTime );
            }

            statement.getTransaction().commit();
        } catch ( Throwable e ) {
            log.error( "Error during execution of CQL query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }

        return result.getResult( res );
    }


    private Transaction getTransaction() {
        try {
            String userName = "pa";
            return transactionManager.startTransaction( userName, databaseName, false, "CQL Interface", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    @Override
    public List<QueryInterfaceSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        server.stop();
        monitoringPage.remove();
        log.info( "{} stopped.", INTERFACE_NAME );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // There is no modifiable setting for this query interface
    }


    @Override
    public String getInterfaceType() {
        return INTERFACE_NAME;
    }


    /**
     * Parses and authenticates the Basic Authorization for a request.
     *
     * @param request the request
     * @return the authorized user
     * @throws UnauthorizedAccessException thrown if no authorization provided or invalid credentials
     */
    public CatalogUser parseBasicAuthentication( Request request ) throws UnauthorizedAccessException {
        if ( request.headers( "Authorization" ) == null ) {
            log.debug( "No Authorization header for request id: {}.", request.session().id() );
            throw new UnauthorizedAccessException( "No Basic Authorization sent by user." );
        }

        final String basicAuthHeader = request.headers( "Authorization" );

        final Pair<String, String> decoded = decodeBasicAuthorization( basicAuthHeader );

        try {
            return authenticator.authenticate( decoded.left, decoded.right );
        } catch ( AuthenticationException e ) {
            log.info( "Unable to authenticate user for request id: {}.", request.session().id(), e );
            throw new UnauthorizedAccessException( "Not authorized." );
        }
    }


    static Pair<String, String> decodeBasicAuthorization( String encodedAuthorization ) {
        if ( !Base64.isBase64( encodedAuthorization ) ) {
            throw new UnauthorizedAccessException( "Basic Authorization header is not properly encoded." );
        }
        final String encodedHeader = StringUtils.substringAfter( encodedAuthorization, "Basic" );
        final String decodedHeader = new String( Base64.decodeBase64( encodedHeader ) );
        final String[] decoded = StringUtils.splitPreserveAllTokens( decodedHeader, ":" );
        return new Pair<>( decoded[0], decoded[1] );
    }


    private class MonitoringPage {

        private final InformationPage informationPage;
        private final InformationGroup informationGroupRequests;
        private final InformationKeyValue informationNumberOfRequests;


        public MonitoringPage() {
            InformationManager im = InformationManager.getInstance();

            informationPage = new InformationPage( uniqueName, INTERFACE_NAME ).setLabel( "Interfaces" );
            informationGroupRequests = new InformationGroup( informationPage, "Number of requests" );

            im.addPage( informationPage );
            im.addGroup( informationGroupRequests );

            informationNumberOfRequests = new InformationKeyValue( informationGroupRequests );
            im.registerInformation( informationNumberOfRequests );

            informationGroupRequests.setRefreshFunction( this::update );
        }


        public void update() {
            long requestCount = requestCounter.get();
            informationNumberOfRequests.removePair( "total" );
            informationNumberOfRequests.putPair( "total", requestCount + "" );
        }


        public void remove() {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( informationNumberOfRequests );
            im.removeGroup( informationGroupRequests );
            im.removePage( informationPage );
        }

    }

}
