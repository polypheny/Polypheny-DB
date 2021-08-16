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

package org.polypheny.db.restapi;


import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.cql.CqlQuery;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.cql.parser.ParseException;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
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
            new QueryInterfaceSettingInteger( "port", false, true, false, 8089 ),
            new QueryInterfaceSettingInteger( "maxUploadSizeMb", false, true, true, 10000 )
    );

    private final Gson gson = new Gson();

    private final RequestParser requestParser;
    private final int port;
    private final String uniqueName;
    private final String databaseName = "APP";

    // Counter
    private final AtomicLong deleteCounter = new AtomicLong();
    private final AtomicLong getCounter = new AtomicLong();
    private final AtomicLong patchCounter = new AtomicLong();
    private final AtomicLong postCounter = new AtomicLong();

    private final MonitoringPage monitoringPage;

    private Service restServer;


    public HttpRestServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
        this.requestParser = new RequestParser( transactionManager, authenticator, "pa", "APP" );
        this.uniqueName = uniqueName;
        this.port = Integer.parseInt( settings.get( "port" ) );
        if ( !Util.checkIfPortIsAvailable( port ) ) {
            // Port is already in use
            throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
        }
        // Add information page
        monitoringPage = new MonitoringPage();
    }


    @Override
    public void run() {
        restServer = Service.ignite();
        restServer.port( port );

        Rest rest = new Rest( transactionManager, "pa", databaseName );
        restRoutes( restServer, rest );

        log.info( "{} started and is listening on port {}.", INTERFACE_NAME, port );
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
            restServer.get( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.GET, q, a, q.params( ":resName" ) ) );
            restServer.post( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.POST, q, a, q.params( ":resName" ) ) );
            restServer.delete( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.DELETE, q, a, q.params( ":resName" ) ) );
            restServer.patch( "/res/:resName", ( q, a ) -> this.processResourceRequest( rest, RequestType.PATCH, q, a, q.params( ":resName" ) ) );
            restServer.post( "/multipart", "multipart/form-data", ( q, a ) -> this.processMultipart( rest, RequestType.POST, q, a ), gson::toJson );
            restServer.get( "/cql", ( q, a ) -> this.processCQLRequest( rest, RequestType.GET, q, a ) );
        } );
    }


    String processCQLRequest( Rest rest, RequestType type, Request request, Response response ) {
        try {
            String cqlQueryStr = request.body();
            if ( request.queryParams( "testing" ) != null ) {
                cqlQueryStr = request.queryParams( "cql" );
            }
            CqlParser cqlParser = new CqlParser( cqlQueryStr, databaseName );
            CqlQuery cqlQuery = cqlParser.parse();

            return rest.processCqlResource( cqlQuery, request, response );
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


    String processResourceRequest( Rest rest, RequestType type, Request request, Response response, String resourceName ) {
        try {
            switch ( type ) {
                case DELETE:
                    deleteCounter.incrementAndGet();
                    ResourceDeleteRequest resourceDeleteRequest = requestParser.parseDeleteResourceRequest( request, resourceName );
                    return rest.processDeleteResource( resourceDeleteRequest, request, response );
                case GET:
                    getCounter.incrementAndGet();
                    ResourceGetRequest resourceGetRequest = requestParser.parseGetResourceRequest( request, resourceName );
                    return rest.processGetResource( resourceGetRequest, request, response );
                case PATCH:
                    patchCounter.incrementAndGet();
                    ResourcePatchRequest resourcePatchRequest = requestParser.parsePatchResourceRequest( request, resourceName, gson );
                    return rest.processPatchResource( resourcePatchRequest, request, response, null );
                case POST:
                    postCounter.incrementAndGet();
                    ResourcePostRequest resourcePostRequest = requestParser.parsePostResourceRequest( request, resourceName, gson );
                    return rest.processPostResource( resourcePostRequest, request, response, null );
            }
        } catch ( ParserException e ) {
            log.error( "ParserException", e );
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "system", "parser" );
            bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
            bodyReturn.put( "error_code", e.getErrorCode().code );
            bodyReturn.put( "error", e.getErrorCode().name );
            bodyReturn.put( "error_description", e.getErrorCode().description );
            bodyReturn.put( "violating_input", e.getViolatingInput() );
            return gson.toJson( bodyReturn );
        } catch ( RestException e ) {
            log.error( "RestException", e );
            response.status( 400 );
            Map<String, Object> bodyReturn = new HashMap<>();
            bodyReturn.put( "system", "rest" );
            bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
            bodyReturn.put( "error_code", e.getErrorCode().code );
            bodyReturn.put( "error", e.getErrorCode().name );
            bodyReturn.put( "error_description", e.getErrorCode().description );
            return gson.toJson( bodyReturn );
        } catch ( Throwable t ) {
            log.error( "Rest error", t );
            throw t;
        }

        log.error( "processResourceRequest should never reach this point in the code!" );
        throw new RuntimeException( "processResourceRequest should never reach this point in the code!" );
    }


    /**
     * Initialize a multipart request, so that the values can be fetched with request.raw().getPart( name )
     *
     * @param req Spark request
     */
    private void initMultipart( Request req ) {
        //see https://stackoverflow.com/questions/34746900/sparkjava-upload-file-didt-work-in-spark-java-framework
        String location = System.getProperty( "java.io.tmpdir" + File.separator + "Polypheny-DB" );
        long maxSizeMB = Long.parseLong( settings.get( "maxUploadSizeMb" ) );
        long maxFileSize = 1_000_000L * maxSizeMB;
        long maxRequestSize = 1_000_000L * maxSizeMB;
        int fileSizeThreshold = 1024;
        MultipartConfigElement multipartConfigElement = new MultipartConfigElement( location, maxFileSize, maxRequestSize, fileSizeThreshold );
        req.raw().setAttribute( "org.eclipse.jetty.multipartConfig", multipartConfigElement );
    }


    private String rawPartToString( Part part ) throws IOException, ServletException {
        return new BufferedReader( new InputStreamReader( part.getInputStream(), StandardCharsets.UTF_8 ) ).lines().collect( Collectors.joining( System.lineSeparator() ) );
    }


    String processMultipart( Rest rest, RequestType type, Request req, Response res ) {
        Gson gson = new Gson();
        initMultipart( req );

        Map<String, String> params = new HashMap<>();
        Map<String, InputStream> inputStreams = new HashMap<>();
        try {

            for ( Part part : req.raw().getParts() ) {
                if ( part.getSubmittedFileName() != null ) {
                    inputStreams.put( part.getName(), part.getInputStream() );
                } else {
                    params.put( part.getName(), rawPartToString( part ) );
                }
            }

        } catch ( Throwable t ) {
            throw new RuntimeException( "Could not process multipart request", t );
        }

        switch ( type ) {
            case POST:
                String resName = params.get( "resName" );
                String[] projections = params.get( "_project" ) == null ? null : gson.fromJson( params.get( "_project" ), String[].class );
                List<Object> insertValues = params.get( "data" ) == null ? null : gson.fromJson( params.get( "data" ), List.class );
                Map<String, String[]> filterMap = new HashMap<>();
                params.forEach( ( k, v ) -> {
                    if ( !k.startsWith( "_" ) && !k.equals( "data" ) && !k.equals( "resName" ) ) {
                        String[] filters;
                        try {
                            filters = gson.fromJson( v, String[].class );
                        } catch ( Throwable t ) {
                            filters = new String[]{ v };
                        }
                        filterMap.put( k, filters );
                    }
                } );
                try {
                    ResourcePostRequest resourcePatchRequest = requestParser.parsePostMultipartRequest( resName, projections, insertValues );
                    resourcePatchRequest.useDynamicParams = true;
                    return rest.processPostResource( resourcePatchRequest, null, null, inputStreams );
                } catch ( ParserException e ) {
                    log.error( "ParserException", e );
                    res.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "parser" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    bodyReturn.put( "violating_input", e.getViolatingInput() );
                    return gson.toJson( bodyReturn );
                } catch ( RestException e ) {
                    log.error( "RestException", e );
                    res.status( 400 );
                    Map<String, Object> bodyReturn = new HashMap<>();
                    bodyReturn.put( "system", "rest" );
                    bodyReturn.put( "subsystem", e.getErrorCode().subsystem );
                    bodyReturn.put( "error_code", e.getErrorCode().code );
                    bodyReturn.put( "error", e.getErrorCode().name );
                    bodyReturn.put( "error_description", e.getErrorCode().description );
                    return gson.toJson( bodyReturn );
                } catch ( Throwable t ) {
                    log.error( "Rest multipart error", t );
                    throw t;
                } finally {
                    try {
                        inputStreams.clear();
                        for ( Part part : req.raw().getParts() ) {
                            part.delete();
                        }
                    } catch ( ServletException | IOException e ) {
                        log.error( "Could not delete temporary files", e );
                    }
                }
        }
        log.error( "processMultipart should never reach this point in the code!" );
        throw new RuntimeException( "processMultipart should never reach this point in the code!" );
    }


    @Override
    public List<QueryInterfaceSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        restServer.stop();
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


    private class MonitoringPage {

        private final InformationPage informationPage;
        private final InformationGroup informationGroupRequests;
        private final InformationGraph counterGraph;
        private final InformationTable counterTable;


        public MonitoringPage() {
            InformationManager im = InformationManager.getInstance();

            informationPage = new InformationPage( uniqueName, INTERFACE_NAME ).fullWidth().setLabel( "Interfaces" );
            informationGroupRequests = new InformationGroup( informationPage, "Requests" );

            im.addPage( informationPage );
            im.addGroup( informationGroupRequests );

            counterGraph = new InformationGraph(
                    informationGroupRequests,
                    GraphType.DOUGHNUT,
                    new String[]{ "DELETE", "GET", "PATCH", "POST" }
            );
            counterGraph.setOrder( 1 );
            im.registerInformation( counterGraph );

            counterTable = new InformationTable(
                    informationGroupRequests,
                    Arrays.asList( "Type", "Percent", "Absolute" )
            );
            counterTable.setOrder( 2 );
            im.registerInformation( counterTable );

            informationGroupRequests.setRefreshFunction( this::update );
        }


        public void update() {
            long deleteCount = deleteCounter.get();
            long getCount = getCounter.get();
            long patchCount = patchCounter.get();
            long postCount = postCounter.get();
            double total = deleteCount + getCount + patchCount + postCount;

            counterGraph.updateGraph(
                    new String[]{ "DELETE", "GET", "PATCH", "POST" },
                    new GraphData<>( "requests", new Long[]{ deleteCount, getCount, patchCount, postCount } )
            );

            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
            symbols.setDecimalSeparator( '.' );
            DecimalFormat df = new DecimalFormat( "0.0", symbols );
            counterTable.reset();
            counterTable.addRow( "DELETE", df.format( total == 0 ? 0 : (deleteCount / total) * 100 ) + " %", deleteCount );
            counterTable.addRow( "GET", df.format( total == 0 ? 0 : (getCount / total) * 100 ) + " %", getCount );
            counterTable.addRow( "PATCH", df.format( total == 0 ? 0 : (patchCount / total) * 100 ) + " %", patchCount );
            counterTable.addRow( "POST", df.format( total == 0 ? 0 : (postCount / total) * 100 ) + " %", postCount );
        }


        public void remove() {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( counterGraph, counterTable );
            im.removeGroup( informationGroupRequests );
            im.removePage( informationPage );
        }

    }

}
