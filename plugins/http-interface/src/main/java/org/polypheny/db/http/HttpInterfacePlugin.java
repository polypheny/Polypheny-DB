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

package org.polypheny.db.http;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonSyntaxException;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.polypheny.db.StatusNotificationService;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.results.Result;

public class HttpInterfacePlugin extends PolyPlugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public HttpInterfacePlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        // Add HTTP interface
        Map<String, String> httpSettings = new HashMap<>();
        httpSettings.put( "port", "13137" );
        httpSettings.put( "maxUploadSizeMb", "10000" );
        QueryInterfaceManager.addInterfaceType( "http", HttpInterface.class, httpSettings );
    }


    @Override
    public void stop() {
        QueryInterfaceManager.removeInterfaceType( HttpInterface.class );
    }


    @Slf4j
    @Extension
    public static class HttpInterface extends QueryInterface {

        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_NAME = "HTTP Interface";
        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_DESCRIPTION = "HTTP-based query interface, which supports all available languages via specific routes.";
        @SuppressWarnings("WeakerAccess")
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingInteger( "port", false, true, false, 13137 ),
                new QueryInterfaceSettingInteger( "maxUploadSizeMb", false, true, true, 10000 )
        );

        private final ConcurrentHashMap<String, Set<String>> sessionXids = new ConcurrentHashMap<>();

        private final int port;
        private final String uniqueName;

        // Counters
        private final Map<QueryLanguage, AtomicLong> statementCounters = new HashMap<>();

        private final MonitoringPage monitoringPage;

        private static Javalin server;


        public HttpInterface( TransactionManager transactionManager, Authenticator authenticator, long ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
            this.uniqueName = uniqueName;
            this.port = Integer.parseInt( settings.get( "port" ) );
            if ( !Util.checkIfPortIsAvailable( port ) ) {
                // Port is already in use
                throw new GenericRuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
            }
            // Add information page
            monitoringPage = new MonitoringPage();
        }


        @Override
        public void run() {
            /*server = Javalin.create( config -> {  // todo dl enable, when we removed avatica and can finally bump javalin
                config.plugins.enableCors( cors -> cors.add( CorsPluginConfig::anyHost ) );
                config.staticFiles.add( "webapp" );
                config.jsonMapper( new JavalinJackson().updateMapper( mapper -> {
                    mapper.setSerializationInclusion( JsonInclude.Include.NON_NULL );
                } ) );
            } ).start( port );*/

            server = Javalin.create( config -> {
                config.jsonMapper( new JavalinJackson( new ObjectMapper() {
                    {
                        setSerializationInclusion( JsonInclude.Include.NON_NULL );
                        configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false );
                        configure( SerializationFeature.FAIL_ON_EMPTY_BEANS, false );
                        writerWithDefaultPrettyPrinter();
                    }
                } ) );
                config.enableCorsForAllOrigins();
                config.addStaticFiles( staticFileConfig -> staticFileConfig.directory = "webapp/" );
            } ).start( port );
            server.exception( Exception.class, ( e, ctx ) -> {
                log.warn( "Caught exception in the HTTP interface", e );
                if ( e instanceof JsonSyntaxException ) {
                    ctx.result( "Malformed request: " + e.getCause().getMessage() );
                } else {
                    ctx.result( "Error: " + e.getMessage() );
                }
            } );

            server.routes( () -> {
                StatusNotificationService.printInfo( String.format( "%s started and is listening on port %d.", INTERFACE_NAME, port ) );
            } );

            LanguageManager.getLanguages().forEach( this::addRoute );
        }


        public void addRoute( QueryLanguage language ) {
            for ( String route : language.otherNames() ) {
                log.info( "Added HTTP Route: /{}", route );
                server.post( route, ctx -> anyQuery( language, ctx ) );
            }
        }


        public void anyQuery( QueryLanguage language, final Context ctx ) {
            QueryRequest query = ctx.bodyAsClass( QueryRequest.class );
            String sessionId = ctx.req.getSession().getId();
            Crud.cleanupOldSession( sessionXids, sessionId );
            LogicalNamespace namespace = Catalog.snapshot().getNamespace( query.namespace ).orElse( null );

            List<? extends Result<?, ?>> results = LanguageCrud.anyQueryResult(
                    QueryContext.builder()
                            .query( query.query )
                            .language( language )
                            .userId( Catalog.defaultUserId )
                            .origin( "Http Interface" )
                            .transactionManager( transactionManager )
                            .namespaceId( namespace == null ? Catalog.defaultNamespaceId : namespace.id )
                            .build(), query );
            ctx.json( results.toArray( new Result[0] ) );

            if ( !statementCounters.containsKey( language ) ) {
                statementCounters.put( language, new AtomicLong() );
            }
            statementCounters.get( language ).incrementAndGet();
            // is empty from cleanupOldInfoAndFiles
            sessionXids.put( sessionId, results.stream().map( t -> t.xid ).filter( Objects::nonNull ).collect( Collectors.toSet() ) );
        }


        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return AVAILABLE_SETTINGS;
        }


        @Override
        public void shutdown() {
            server.stop();
            monitoringPage.remove();
        }


        @Override
        public String getInterfaceType() {
            return "Http Interface";
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {

        }


        @Override
        public void languageChange() {
            for ( QueryLanguage language : LanguageManager.getLanguages() ) {
                addRoute( language );
            }
        }


        private class MonitoringPage {

            private final InformationPage informationPage;
            private final InformationGroup informationGroupRequests;
            private final InformationTable statementsTable;


            public MonitoringPage() {
                InformationManager im = InformationManager.getInstance();

                informationPage = new InformationPage( uniqueName, INTERFACE_NAME ).fullWidth().setLabel( "Interfaces" );
                informationGroupRequests = new InformationGroup( informationPage, "Requests" );

                im.addPage( informationPage );
                im.addGroup( informationGroupRequests );

                statementsTable = new InformationTable(
                        informationGroupRequests,
                        Arrays.asList( "Language", "Percent", "Absolute" )
                );
                statementsTable.setOrder( 2 );
                im.registerInformation( statementsTable );

                informationGroupRequests.setRefreshFunction( this::update );
            }


            public void update() {
                double total = 0;
                for ( AtomicLong counter : statementCounters.values() ) {
                    total += counter.get();
                }

                DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
                symbols.setDecimalSeparator( '.' );
                DecimalFormat df = new DecimalFormat( "0.0", symbols );
                statementsTable.reset();
                for ( Map.Entry<QueryLanguage, AtomicLong> entry : statementCounters.entrySet() ) {
                    statementsTable.addRow( entry.getKey().serializedName(), df.format( total == 0 ? 0 : (entry.getValue().longValue() / total) * 100 ) + " %", entry.getValue().longValue() );
                }
            }


            public void remove() {
                InformationManager im = InformationManager.getInstance();
                im.removeInformation( statementsTable );
                im.removeGroup( informationGroupRequests );
                im.removePage( informationPage );
            }

        }

    }

}