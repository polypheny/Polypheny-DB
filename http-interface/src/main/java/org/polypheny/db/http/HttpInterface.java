/*
 * Copyright 2019-2022 The Polypheny Project
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


import static io.javalin.apibuilder.ApiBuilder.post;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JsonMapper;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.StatusService;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.requests.QueryRequest;


@Slf4j
public class HttpInterface extends QueryInterface {

    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_NAME = "HTTP Interface";
    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_DESCRIPTION = "HTTP-based query interface, which supports all available languages via specific routes.";
    @SuppressWarnings("WeakerAccess")
    public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new QueryInterfaceSettingInteger( "port", false, true, false, 13137 ),
            new QueryInterfaceSettingInteger( "maxUploadSizeMb", false, true, true, 10000 )
    );

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter( Result.class, Result.getSerializer() )
            .create();

    private final int port;
    private final String uniqueName;

    // Counters
    private final Map<QueryLanguage, AtomicLong> statementCounters = new HashMap<>();

    private final MonitoringPage monitoringPage;

    private Javalin restServer;


    public HttpInterface( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
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
        JsonMapper gsonMapper = new JsonMapper() {
            @NotNull
            @Override
            public String toJsonString( @NotNull Object obj ) {
                return gson.toJson( obj );
            }


            @NotNull
            @Override
            public <T> T fromJsonString( @NotNull String json, @NotNull Class<T> targetClass ) {
                return gson.fromJson( json, targetClass );
            }
        };
        restServer = Javalin.create( config -> {
            config.jsonMapper( gsonMapper );
            config.enableCorsForAllOrigins();
        } ).start( port );
        restServer.exception( Exception.class, ( e, ctx ) -> {
            log.warn( "Caught exception in the HTTP interface", e );
            if ( e instanceof JsonSyntaxException ) {
                ctx.result( "Malformed request: " + e.getCause().getMessage() );
            } else {
                ctx.result( "Error: " + e.getMessage() );
            }
        } );

        restServer.routes( () -> {
            post( "/mongo", ctx -> anyQuery( QueryLanguage.MONGO_QL, ctx ) );
            post( "/mql", ctx -> anyQuery( QueryLanguage.MONGO_QL, ctx ) );

            post( "/sql", ctx -> anyQuery( QueryLanguage.SQL, ctx ) );

            post( "/piglet", ctx -> anyQuery( QueryLanguage.PIG, ctx ) );
            post( "/pig", ctx -> anyQuery( QueryLanguage.PIG, ctx ) );

            post( "/cql", ctx -> anyQuery( QueryLanguage.CQL, ctx ) );

            post( "/cypher", ctx -> anyQuery( QueryLanguage.CYPHER, ctx ) );
            post( "/opencypher", ctx -> anyQuery( QueryLanguage.CYPHER, ctx ) );

            StatusService.printInfo( String.format( "%s started and is listening on port %d.", INTERFACE_NAME, port ) );
        } );
    }


    public void anyQuery( QueryLanguage language, final Context ctx ) {
        QueryRequest query = ctx.bodyAsClass( QueryRequest.class );

        List<Result> results = LanguageCrud.anyQuery(
                language,
                null,
                query,
                transactionManager,
                Catalog.getInstance().getUser( Catalog.defaultUserId ).name,
                Catalog.getInstance().getDatabase( Catalog.defaultDatabaseId ).name, null );
        ctx.json( results.toArray( new Result[0] ) );

        if ( !statementCounters.containsKey( language ) ) {
            statementCounters.put( language, new AtomicLong() );
        }
        statementCounters.get( language ).incrementAndGet();
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
    public String getInterfaceType() {
        return "Http Interface";
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

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
                statementsTable.addRow( entry.getKey().name(), df.format( total == 0 ? 0 : (entry.getValue().longValue() / total) * 100 ) + " %", entry.getValue().longValue() );
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
