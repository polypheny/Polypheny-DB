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

package org.polypheny.db.http;


import static io.javalin.apibuilder.ApiBuilder.post;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JsonMapper;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
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

    private final Gson gson = new Gson();

    private final int port;
    private final String uniqueName;

    // Counter
    private final AtomicLong deleteCounter = new AtomicLong();
    private final AtomicLong getCounter = new AtomicLong();
    private final AtomicLong patchCounter = new AtomicLong();
    private final AtomicLong postCounter = new AtomicLong();

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

        restServer.routes( () -> {
            post( "/mongo", ctx -> anyQuery( QueryLanguage.MONGO_QL, ctx ) );
            post( "/mql", ctx -> anyQuery( QueryLanguage.MONGO_QL, ctx ) );

            post( "/sql", ctx -> anyQuery( QueryLanguage.SQL, ctx ) );

            post( "/piglet", ctx -> anyQuery( QueryLanguage.PIG, ctx ) );
            post( "/pig", ctx -> anyQuery( QueryLanguage.PIG, ctx ) );

            post( "/cql", ctx -> anyQuery( QueryLanguage.CQL, ctx ) );
            log.info( "{} started and is listening on port {}.", INTERFACE_NAME, port );
        } );
    }


    public void anyQuery( QueryLanguage language, final Context ctx ) {
        QueryRequest query = ctx.bodyAsClass( QueryRequest.class );

        ctx.json( LanguageCrud.anyQuery(
                language,
                null,
                query,
                transactionManager,
                Catalog.getInstance().getUser( Catalog.defaultUserId ).name,
                Catalog.getInstance().getDatabase( Catalog.defaultDatabaseId ).name,
                null ).toArray( new Result[0] ) );
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
