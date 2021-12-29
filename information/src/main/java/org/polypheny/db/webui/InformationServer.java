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

package org.polypheny.db.webui;


import com.google.gson.Gson;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JsonMapper;
import java.io.IOException;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationResponse;


/**
 * RESTful server for requesting data from the information manager. It is primarily used by the Polypheny-UI.
 */
@Slf4j
public class InformationServer implements InformationObserver {

    private final Gson gson = new Gson();


    public InformationServer( final int port ) {
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
        Javalin http = Javalin.create( config -> {
            config.jsonMapper( gsonMapper );
            config.enableCorsForAllOrigins();
        } ).start( port );

        // Needs to be called before defining routes!
        webSockets( http );

        //enableCORS( http );

        informationRoutes( http );

        log.info( "InformationServer started." );
    }


    private void webSockets( final Javalin http ) {
        // Websockets need to be defined before the post/get requests
        http.ws( "/informationWebSocket", new InformationWebSocket() );
    }


    private void informationRoutes( final Javalin http ) {
        InformationManager im = InformationManager.getInstance();
        im.observe( this );

        http.get( "/getPageList", ctx -> ctx.result( im.getPageList() ) );

        http.post( "/getPage", ctx -> {
            //input: req: {pageId: "page1"}
            try {
                InformationPage page = im.getPage( ctx.body() );
                if ( page == null ) {
                    log.error( "Request for unknown page: {}", ctx.body() );
                    ctx.result( "" );
                    return;
                }
                ctx.result( page.asJson() );
            } catch ( Exception e ) {
                // if input not number or page does not exist
                log.error( "Caught exception!", e );
                ctx.result( "" );
            }
        } );

        http.post( "/executeAction", ctx -> {
            try {
                InformationAction action = ctx.bodyAsClass( InformationAction.class );
                String msg = im.getInformation( action.getId() ).unwrap( InformationAction.class ).executeAction( action.getParameters() );
                ctx.json( new InformationResponse().message( msg ) );
            } catch ( Exception e ) {
                String errorMsg = "Could not execute InformationAction";
                log.error( errorMsg, e );
                ctx.json( new InformationResponse().error( errorMsg ) );
            }
        } );

        http.post( "/refreshPage", ctx -> {
            //refresh not necessary, since getPage already triggers a refresh
            try {
                im.getPage( ctx.body() );
            } catch ( Exception e ) {
                log.error( "Caught exception!", e );
            }
            ctx.result( "" );
        } );

        http.post( "/refreshGroup", ctx -> {
            try {
                im.getGroup( ctx.body() ).refresh();
            } catch ( Exception e ) {
                log.error( "Caught exception!", e );
            }
            ctx.result( "" );
        } );

        http.get( "/getEnabledPlugins", this::getEnabledPlugins );

    }


    public void getEnabledPlugins( final Context ctx ) {
        ctx.json( Collections.singletonList( "Explore-By-Example" ) );
    }


    /**
     * Observe Changes in Information Objects of the Information Manager
     */
    @Override
    public void observeInfos( final Information info, final String informationManagerId, final Session session ) {
        try {
            InformationWebSocket.broadcast( info.asJson() );
        } catch ( IOException e ) {
            log.info( "Error while sending information object to web ui!", e );
        }
    }


    /**
     * Observe Changes in the PageList of the Information Manager
     */
    @Override
    public void observePageList( final InformationPage[] pages, final String debugId, final Session session ) {
        // TODO: can be implemented if needed
    }

}
