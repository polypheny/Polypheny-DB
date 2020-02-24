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

package org.polypheny.db.webui;


import static spark.Service.ignite;

import com.google.gson.Gson;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationResponse;
import spark.Service;


/**
 * RESTful server for the WebUis, working with the InformationManager.
 */
@Slf4j
public class InformationServer implements InformationObserver {

    Gson gson = new Gson();

    public InformationServer( final int port ) {

        Service http = ignite().port( port );

        // Needs to be called before defining routes!
        webSockets( http );

        enableCORS( http );

        informationRoutes( http );

        log.info( "InformationServer started." );
    }


    private void webSockets( final Service http ) {
        // Websockets need to be defined before the post/get requests
        http.webSocket( "/informationWebSocket", InformationWebSocket.class );
    }


    private void informationRoutes( final Service http ) {
        InformationManager im = InformationManager.getInstance();
        im.observe( this );

        http.get( "/getPageList", ( req, res ) -> im.getPageList() );

        http.post( "/getPage", ( req, res ) -> {
            //input: req: {pageId: "page1"}
            try {
                return im.getPage( req.body() ).asJson();
            } catch ( Exception e ) {
                // if input not number or page does not exist
                log.error( "Caught exception!", e );
                return "";
            }
        } );

        http.post( "/executeAction", ( res, req ) -> {
            try {
                String msg = im.getInformation( res.body() ).unwrap( InformationAction.class ).executeAction();
                return new InformationResponse().message( msg );
            } catch ( Exception e ) {
                String errorMsg = "Could not execute InformationAction";
                log.error( errorMsg, e );
                return new InformationResponse().error( errorMsg );
            }
        }, gson::toJson );
    }


    /**
     * Observe Changes in Information Objects of the Information Manager
     */
    @Override
    public void observeInfos( final Information info ) {
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
    public void observePageList( final String debugId, final InformationPage[] pages ) {
        //todo can be implemented if needed
    }

    /**
     * To avoid the CORS problem, when the ConfigServer receives requests from the WebUi
     */
    private static void enableCORS( final Service http ) {
        http.options( "/*", ( req, res ) -> {
            String accessControlRequestHeaders = req.headers( "Access-Control-Request-Headers" );
            if ( accessControlRequestHeaders != null ) {
                res.header( "Access-Control-Allow-Headers", accessControlRequestHeaders );
            }

            String accessControlRequestMethod = req.headers( "Access-Control-Request-Method" );
            if ( accessControlRequestMethod != null ) {
                res.header( "Access-Control-Allow-Methods", accessControlRequestMethod );
            }

            return "OK";
        } );

        http.before( ( req, res ) -> {
            //res.header("Access-Control-Allow-Origin", "*");
            res.header( "Access-Control-Allow-Origin", "*" );
            res.header( "Access-Control-Allow-Credentials", "true" );
            res.header( "Access-Control-Allow-Headers", "*" );
            res.type( "application/json" );
        } );
    }

}
