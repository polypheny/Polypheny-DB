/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import static spark.Service.ignite;

import ch.unibas.dmi.dbis.polyphenydb.information.Information;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationObserver;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Service;


/**
 * RESTful server for the WebUis, working with the InformationManager
 */
public class InformationServer implements InformationObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger( InformationServer.class );


    public InformationServer( final int port ) {


        Service http = ignite().port( port );

        // Needs to be called before defining routes!
        webSockets( http );

        enableCORS( http );

        informationRoutes( http );

        LOGGER.info( "InformationServer started." );
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
                e.printStackTrace();
                return "";
            }
        } );

    }


    /**
     * Observe Changes in Information Objects of the Information Manager
     */
    @Override
    public void observeInfos( final Information info ) {
        try {
            InformationWebSocket.broadcast( info.asJson() );
        } catch ( IOException e ) {
            LOGGER.info( "Error while sending information object to web ui!", e );
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
