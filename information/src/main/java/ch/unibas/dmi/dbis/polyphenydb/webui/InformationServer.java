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


import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.webSocket;

import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RESTful server for the WebUis, working with the InformationManager
 */
public class InformationServer {

    private static final Logger LOGGER = LoggerFactory.getLogger( InformationServer.class );


    public InformationServer() {

        port( 8082 );

        // Needs to be called before defining routes!
        webSockets();

        enableCORS();

        informationRoutes();

        LOGGER.info( "InformationServer started." );
    }


    private void webSockets() {
        // Websockets need to be defined before the post/get requests
        webSocket( "/informationWebSocket", InformationWebSocket.class );
    }


    private void informationRoutes() {
        InformationManager im = InformationManager.getInstance();

        get( "/getPageList", ( req, res ) -> im.getPageList() );

        post( "/getPage", ( req, res ) -> {
            //input: req: {pageId: "page1"}
            try {
                return im.getPage( req.body() );
            } catch ( Exception e ) {
                // if input not number or page does not exist
                return "";
            }
        } );

    }


    /**
     * To avoid the CORS problem, when the ConfigServer receives requests from the WebUi
     */
    private static void enableCORS() {
        InformationServer.enableCORS();
    }


}
