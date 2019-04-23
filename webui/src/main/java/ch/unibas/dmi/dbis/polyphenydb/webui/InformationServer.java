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


import static spark.Spark.*;

import ch.unibas.dmi.dbis.polyphenydb.informationprovider.*;
import ch.unibas.dmi.dbis.polyphenydb.informationprovider.InformationGraph.GraphType;
import com.google.gson.Gson;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;


/**
 * RESTful server for the WebUis, working with the InformationManager
 */
public class InformationServer {


    public InformationServer() {

        port( 8082 );

        //needs to be called before defining routes!
        webSockets();

        enableCORS();

        informationRoutes();

    }


    public static void main( String[] args ) {
        new InformationServer();
        System.out.println( "InformationServer running.." );
    }


    private void webSockets() {
        //Websockets need to be defined before the post/get requests
        webSocket( "/informationWebSocket", InformationWebSocket.class );
    }


    private void informationRoutes() {
        Gson gson = new Gson();
        InformationManager im = InformationManager.getInstance();

        get( "/getPageList", ( req, res ) -> im.getPageList() );

        post( "/getPage", ( req, res ) -> {
            //input: req: {pageId: "page1"}
            try {
                //System.out.println("get page "+req.body());
                return im.getPage( req.body() );
            } catch ( Exception e ) {
                //if input not number or page does not exist
                return "";
            }
        } );

    }


    /**
     * to avoid the CORS problem, when the ConfigServer receives requests from the WebUi
     */
    private static void enableCORS() {
        ConfigServer.enableCORS();
    }


}
