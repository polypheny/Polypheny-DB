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

import ch.unibas.dmi.dbis.polyphenydb.config.*;
import ch.unibas.dmi.dbis.polyphenydb.config.Config.ConfigListener;
import com.google.gson.Gson;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;


/**
 * RESTful server for the WebUis, working with the ConfigManager
 */
public class ConfigServer implements ConfigListener {


    public ConfigServer() {

        port( 8081 );

        //needs to be called before route mapping!
        webSockets();

        enableCORS();

        configRoutes();

    }


    public static void main( String[] args ) {
        new ConfigServer();
        System.out.println( "ConfigServer running.." );
    }


    private void webSockets() {
        //Websockets need to be defined before the post/get requests
        webSocket( "/configWebSocket", ConfigWebsocket.class );
    }


    /**
     * many routes just for testing
     * route getPage: get a WebUiPage as JSON (with all its groups and configs
     */
    private void configRoutes() {
        String type = "application/json";
        Gson gson = new Gson();
        ConfigManager cm = ConfigManager.getInstance();


        get( "/getPageList", ( req, res ) -> cm.getWebUiPageList() );

        //get Ui of certain page
        post( "/getPage", ( req, res ) -> {
            //input: req: {pageId: 123}
            try{
                return cm.getPage( req.body() );
            } catch ( Exception e ){
                //if input not number or page does not exist
                return "";
            }
        } );

        //save changes from WebUi
        post( "/updateConfigs", ( req, res ) -> {
            System.out.println( req.body() );
            Map<String, Object> changes = gson.fromJson( req.body(), Map.class );
            for ( Map.Entry<String, Object> entry : changes.entrySet() ) {
                //todo give feedback if config does not exist
                //cm.setConfigValue( entry.getKey(), entry.getValue() );
                Config c = cm.getConfig( entry.getKey() );
                switch ( c.getConfigType() ) {
                    case "ConfigInteger":
                        Double d = (Double) entry.getValue();
                        c.setInt( d.intValue() );
                        break;
                    case "ConfigDouble":
                        c.setDouble( (double) entry.getValue() );
                        break;
                    case "ConfigDecimal":
                        c.setDecimal( (BigDecimal) entry.getValue() );
                        break;
                    case "ConfigLong":
                        c.setLong( (long) entry.getValue() );
                    case "ConfigString":
                        c.setString( (String) entry.getValue() );
                        break;
                    case "ConfigBoolean":
                        c.setBoolean( (boolean) entry.getValue() );
                        break;
                    default:
                        System.err.println( "Config with type " + c.getConfigType() + " is not supported yet." );
                }
                //cm.getConfig( entry.getKey() ).setObject( entry.getValue() );
            }
            return "{\"success\":1}";
        } );
    }

    // https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf


    /**
     * to avoid the CORS problem, when the ConfigServer receives requests from the WebUi
     */
    public static void enableCORS() {
        //staticFiles.header("Access-Control-Allow-Origin", "*");

        options( "/*", ( req, res ) -> {
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

        before( ( req, res ) -> {
            //res.header("Access-Control-Allow-Origin", "*");
            res.header( "Access-Control-Allow-Origin", "http://localhost:4200" );
            res.header( "Access-Control-Allow-Credentials", "true" );
            res.header( "Access-Control-Allow-Headers", "*" );
            res.type( "application/json" );
        } );
    }


    @Override
    public void onConfigChange( Config c ) {
        Gson gson = new Gson();
        try {
            ConfigWebsocket.broadcast( gson.toJson( c ) );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }


    @Override
    public void restart( Config c ) {

    }

}
