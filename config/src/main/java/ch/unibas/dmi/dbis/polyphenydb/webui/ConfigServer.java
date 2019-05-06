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


import static spark.Spark.before;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.webSocket;

import ch.unibas.dmi.dbis.polyphenydb.config.Config;
import ch.unibas.dmi.dbis.polyphenydb.config.Config.ConfigListener;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import com.google.gson.Gson;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RESTful server used by the Web UI to interact with the Config Manager.
 */
public class ConfigServer implements ConfigListener {

    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigServer.class );


    public ConfigServer() {
        port( 8081 );

        //needs to be called before route mapping!
        webSockets();
        enableCORS();
        configRoutes();
    }


    public static void main( String[] args ) {
        LOGGER.debug( "Starting config server..." );
        new ConfigServer();
    }


    private void webSockets() {
        // Websockets need to be defined before the post/get requests
        webSocket( "/configWebSocket", ConfigWebsocket.class );
    }


    /**
     * Many routes just for testing.
     * Route getPage: get a WebUiPage as JSON (with all its groups and configs).
     */
    private void configRoutes() {
        String type = "application/json";
        Gson gson = new Gson();
        ConfigManager cm = ConfigManager.getInstance();

        get( "/getPageList", ( req, res ) -> cm.getWebUiPageList() );

        // get Ui of certain page
        post( "/getPage", ( req, res ) -> {
            //input: req: {pageId: 123}
            try {
                return cm.getPage( req.body() );
            } catch ( Exception e ) {
                //if input not number or page does not exist
                return "";
            }
        } );

        // save changes from WebUi
        post( "/updateConfigs", ( req, res ) -> {
            LOGGER.trace( req.body() );
            Map<String, Object> changes = gson.fromJson( req.body(), Map.class );
            StringBuilder feedback = new StringBuilder();
            boolean allValid = true;
            for ( Map.Entry<String, Object> entry : changes.entrySet() ) {
                //cm.setConfigValue( entry.getKey(), entry.getValue() );
                Config c = cm.getConfig( entry.getKey() );
                switch ( c.getConfigType() ) {
                    case "ConfigInteger":
                        Double d = (Double) entry.getValue();
                        if ( !c.setInt( d.intValue() ) ) {
                            allValid = false;
                            feedback.append( "Could not set " ).append( c.getKey() ).append( " to " ).append( entry.getValue() ).append( " because it was blocked by Java validation. " );
                        }
                        break;
                    case "ConfigDouble":
                        if ( !c.setDouble( (double) entry.getValue() ) ) {
                            allValid = false;
                            feedback.append( "Could not set " ).append( c.getKey() ).append( " to " ).append( entry.getValue() ).append( " because it was blocked by Java validation. " );
                        }
                        break;
                    case "ConfigDecimal":
                        if ( !c.setDecimal( (BigDecimal) entry.getValue() ) ) {
                            allValid = false;
                            feedback.append( "Could not set " ).append( c.getKey() ).append( " to " ).append( entry.getValue() ).append( " because it was blocked by Java validation. " );
                        }
                        break;
                    case "ConfigLong":
                        if ( !c.setLong( (long) entry.getValue() ) ) {
                            allValid = false;
                            feedback.append( "Could not set " ).append( c.getKey() ).append( " to " ).append( entry.getValue() ).append( " because it was blocked by Java validation. " );
                        }
                    case "ConfigString":
                        if ( !c.setString( (String) entry.getValue() ) ) {
                            allValid = false;
                            feedback.append( "Could not set " ).append( c.getKey() ).append( " to " ).append( entry.getValue() ).append( " because it was blocked by Java validation. " );
                        }
                        break;
                    case "ConfigBoolean":
                        if ( !c.setBoolean( (boolean) entry.getValue() ) ) {
                            allValid = false;
                            feedback.append( "Could not set " ).append( c.getKey() ).append( " to " ).append( entry.getValue() ).append( " because it was blocked by Java validation. " );
                        }
                        break;
                    default:
                        allValid = false;
                        feedback.append( "Config with type " ).append( c.getConfigType() ).append( " is not supported yet." );
                        LOGGER.error( "Config with type " + c.getConfigType() + " is not supported yet." );
                }
                //cm.getConfig( entry.getKey() ).setObject( entry.getValue() );
            }
            if ( allValid ) {
                return "{\"success\":1}";
            } else {
                feedback.append( "All other values were saved." );
                return "{\"warning\": \"" + feedback.toString() + "\"}";
            }
        } );
    }


    /**
     * To avoid the CORS problem, when the ConfigServer receives requests from the Web UI.
     * See https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf
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
