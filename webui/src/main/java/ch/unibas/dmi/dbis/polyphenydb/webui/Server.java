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
import static spark.Spark.post;
import static spark.Spark.options;
import static spark.Spark.port;

import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger( Server.class );
    private final ConfigManager cm = ConfigManager.getInstance();

    public static void main( String[] args ) {
        if ( args.length < 4) {
            LOGGER.error( "Missing command-line arguments. Please provied the following information:\n"
                    + "java Server <host> <port> <database> <user> <password>\n"
                    + "e.g. java Server localhost 8080 myDatabase root secret" );
            System.exit( 1 );
        }

        ConfigManager cm = ConfigManager.getInstance();
        cm.registerConfig( new ConfigInteger( "configServer.port", "port of the ConfigServer", 8081 ) );
        cm.registerConfig( new ConfigInteger( "informationServer.port", "port of the InformationServer", 8082 ) );
        cm.registerConfig( new ConfigInteger( "webUI.port", "port of the webUI server", 8083 ) );

        //Spark.ignite: see https://stackoverflow.com/questions/41452156/multiple-spark-servers-in-a-single-jvm
        ConfigServer configServer = new ConfigServer( cm.getConfig( "configServer.port" ).getInt() );
        InformationServer informationServer = new InformationServer( cm.getConfig( "informationServer.port" ).getInt() );
        Server webUIServer = new Server( cm.getConfig( "webUI.port" ).getInt(), args );
    }

    public Server( final int port, String[] args ) {

        port( port );

        Spark.staticFiles.location( "webapp/" );

        enableCORS();

        //get modified index.html
        get( "/", ( req, res ) -> {
            res.type( "text/html" );
            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream()) {
                return streamToString( stream );
            } catch( NullPointerException e ){
                return "Error: Spark server could not find index.html";
            }
        } );

        crudRoutes( args );

        LOGGER.info( "HTTP Server started." );

    }


    /**
     * defines the routes for this Server
     */
    private void crudRoutes( String[] args ) {

        Crud crud = new Crud( args );

        post( "/getTable", crud::getTable );

        post( "/getSchemaTree", crud::getSchemaTree );

        post( "/insertRow", crud::insertIntoTable );

        post( "/deleteRow", crud::deleteRow );

        post( "/updateRow", crud::updateRow );

        post( "/anyQuery", crud::anyQuery );

        post( "/getColumns", crud::getColumns );

        post( "/updateColumn", crud::updateColumn );

        post( "/addColumn", crud::addColumn );

    }
    /**
     * reads the index.html and replaces the line "//SPARK-REPLACE" with information about the ConfigServer and InformationServer
     */
    //see: http://roufid.com/5-ways-convert-inputstream-string-java/
    private String streamToString( final InputStream stream ) {
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        try ( BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( stream, Charset.defaultCharset() ))) {
            while (( line = bufferedReader.readLine() ) != null ) {
                if( line.contains( "//SPARK-REPLACE" )){
                    stringBuilder.append( "\nlocalStorage.setItem('configServer.port', '" ).append( this.cm.getConfig( "configServer.port" ).getInt() ).append( "');" );
                    stringBuilder.append( "\nlocalStorage.setItem('informationServer.port', '" ).append( this.cm.getConfig( "informationServer.port" ).getInt() ).append( "');" );
                    stringBuilder.append( "\nlocalStorage.setItem('webUI.port', '" ).append( this.cm.getConfig( "webUI.port" ).getInt() ).append( "');" );
                }else {
                    stringBuilder.append(line);
                }
            }
        } catch ( IOException e ){
            LOGGER.error( e.getMessage() );
        }

        return stringBuilder.toString();
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
            res.header( "Access-Control-Allow-Origin", "*" );
            res.header( "Access-Control-Allow-Credentials", "true" );
            res.header( "Access-Control-Allow-Headers", "*" );
            res.type( "application/json" );
        } );
    }

}
