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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger( Server.class );

    public static void main( String[] args ) {
        Server s = new Server();
    }

    public Server() {

        port( 8083 );

        Spark.staticFiles.location( "webapp/" );

        enableCORS();

        //get modified index.html
        get( "/", ( req, res ) -> {
            res.type( "text/html" );
            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream()) {
                final DatagramSocket socket = new DatagramSocket();
                socket.connect( InetAddress.getByName( "8.8.8.8" ), 10002 );
                String ip = socket.getLocalAddress().getHostAddress();
                return streamToString( stream, ip );
            } catch( NullPointerException e ){
                return "Error: Spark server could not find index.html";
            } catch ( SocketException e ){
                return "Error: Spark server could not determine its ip address.";
            }
        } );

        crudRoutes();

        LOGGER.info( "HTTP Server started." );

    }


    /**
     * defines the routes for this Server
     */
    private void crudRoutes() {

        Crud crud = new Crud();

        post( "/getTable", crud::getTable );

        get( "getSchemaTree", crud::getSchemaTree );

    }

    /**
     * reads the index.html and replaces the line "//SPARK-REPLACE" with information about the ConfigServer and InformationServer
     */
    //see: http://roufid.com/5-ways-convert-inputstream-string-java/
    private String streamToString( final InputStream stream, final String ip ) {
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        try ( BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( stream, Charset.defaultCharset() ))) {
            while (( line = bufferedReader.readLine() ) != null ) {
                if( line.contains( "//SPARK-REPLACE" )){
                    stringBuilder.append( "\nlocalStorage.setItem('settings.config.rest', 'http://" ).append( ip ).append( ":8081');" );
                    stringBuilder.append( "\nlocalStorage.setItem('settings.config.socket', 'ws://" ).append( ip ).append( ":8081/configWebSocket');" );
                    stringBuilder.append( "\nlocalStorage.setItem('settings.information.rest', 'http://" ).append( ip ).append( ":8082');" );
                    stringBuilder.append( "\nlocalStorage.setItem('settings.information.socket', 'ws://" ).append( ip ).append( ":8082/informationWebSocket');" );
                    stringBuilder.append( "\nlocalStorage.setItem('settings.crud.rest', 'http://" ).append( ip ).append( ":8083');\n" );
                }else {
                    stringBuilder.append(line);
                }
            }
        } catch ( IOException e ){
            e.printStackTrace();
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
