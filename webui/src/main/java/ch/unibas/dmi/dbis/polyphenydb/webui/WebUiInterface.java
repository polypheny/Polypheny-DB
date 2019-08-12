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

import ch.unibas.dmi.dbis.polyphenydb.Authenticator;
import ch.unibas.dmi.dbis.polyphenydb.QueryInterface;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
public class WebUiInterface extends QueryInterface {

    private static final Logger LOGGER = LoggerFactory.getLogger( WebUiInterface.class );
    private final ConfigManager cm = ConfigManager.getInstance();
    private Gson gson = new Gson();


    public WebUiInterface( final TransactionManager transactionManager, final Authenticator authenticator, final int port, Crud crud ) {
        super( transactionManager, authenticator );

        port( port );

        webSockets();

        Spark.staticFiles.location( "webapp/" );

        enableCORS();

        // get modified index.html
        get( "/", ( req, res ) -> {
            res.type( "text/html" );
            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream()) {
                return streamToString( stream );
            } catch( NullPointerException e ){
                return "Error: Spark server could not find index.html";
            } catch ( SocketException e ) {
                return "Error: Spark server could not determine its ip address.";
            }
        } );

        crudRoutes( crud );

        LOGGER.info( "HTTP Server started." );
    }


    public WebUiInterface( final TransactionManager transactionManager, final Authenticator authenticator, final int port ) {
        this(
                transactionManager,
                authenticator,
                port,
                new CrudPolypheny(
                        "ch.unibas.dmi.dbis.polyphenydb.jdbc.Driver",
                        "jdbc:polypheny://",
                        "localhost",
                        20591,
                        "app",
                        "user",
                        "pa" )
                        .setTransactionManager( transactionManager ) );
    }


    @Override
    public void run() {
        // TODO: Start server
    }


    /**
     * defines the routes for this Server
     */
    private void crudRoutes( Crud crud ) {

        post( "/getTable", crud::getTable, gson::toJson );

        post( "/getSchemaTree", crud::getSchemaTree, gson::toJson );

        post( "/insertRow", crud::insertRow, gson::toJson );

        post( "/deleteRow", crud::deleteRow, gson::toJson );

        post( "/updateRow", crud::updateRow, gson::toJson );

        post( "/anyQuery", crud::anyQuery, gson::toJson );

        post( "/getColumns", crud::getColumns, gson::toJson );

        post( "/updateColumn", crud::updateColumn, gson::toJson );

        post( "/addColumn", crud::addColumn, gson::toJson );

        post( "/dropColumn", crud::dropColumn, gson::toJson );

        post( "/getTables", crud::getTables, gson::toJson );

        post( "/dropTruncateTable", crud::dropTruncateTable, gson::toJson );

        post( "/createTable", crud::createTable, gson::toJson );

        post( "/getConstraints", crud::getConstraints, gson::toJson );

        post( "/dropConstraint", crud::dropConstraint, gson::toJson );

        post( "/addPrimaryKey", crud::addPrimaryKey, gson::toJson );

        post( "/getIndexes", crud::getIndexes, gson::toJson );

        post( "/dropIndex", crud::dropIndex, gson::toJson );

        post( "/getUml", crud::getUml, gson::toJson );

        post( "/addForeignKey", crud::addForeignKey, gson::toJson );

        post( "/createIndex", crud::createIndex, gson::toJson );

        post( "/getAnalyzerPage", crud::getAnalyzerPage );

        post( "/closeAnalyzer", crud::closeAnalyzer );

        post( "/executeRelAlg", crud::executeRelAlg, gson::toJson );

        post( "/schemaRequest", crud::schemaRequest, gson::toJson );

        get( "/getTypeInfo", crud::getTypeInfo, gson::toJson );

        get( "/getForeignKeyActions", crud::getForeignKeyActions, gson::toJson );
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
                    stringBuilder.append( "\nlocalStorage.setItem('configServer.port', '" ).append( RuntimeConfig.CONFIG_SERVER_PORT.getInteger() ).append( "');" );
                    stringBuilder.append( "\nlocalStorage.setItem('informationServer.port', '" ).append( RuntimeConfig.INFORMATION_SERVER_PORT.getInteger() ).append( "');" );
                    stringBuilder.append( "\nlocalStorage.setItem('webUI.port', '" ).append( RuntimeConfig.WEBUI_SERVER_PORT.getInteger() ).append( "');" );
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
     * Define websocket paths
     */
    private void webSockets() {
        webSocket( "/queryAnalyzer", CrudWebSocket.class );
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
