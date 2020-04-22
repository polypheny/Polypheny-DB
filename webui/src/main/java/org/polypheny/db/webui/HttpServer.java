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


import static spark.Spark.before;
import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.webSocket;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.transaction.TransactionManager;
import spark.Spark;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
@Slf4j
public class HttpServer extends QueryInterface {

    private Gson gson = new Gson();

    private Gson gsonExpose = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

    private final int port;


    public HttpServer( final TransactionManager transactionManager, final Authenticator authenticator, final int port ) {
        super( transactionManager, authenticator );
        this.port = port;
    }


    @Override
    public void run() {
        port( port );

        webSockets();

        Spark.staticFiles.location( "webapp/" );

        enableCORS();

        // get modified index.html
        get( "/", ( req, res ) -> {
            res.type( "text/html" );
            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream() ) {
                return streamToString( stream );
            } catch ( NullPointerException e ) {
                return "Error: Spark server could not find index.html";
            } catch ( SocketException e ) {
                return "Error: Spark server could not determine its ip address.";
            }
        } );

        crudRoutes( new Crud( transactionManager, "pa", "APP" ) );

        log.info( "HTTP Server started." );
    }


    /**
     * Defines the routes for this Server
     */
    private void crudRoutes( Crud crud ) {

        post( "/getTable", crud::getTable, gson::toJson );

        post( "/getSchemaTree", crud::getSchemaTree, gson::toJson );

        post( "/insertRow", crud::insertRow, gson::toJson );

        post( "/deleteRow", crud::deleteRow, gson::toJson );

        post( "/updateRow", crud::updateRow, gson::toJson );

        post( "/anyQuery", crud::anyQuery, gson::toJson );

        post("/classifyData", crud::classifyData, gson::toJson);

        post("/getExploreTables", crud::getExploreTables, gson::toJson);

        post("/createInitialExploreQuery", crud::createInitialExploreQuery, gson::toJson);

        post("/exploration", crud::exploration, gson::toJson);

        post( "/allStatistics", crud::getStatistics, gsonExpose::toJson );

        post( "/getColumns", crud::getColumns, gson::toJson );

        post( "/updateColumn", crud::updateColumn, gson::toJson );

        post( "/addColumn", crud::addColumn, gson::toJson );

        post( "/dropColumn", crud::dropColumn, gson::toJson );

        post( "/getTables", crud::getTables, gson::toJson );

        post( "/dropTruncateTable", crud::dropTruncateTable, gson::toJson );

        post( "/createTable", crud::createTable, gson::toJson );

        get( "/getGeneratedNames", crud::getGeneratedNames, gson::toJson );

        post( "/getConstraints", crud::getConstraints, gson::toJson );

        post( "/dropConstraint", crud::dropConstraint, gson::toJson );

        post( "/addPrimaryKey", crud::addPrimaryKey, gson::toJson );

        post( "/addUniqueConstraint", crud::addUniqueConstraint, gson::toJson );

        post( "/getIndexes", crud::getIndexes, gson::toJson );

        post( "/dropIndex", crud::dropIndex, gson::toJson );

        post( "/getUml", crud::getUml, gson::toJson );

        post( "/addForeignKey", crud::addForeignKey, gson::toJson );

        post( "/createIndex", crud::createIndex, gson::toJson );

        post( "/getPlacements", crud::getPlacements, gson::toJson );

        post( "/addDropPlacement", crud::addDropPlacement, gson::toJson );

        post( "/getAnalyzerPage", crud::getAnalyzerPage );

        post( "/closeAnalyzer", crud::closeAnalyzer );

        post( "/executeRelAlg", crud::executeRelAlg, gson::toJson );

        post( "/schemaRequest", crud::schemaRequest, gson::toJson );

        get( "/getTypeInfo", crud::getTypeInfo, gson::toJson );

        get( "/getForeignKeyActions", crud::getForeignKeyActions, gson::toJson );

        post( "/importDataset", crud::importDataset, gson::toJson );

        post( "/exportTable", crud::exportTable, gson::toJson );

        get( "/getStores", crud::getStores );

        post( "/removeStore", crud::removeStore, gson::toJson );

        post( "/updateStoreSettings", crud::updateStoreSettings, gson::toJson );

        get( "/getAdapters", crud::getAdapters );

        post( "/addStore", crud::addStore, gson::toJson );

    }


    /**
     * reads the index.html and replaces the line "//SPARK-REPLACE" with information about the ConfigServer and InformationServer
     */
    //see: http://roufid.com/5-ways-convert-inputstream-string-java/
    private String streamToString( final InputStream stream ) {
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        try ( BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( stream, Charset.defaultCharset() ) ) ) {
            while ( (line = bufferedReader.readLine()) != null ) {
                if ( line.contains( "//SPARK-REPLACE" ) ) {
                    stringBuilder.append( "\nlocalStorage.setItem('configServer.port', '" ).append( RuntimeConfig.CONFIG_SERVER_PORT.getInteger() ).append( "');" );
                    stringBuilder.append( "\nlocalStorage.setItem('informationServer.port', '" ).append( RuntimeConfig.INFORMATION_SERVER_PORT.getInteger() ).append( "');" );
                    stringBuilder.append( "\nlocalStorage.setItem('webUI.port', '" ).append( RuntimeConfig.WEBUI_SERVER_PORT.getInteger() ).append( "');" );
                } else {
                    stringBuilder.append( line );
                }
            }
        } catch ( IOException e ) {
            log.error( e.getMessage() );
        }

        return stringBuilder.toString();
    }


    /**
     * Define websocket paths
     */
    private void webSockets() {
        webSocket( "/queryAnalyzer", WebSocket.class );
    }


    /**
     * To avoid the CORS problem, when the ConfigServer receives requests from the Web UI.
     * See https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf
     */
    private static void enableCORS() {
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
