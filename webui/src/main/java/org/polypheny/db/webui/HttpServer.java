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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.TransactionManager;
import spark.Service;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
@Slf4j
public class HttpServer implements Runnable {

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;

    private final Gson gson = new Gson();
    private final Gson gsonExpose = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();


    public HttpServer( final TransactionManager transactionManager, final Authenticator authenticator ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
    }


    @Override
    public void run() {
        final Service server = Service.ignite();
        server.port( RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        Crud crud = new Crud( transactionManager, "pa", "APP" );

        WebSocket webSocketHandler = new WebSocket( crud );
        webSockets( server, webSocketHandler );

        server.staticFiles.location( "webapp/" );

        enableCORS( server );

        URL url = this.getClass().getClassLoader().getResource( "webapp/" );
        File mmFolder = new File( url.getPath(), "mm-files" );
        mmFolder.mkdirs();

        // get modified index.html
        server.get( "/", ( req, res ) -> {
            res.type( "text/html" );
            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream() ) {
                return streamToString( stream );
            } catch ( NullPointerException e ) {
                return "Error: Spark server could not find index.html";
            } catch ( SocketException e ) {
                return "Error: Spark server could not determine its ip address.";
            }
        } );

        crudRoutes( server, crud );

        log.info( "Polypheny-UI started and is listening on port {}.", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
    }


    /**
     * Defines the routes for this Server
     */
    private void crudRoutes( Service webuiServer, Crud crud ) {
        webuiServer.post( "/getSchemaTree", crud::getSchemaTree, gson::toJson );

        webuiServer.post( "/insertRow", "multipart/form-data", crud::insertRow, gson::toJson );

        webuiServer.post( "/deleteRow", crud::deleteRow, gson::toJson );

        webuiServer.post( "/updateRow", "multipart/form-data", crud::updateRow, gson::toJson );

        webuiServer.post( "/batchUpdate", "multipart/form-data", crud::batchUpdate, gson::toJson );

        webuiServer.post( "/classifyData", crud::classifyData, gson::toJson );

        webuiServer.post( "/getExploreTables", crud::getExploreTables, gson::toJson );

        webuiServer.post( "/createInitialExploreQuery", crud::createInitialExploreQuery, gson::toJson );

        webuiServer.post( "/exploration", crud::exploration, gson::toJson );

        webuiServer.post( "/allStatistics", crud::getStatistics, gsonExpose::toJson );

        webuiServer.post( "/getColumns", crud::getColumns, gson::toJson );

        webuiServer.post( "/updateColumn", crud::updateColumn, gson::toJson );

        webuiServer.post( "/addColumn", crud::addColumn, gson::toJson );

        webuiServer.post( "/dropColumn", crud::dropColumn, gson::toJson );

        webuiServer.post( "/getTables", crud::getTables, gson::toJson );

        webuiServer.post( "/renameTable", crud::renameTable, gson::toJson );

        webuiServer.post( "/dropTruncateTable", crud::dropTruncateTable, gson::toJson );

        webuiServer.post( "/createTable", crud::createTable, gson::toJson );

        webuiServer.get( "/getGeneratedNames", crud::getGeneratedNames, gson::toJson );

        webuiServer.post( "/getConstraints", crud::getConstraints, gson::toJson );

        webuiServer.post( "/dropConstraint", crud::dropConstraint, gson::toJson );

        webuiServer.post( "/addPrimaryKey", crud::addPrimaryKey, gson::toJson );

        webuiServer.post( "/addUniqueConstraint", crud::addUniqueConstraint, gson::toJson );

        webuiServer.post( "/getIndexes", crud::getIndexes, gson::toJson );

        webuiServer.post( "/dropIndex", crud::dropIndex, gson::toJson );

        webuiServer.post( "/getUml", crud::getUml, gson::toJson );

        webuiServer.post( "/addForeignKey", crud::addForeignKey, gson::toJson );

        webuiServer.post( "/createIndex", crud::createIndex, gson::toJson );

        webuiServer.post( "/getPlacements", crud::getPlacements, gson::toJson );

        webuiServer.post( "/addDropPlacement", crud::addDropPlacement, gson::toJson );

        webuiServer.post( "/getAnalyzerPage", crud::getAnalyzerPage );

        webuiServer.post( "/schemaRequest", crud::schemaRequest, gson::toJson );

        webuiServer.get( "/getTypeInfo", crud::getTypeInfo );

        webuiServer.get( "/getForeignKeyActions", crud::getForeignKeyActions, gson::toJson );

        webuiServer.post( "/importDataset", crud::importDataset, gson::toJson );

        webuiServer.post( "/exportTable", crud::exportTable, gson::toJson );

        webuiServer.get( "/getStores", crud::getStores );

        webuiServer.post( "/getAvailableStoresForIndexes", crud::getAvailableStoresForIndexes );

        webuiServer.post( "/removeStore", crud::removeStore, gson::toJson );

        webuiServer.post( "/updateStoreSettings", crud::updateStoreSettings, gson::toJson );

        webuiServer.get( "/getAdapters", crud::getAdapters );

        webuiServer.post( "/addStore", crud::addStore, gson::toJson );

        webuiServer.get( "/getQueryInterfaces", crud::getQueryInterfaces );

        webuiServer.get( "/getAvailableQueryInterfaces", crud::getAvailableQueryInterfaces );

        webuiServer.post( "/addQueryInterface", crud::addQueryInterface, gson::toJson );

        webuiServer.post( "/updateQueryInterfaceSettings", crud::updateQueryInterfaceSettings, gson::toJson );

        webuiServer.post( "/removeQueryInterface", crud::removeQueryInterface, gson::toJson );

        webuiServer.get( "/getFile/:file", crud::getFile );

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
    private void webSockets( Service webuiServer, WebSocket webSocketHandler ) {
        webuiServer.webSocket( "/webSocket", webSocketHandler );
    }


    /**
     * To avoid the CORS problem, when the ConfigServer receives requests from the Web UI.
     * See https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf
     */
    private static void enableCORS( Service webuiServer ) {
        //staticFiles.header("Access-Control-Allow-Origin", "*");

        webuiServer.options( "/*", ( req, res ) -> {
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

        webuiServer.before( ( req, res ) -> {
            //res.header("Access-Control-Allow-Origin", "*");
            res.header( "Access-Control-Allow-Origin", "*" );
            res.header( "Access-Control-Allow-Credentials", "true" );
            res.header( "Access-Control-Allow-Headers", "*" );
            res.type( "application/json" );
        } );
    }

}
