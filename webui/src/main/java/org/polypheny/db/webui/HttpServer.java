/*
 * Copyright 2019-2022 The Polypheny Project
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
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.javalin.Javalin;
import io.javalin.plugin.json.JsonMapper;
import io.javalin.websocket.WsConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.function.Consumer;
import javax.servlet.ServletException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.StatusService;
import org.polypheny.db.adapter.Adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.Adapter.AdapterSettingDeserializer;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.information.InformationDuration.Duration;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationStacktrace;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.webui.models.Result;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
@Slf4j
public class HttpServer implements Runnable {

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;

    private static final Gson gson;
    private final Gson gsonExpose = new GsonBuilder()
            .excludeFieldsWithoutExposeAnnotation()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();

    public static TypeAdapter<Throwable> throwableTypeAdapter;


    static {
        throwableTypeAdapter = new TypeAdapter<Throwable>() {
            @Override
            public void write( JsonWriter out, Throwable value ) throws IOException {
                if ( value == null ) {
                    out.nullValue();
                    return;
                }
                out.beginObject();
                out.name( "message" );
                out.value( value.getMessage() );
                out.endObject();
            }


            @Override
            public Throwable read( JsonReader in ) throws IOException {
                return new Throwable( in.nextString() );
            }
        };
        gson = new GsonBuilder()
                .enableComplexMapKeySerialization()
                .registerTypeAdapter( DataSource.class, DataSource.getSerializer() )
                .registerTypeAdapter( DataStore.class, DataStore.getSerializer() )
                .registerTypeAdapter( PolyType.class, PolyType.getSerializer() )
                .registerTypeAdapter( AdapterInformation.class, AdapterInformation.getSerializer() )
                .registerTypeAdapter( AbstractAdapterSetting.class, new AdapterSettingDeserializer() )
                .registerTypeAdapter( Throwable.class, throwableTypeAdapter )
                .registerTypeAdapter( InformationDuration.class, InformationDuration.getSerializer() )
                .registerTypeAdapter( Duration.class, Duration.getSerializer() )
                .registerTypeAdapter( Result.class, Result.getSerializer() )
                .registerTypeAdapter( InformationPage.class, InformationPage.getSerializer() )
                .registerTypeAdapter( InformationGroup.class, InformationGroup.getSerializer() )
                .registerTypeAdapter( InformationStacktrace.class, InformationStacktrace.getSerializer() )
                .create();
    }


    public HttpServer( final TransactionManager transactionManager, final Authenticator authenticator ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
    }


    @Override
    public void run() {
        JsonMapper gsonMapper = new JsonMapper() {
            @NotNull
            @Override
            public String toJsonString( @NotNull Object obj ) {
                return gson.toJson( obj );
            }


            @NotNull
            @Override
            public <T> T fromJsonString( @NotNull String json, @NotNull Class<T> targetClass ) {
                return gson.fromJson( json, targetClass );
            }
        };
        Javalin server = Javalin.create( config -> {
            config.jsonMapper( gsonMapper );
            config.enableCorsForAllOrigins();
            config.addStaticFiles( staticFileConfig -> {
                staticFileConfig.directory = "webapp/";
            } );
        } ).start( RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );

        Crud crud = new Crud(
                transactionManager,
                Catalog.getInstance().getUser( Catalog.defaultUserId ).name,
                Catalog.getInstance().getDatabase( Catalog.defaultDatabaseId ).name );

        WebSocket webSocketHandler = new WebSocket( crud, gson );
        webSockets( server, webSocketHandler );

        // Get modified index.html
        server.get( "/", ctx -> {
            ctx.contentType( "text/html" );

            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream() ) {
                ctx.result( streamToString( stream ) );
            } catch ( NullPointerException e ) {
                ctx.result( "Error: Could not find index.html" );
            }
        } );

        attachExceptions( server );

        crudRoutes( server, crud );

        StatusService.printInfo( String.format( "Polypheny-UI started and is listening on port %d.", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() ) );
    }


    private void attachExceptions( Javalin server ) {
        server.exception( SocketException.class, ( e, ctx ) -> {
            ctx.status( 400 ).result( "Error: Could not determine IP address." );
        } );

        defaultException( IOException.class, server );
        defaultException( ServletException.class, server );
        defaultException( UnknownDatabaseException.class, server );
        defaultException( UnknownSchemaException.class, server );
        defaultException( UnknownTableException.class, server );
        defaultException( UnknownColumnException.class, server );
    }


    private void defaultException( Class<? extends Exception> exceptionClass, Javalin server ) {
        server.exception( exceptionClass, ( e, ctx ) -> {
            ctx.status( 400 ).json( new Result( e ) );
        } );
    }


    /**
     * Defines the routes for this Server
     */
    private void crudRoutes( Javalin webuiServer, Crud crud ) {
        webuiServer.post( "/getSchemaTree", crud::getSchemaTree );

        webuiServer.get( "/getTypeSchemas", crud::getTypeSchemas );

        webuiServer.post( "/insertRow", crud::insertRow );

        webuiServer.post( "/deleteRow", crud::deleteRow );

        webuiServer.post( "/updateRow", crud::updateRow );

        webuiServer.post( "/batchUpdate", crud::batchUpdate );

        webuiServer.post( "/classifyData", crud::classifyData );

        webuiServer.post( "/getExploreTables", crud::getExploreTables );

        webuiServer.post( "/createInitialExploreQuery", crud::createInitialExploreQuery );

        webuiServer.post( "/exploration", crud::exploration );

        webuiServer.post( "/allStatistics", ( ctx ) -> crud.statisticCrud.getStatistics( ctx, gsonExpose ) );

        webuiServer.post( "/getTableStatistics", crud.statisticCrud::getTableStatistics );

        webuiServer.post( "/getDashboardInformation", crud.statisticCrud::getDashboardInformation );

        webuiServer.post( "/getDashboardDiagram", crud.statisticCrud::getDashboardDiagram );

        webuiServer.post( "/getColumns", crud::getColumns );

        webuiServer.post( "/getDataSourceColumns", crud::getDataSourceColumns );

        webuiServer.post( "/getAvailableSourceColumns", crud::getAvailableSourceColumns );

        webuiServer.post( "/updateColumn", crud::updateColumn );

        webuiServer.post( "/getMaterializedInfo", crud::getMaterializedInfo );

        webuiServer.post( "/updateMaterialized", crud::updateMaterialized );

        webuiServer.post( "/addColumn", crud::addColumn );

        webuiServer.post( "/dropColumn", crud::dropColumn );

        webuiServer.post( "/getTables", crud::getTables );

        webuiServer.post( "/renameTable", crud::renameTable );

        webuiServer.post( "/dropTruncateTable", crud::dropTruncateTable );

        webuiServer.post( "/createTable", crud::createTable );

        webuiServer.post( "/createCollection", crud.languageCrud::createCollection );

        webuiServer.get( "/getGeneratedNames", crud::getGeneratedNames );

        webuiServer.post( "/getConstraints", crud::getConstraints );

        webuiServer.post( "/dropConstraint", crud::dropConstraint );

        webuiServer.post( "/addPrimaryKey", crud::addPrimaryKey );

        webuiServer.post( "/addUniqueConstraint", crud::addUniqueConstraint );

        webuiServer.post( "/getIndexes", crud::getIndexes );

        webuiServer.post( "/dropIndex", crud::dropIndex );

        webuiServer.post( "/getUml", crud::getUml );

        webuiServer.post( "/addForeignKey", crud::addForeignKey );

        webuiServer.post( "/createIndex", crud::createIndex );

        webuiServer.post( "/getUnderlyingTable", crud::getUnderlyingTable );

        webuiServer.post( "/getPlacements", crud::getPlacements );

        webuiServer.post( "/addDropPlacement", crud::addDropPlacement );

        webuiServer.get( "/getPartitionTypes", crud::getPartitionTypes );

        webuiServer.post( "/getPartitionFunctionModel", crud::getPartitionFunctionModel );

        webuiServer.post( "/partitionTable", crud::partitionTable );

        webuiServer.post( "/mergePartitions", crud::mergePartitions );

        webuiServer.post( "/modifyPartitions", crud::modifyPartitions );

        webuiServer.post( "/getAnalyzerPage", crud::getAnalyzerPage );

        webuiServer.post( "/schemaRequest", crud::schemaRequest );

        webuiServer.get( "/getTypeInfo", crud::getTypeInfo );

        webuiServer.get( "/getForeignKeyActions", crud::getForeignKeyActions );

        webuiServer.post( "/importDataset", crud::importDataset );

        webuiServer.post( "/exportTable", crud::exportTable );

        webuiServer.get( "/getStores", crud::getStores );

        webuiServer.get( "/getSources", crud::getSources );

        webuiServer.post( "/getAvailableStoresForIndexes", ( ctx ) -> crud.getAvailableStoresForIndexes( ctx, gson ) );

        webuiServer.post( "/removeAdapter", crud::removeAdapter );

        webuiServer.post( "/updateAdapterSettings", crud::updateAdapterSettings );

        webuiServer.get( "/getAvailableStores", crud::getAvailableStores );

        webuiServer.get( "/getAvailableSources", crud::getAvailableSources );

        webuiServer.post( "/addAdapter", ( ctx ) -> crud.addAdapter( ctx, gson ) );

        webuiServer.get( "/getQueryInterfaces", crud::getQueryInterfaces );

        webuiServer.get( "/getAvailableQueryInterfaces", crud::getAvailableQueryInterfaces );

        webuiServer.post( "/addQueryInterface", crud::addQueryInterface );

        webuiServer.post( "/updateQueryInterfaceSettings", crud::updateQueryInterfaceSettings );

        webuiServer.post( "/removeQueryInterface", crud::removeQueryInterface );

        webuiServer.get( "/getFile/{file}", crud::getFile );

        webuiServer.get( "/testDockerInstance/{dockerId}", crud::testDockerInstance );

        webuiServer.get( "/usedDockerPorts", crud::getUsedDockerPorts );

        webuiServer.get( "/getDocumentDatabases", crud.languageCrud::getDocumentDatabases );

        webuiServer.get( "/product", ctx -> ctx.result( "Polypheny-DB" ) );

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
    private void webSockets( Javalin webuiServer, Consumer<WsConfig> handler ) {
        webuiServer.ws( "/webSocket", handler );
    }


    /**
     * To avoid the CORS problem, when the ConfigServer receives requests from the Web UI.
     * See https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf
     */
    private static void enableCORS( Javalin webuiServer ) {
        //staticFiles.header("Access-Control-Allow-Origin", "*");

        webuiServer.options( "/*", ctx -> {
            String accessControlRequestHeaders = ctx.req.getHeader( "Access-Control-Request-Headers" );
            if ( accessControlRequestHeaders != null ) {
                ctx.res.setHeader( "Access-Control-Allow-Headers", accessControlRequestHeaders );
            }

            String accessControlRequestMethod = ctx.req.getHeader( "Access-Control-Request-Method" );
            if ( accessControlRequestMethod != null ) {
                ctx.res.setHeader( "Access-Control-Allow-Methods", accessControlRequestMethod );
            }

            ctx.result( "OK" );
        } );

        webuiServer.before( ctx -> {
            //res.header("Access-Control-Allow-Origin", "*");
            ctx.res.setHeader( "Access-Control-Allow-Origin", "*" );
            ctx.res.setHeader( "Access-Control-Allow-Credentials", "true" );
            ctx.res.setHeader( "Access-Control-Allow-Headers", "*" );
            ctx.res.setContentType( "application/json" );
        } );
    }


}
