/*
 * Copyright 2019-2023 The Polypheny Project
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


import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.websocket.WsConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatusNotificationService;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.ConfigService.Consumer3;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.results.RelationalResult;


/**
 * HTTP server for serving the Polypheny-DB UI
 */
@Slf4j
public class HttpServer implements Runnable {

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;

    private static HttpServer INSTANCE = null;
    @Getter
    private WebSocket webSocketHandler;


    public static HttpServer getInstance() {
        if ( INSTANCE == null ) {
            throw new GenericRuntimeException( "HttpServer is not yet created." );
        }
        return INSTANCE;
    }


    public static final ObjectMapper mapper = new ObjectMapper() {
        {
            setSerializationInclusion( JsonInclude.Include.NON_NULL );
            configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false );
            setVisibility( getSerializationConfig().getDefaultVisibilityChecker()
                    .withIsGetterVisibility( Visibility.NONE )
                    .withGetterVisibility( Visibility.NONE )
                    .withSetterVisibility( Visibility.NONE ) );
            configure( SerializationFeature.FAIL_ON_EMPTY_BEANS, false );
            writerWithDefaultPrettyPrinter();
        }
    };

    @Getter
    private final Javalin server = Javalin.create( config -> {
        config.jsonMapper( new JavalinJackson( mapper ) );
        config.enableCorsForAllOrigins();
        config.addStaticFiles( staticFileConfig -> staticFileConfig.directory = "webapp/" );
    } ).start( RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
    private Crud crud;


    public HttpServer( TransactionManager manager, final Authenticator authenticator ) {
        this.transactionManager = manager;
        this.authenticator = authenticator;
    }


    @Override
    public void run() {
        this.crud = new Crud( this.transactionManager );

        this.webSocketHandler = new WebSocket( crud );
        webSockets( server, this.webSocketHandler );

        // Get modified index.html
        server.get( "/", ctx -> {
            ctx.contentType( "text/html" );

            try ( InputStream stream = this.getClass().getClassLoader().getResource( "index/index.html" ).openStream() ) {
                ctx.result( streamToString( stream ) );
            } catch ( NullPointerException e ) {
                ctx.result( "Error: Could not find index.html" );
            }
        } );

        attachRoutes( server, crud );

        StatusNotificationService.printInfo(
                String.format( "Polypheny-UI started and is listening on port %d.", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() ) );

        attachExceptions( server );
        INSTANCE = this;
    }


    public Consumer3<String, Consumer<Context>, HandlerType> getRouteRegisterer() {
        return this::addSerializedRoute;
    }


    public BiConsumer<String, Consumer<WsConfig>> getWebSocketRegisterer() {
        return this::addWebsocketRoute;
    }


    private void attachExceptions( Javalin server ) {
        server.exception( SocketException.class, ( e, ctx ) -> {
            ctx.status( 400 ).result( "Error: Could not determine IP address." );
        } );

        defaultException( IOException.class, server );
    }


    private void defaultException( Class<? extends Exception> exceptionClass, Javalin server ) {
        server.exception( exceptionClass, ( e, ctx ) -> {
            ctx.status( 400 ).json( RelationalResult.builder().error( e.getMessage() ).build() );
        } );
    }


    /**
     * Defines the routes for this server
     */
    private void attachRoutes( Javalin webuiServer, Crud crud ) {
        attachCatalogMetaRoutes( webuiServer, crud );

        attachPartnerRoutes( webuiServer, crud );

        attachStatisticRoutes( webuiServer, crud );

        attachPluginRoutes( webuiServer, crud );

        attachDockerRoutes( webuiServer, crud );

        webuiServer.post( "/anyQuery", LanguageCrud::anyQuery );

        webuiServer.post( "/insertTuple", crud::insertTuple );

        webuiServer.post( "/deleteTuple", crud::deleteTuple );

        webuiServer.post( "/updateTuple", crud::updateTuple );

        webuiServer.post( "/batchUpdate", crud::batchUpdate );

        webuiServer.post( "/getColumns", crud::getColumns );

        webuiServer.post( "/getDataSourceColumns", crud::getDataSourceColumns );

        webuiServer.post( "/getAvailableSourceColumns", crud::getAvailableSourceColumns );

        webuiServer.post( "/updateColumn", crud::updateColumn );

        webuiServer.post( "/getMaterializedInfo", crud::getMaterializedInfo );

        webuiServer.post( "/updateMaterialized", crud::updateMaterialized );

        webuiServer.post( "/createColumn", crud::addColumn );

        webuiServer.post( "/dropColumn", crud::dropColumn );

        webuiServer.post( "/getEntities", crud::getEntities );

        webuiServer.post( "/renameTable", crud::renameTable );

        webuiServer.post( "/dropTruncateTable", crud::dropTruncateTable );

        webuiServer.post( "/createTable", crud::createTable );

        webuiServer.get( "/getGeneratedNames", crud::getGeneratedNames );

        webuiServer.post( "/getConstraints", crud::getConstraints );

        webuiServer.post( "/dropConstraint", crud::dropConstraint );

        webuiServer.post( "/createPrimaryKey", crud::addPrimaryKey );

        webuiServer.post( "/createUniqueConstraint", crud::addUniqueConstraint );

        webuiServer.post( "/getIndexes", crud::getIndexes );

        webuiServer.post( "/dropIndex", crud::dropIndex );

        webuiServer.post( "/getUml", crud::getUml );

        webuiServer.post( "/createForeignKey", crud::addForeignKey );

        webuiServer.post( "/createIndex", crud::createIndex );

        webuiServer.post( "/getUnderlyingTable", crud::getUnderlyingTable );

        webuiServer.post( "/getPlacements", crud::getPlacements );

        webuiServer.post( "/getGraphPlacements", crud.languageCrud::getGraphPlacements );

        webuiServer.post( "/getFixedFields", crud.languageCrud::getFixedFields );

        webuiServer.post( "/getCollectionPlacements", crud.languageCrud::getCollectionPlacements );

        webuiServer.post( "/addDropPlacement", crud::addDropPlacement );

        webuiServer.get( "/getPartitionTypes", crud::getPartitionTypes );

        webuiServer.post( "/getPartitionFunctionModel", crud::getPartitionFunctionModel );

        webuiServer.post( "/partitionTable", crud::partitionTable );

        webuiServer.post( "/mergePartitions", crud::mergePartitions );

        webuiServer.post( "/modifyPartitions", crud::modifyPartitions );

        webuiServer.post( "/getAnalyzerPage", crud::getAnalyzerPage );

        webuiServer.post( "/namespaceRequest", crud::namespaceRequest );

        webuiServer.get( "/getTypeInfo", crud::getTypeInfo );

        webuiServer.get( "/getForeignKeyActions", crud::getForeignKeyActions );

        webuiServer.get( "/getStores", crud::getStores );

        webuiServer.get( "/getSources", crud::getSources );

        webuiServer.post( "/getAvailableStoresForIndexes", crud::getAvailableStoresForIndexes );

        webuiServer.post( "/removeAdapter", crud::removeAdapter );

        webuiServer.post( "/updateAdapterSettings", crud::updateAdapterSettings );

        webuiServer.get( "/getAvailableStores", crud::getAvailableStores );

        webuiServer.get( "/getAvailableSources", crud::getAvailableSources );

        webuiServer.post( "/createAdapter", crud::addAdapter );

        webuiServer.post( "/pathAccess", crud::startAccessRequest );

        webuiServer.get( "/getQueryInterfaces", crud::getQueryInterfaces );

        webuiServer.get( "/getAvailableQueryInterfaces", crud::getAvailableQueryInterfaces );

        webuiServer.post( "/createQueryInterface", crud::addQueryInterface );

        webuiServer.post( "/updateQueryInterfaceSettings", crud::updateQueryInterfaceSettings );

        webuiServer.post( "/removeQueryInterface", crud::removeQueryInterface );

        webuiServer.get( "/getFile/{file}", crud::getFile );

        webuiServer.get( "/getDocumentDatabases", crud.languageCrud::getDocumentDatabases );

        webuiServer.get( "/product", ctx -> ctx.result( "Polypheny-DB" ) );

    }


    private static void attachDockerRoutes( Javalin webuiServer, Crud crud ) {
        webuiServer.post( "/addDockerInstance", crud::addDockerInstance );

        webuiServer.post( "/testDockerInstance/{dockerId}", crud::testDockerInstance );

        webuiServer.get( "/getDockerInstance/{dockerId}", crud::getDockerInstance );

        webuiServer.get( "/getDockerInstances", crud::getDockerInstances );

        webuiServer.post( "/updateDockerInstance", crud::updateDockerInstance );

        webuiServer.post( "/reconnectToDockerInstance", crud::reconnectToDockerInstance );

        webuiServer.post( "/removeDockerInstance", crud::removeDockerInstance );

        webuiServer.get( "/getAutoDockerStatus", crud::getAutoDockerStatus );

        webuiServer.post( "/doAutoHandshake", crud::doAutoHandshake );

        webuiServer.post( "/startHandshake", crud::startHandshake );

        webuiServer.get( "/getHandshake/{hostname}", crud::getHandshake );

        webuiServer.post( "/cancelHandshake", crud::cancelHandshake );

        webuiServer.get( "/getDockerSettings", crud::getDockerSettings );

        webuiServer.post( "/changeDockerSettings", crud::changeDockerSettings );
    }


    private static void attachPluginRoutes( Javalin webuiServer, Crud crud ) {
        webuiServer.post( "/loadPlugins", crud::loadPlugins );

        webuiServer.post( "/unloadPlugin", crud::unloadPlugin );

        webuiServer.get( "/getAvailablePlugins", crud::getAvailablePlugins );
    }


    private static void attachStatisticRoutes( Javalin webuiServer, Crud crud ) {
        webuiServer.post( "/allStatistics", crud.statisticCrud::getStatistics );

        webuiServer.post( "/getTableStatistics", crud.statisticCrud::getTableStatistics );

        webuiServer.post( "/getDashboardInformation", crud.statisticCrud::getDashboardInformation );

        webuiServer.post( "/getDashboardDiagram", crud.statisticCrud::getDashboardDiagram );
    }


    private static void attachPartnerRoutes( Javalin webuiServer, Crud crud ) {
        webuiServer.get( "/auth/deregister", crud.authCrud::deregister );
    }


    private static void attachCatalogMetaRoutes( Javalin webuiServer, Crud crud ) {
        webuiServer.post( "/getSchemaTree", crud.catalogCrud::getSchemaTree );

        webuiServer.get( "/getSnapshot", crud.catalogCrud::getSnapshot );

        webuiServer.get( "/getTypeSchemas", crud.catalogCrud::getTypeNamespaces );

        webuiServer.post( "/getNamespaces", crud.catalogCrud::getNamespaces );

        webuiServer.get( "/getCurrentSnapshot", crud.catalogCrud::getCurrentSnapshot );

        webuiServer.get( "/getAssetsDefinition", crud.catalogCrud::getAssetsDefinition );

        webuiServer.get( "/getAlgebraNodes", crud.catalogCrud::getAlgebraNodes );
    }


    public void addSerializedRoute( String route, BiConsumer<Context, Crud> action, HandlerType type ) {
        addSerializedRoute( route, r -> action.accept( r, crud ), type );
    }


    public void addSerializedRoute( String route, Consumer<Context> action, HandlerType type ) {
        log.debug( "Added route: {}", route );
        switch ( type ) {
            case GET:
                server.get( route, action::accept );
                break;
            case POST:
                server.post( route, action::accept );
                break;
            case PUT:
                server.put( route, action::accept );
                break;
            case DELETE:
                server.delete( route, action::accept );
                break;
            case PATCH:
                server.patch( route, action::accept );
                break;
        }
    }


    public <T> void addRoute( String route, BiFunction<T, Crud, Object> action, Class<T> requestClass, HandlerType type ) {
        BiConsumer<Context, Crud> func = ( r, c ) -> r.json( action.apply( r.bodyAsClass( requestClass ), crud ) );
        addSerializedRoute( route, func, type );
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


    public void addWebsocketRoute( String route, Consumer<WsConfig> handler ) {
        server.ws( route, handler );
    }


    public void removeRoute( String route, HandlerType type ) {
        addRoute( route, ( ctx, crud ) -> null, Object.class, type );
    }


}
