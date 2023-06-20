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

package org.polypheny.db.jupyter;

import com.google.gson.JsonObject;
import io.javalin.http.Context;
import java.io.File;

import java.security.SecureRandom;
import java.util.Arrays;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.jupyter.model.JupyterSessionManager;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.HttpServer.HandlerType;

@Slf4j
public class JupyterPlugin extends Plugin {

    private String host;
    private String token;
    private final int dockerInstanceId = 0;
    private final int adapterId = 123456;
    private final int PORT = 12345;
    private final String UNIQUE_NAME = "jupyter-container";
    public static final String SERVER_TARGET_PATH = "/home/jovyan/notebooks";
    private final String REST_PATH = "/notebooks";
    private final String WEBSOCKET_PATH = REST_PATH + "/webSocket";
    private DockerManager.Container container;
    private File rootPath;
    private JupyterProxy proxy;

    public static TransactionManager transactionManager;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public JupyterPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        log.info( "Jupyter Plugin was started!" );
        TransactionExtension.REGISTER.add( new JupyterStarter( this ) );
    }


    @Override
    public void stop() {
        JupyterSessionManager.getInstance().reset();
        stopContainer();
        log.info( "Jupyter Plugin was stopped!" );
    }


    public void onContainerRunning() {
        proxy = new JupyterProxy( new JupyterClient( token, host, PORT ) );
        registerEndpoints();
    }


    private void startContainer() {
        token = generateToken();
        log.error( "Token: {}", token );
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        rootPath = fileSystemManager.registerNewFolder( "data/jupyter" );

        log.info( "trying to start jupyter container..." );
        DockerManager.Container container = new DockerManager.ContainerBuilder( adapterId, "polypheny/jupyter-server", UNIQUE_NAME, dockerInstanceId )
                .withMappedPort( 8888, PORT )
                .withBindMount( rootPath.getAbsolutePath(), SERVER_TARGET_PATH )
                .withInitCommands( Arrays.asList( "start-notebook.sh", "--IdentityProvider.token=" + token ) )
                .withReadyTest( this::testConnection, 20000 )
                .build();
        this.container = container;
        DockerManager.getInstance().initialize( container ).start();
        this.host = container.getIpAddress();
        log.info( "Jupyter container started with ip " + container.getIpAddress() );
    }


    private void stopContainer() {
        DockerInstance.getInstance().destroyAll( adapterId );
    }


    public void restartContainer( Context ctx, Crud crud ) {
        stopContainer();
        JupyterSessionManager.getInstance().reset();
        log.warn( "Restarting Jupyter Server container" );
        startContainer();
        proxy.setClient( new JupyterClient( token, host, PORT ) );
        ctx.status( 200 ).json( "restart ok" );
    }


    public void containerStatus( Context ctx, Crud crud ) {
        JsonObject status = new JsonObject();
        status.addProperty( "status", container.getStatus().toString() );
        status.addProperty( "host", container.getHost() );
        status.addProperty( "ip", container.getIpAddress() );
        ctx.status( 200 ).json( status );
    }


    private void registerEndpoints() {
        HttpServer server = HttpServer.getInstance();

        server.addWebsocket( WEBSOCKET_PATH + "/{kernelId}", new JupyterWebSocket() );

        server.addSerializedRoute( REST_PATH + "/contents/<path>", proxy::contents, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/sessions", proxy::sessions, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxy::session, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/kernels", proxy::kernels, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/kernelspecs", proxy::kernelspecs, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/file/<path>", proxy::file, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/container/status", this::containerStatus, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/status", proxy::connectionStatus, HandlerType.GET );

        server.addSerializedRoute( REST_PATH + "/contents/<parentPath>", proxy::createFile, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/sessions", proxy::createSession, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/kernels/{kernelId}/interrupt", proxy::interruptKernel, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/kernels/{kernelId}/restart", proxy::restartKernel, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/container/restart", this::restartContainer, HandlerType.POST );

        server.addSerializedRoute( REST_PATH + "/contents/<filePath>", proxy::moveFile, HandlerType.PATCH );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxy::patchSession, HandlerType.PATCH );

        server.addSerializedRoute( REST_PATH + "/contents/<filePath>", proxy::uploadFile, HandlerType.PUT );

        server.addSerializedRoute( REST_PATH + "/contents/<filePath>", proxy::deleteFile, HandlerType.DELETE );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxy::deleteSession, HandlerType.DELETE );
    }


    private boolean testConnection() {
        if ( container == null ) {
            return false;
        }
        container.updateIpAddress();
        host = container.getIpAddress();
        if ( host == null ) {
            return false;
        }
        JupyterClient client = new JupyterClient( token, host, PORT );
        return client.testConnection();
    }


    private String generateToken() {
        SecureRandom random = new SecureRandom();
        byte[] tokenBytes = new byte[24];
        random.nextBytes( tokenBytes );
        StringBuilder token = new StringBuilder();
        for ( byte i : tokenBytes ) {
            token.append( String.format( "%02x", i ) );
        }
        return token.toString();
    }


    @Extension
    public static class JupyterStarter implements TransactionExtension {

        private final JupyterPlugin plugin;


        public JupyterStarter( JupyterPlugin plugin ) {
            this.plugin = plugin;
        }


        @Override
        public void initExtension( TransactionManager manager, Authenticator authenticator ) {
            log.info( "Initializing Jupyter Extension" );
            plugin.startContainer();
            plugin.onContainerRunning();
            JupyterPlugin.transactionManager = manager;
        }

    }

}
