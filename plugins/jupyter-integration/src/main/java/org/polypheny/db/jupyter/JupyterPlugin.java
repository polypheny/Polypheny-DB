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

import io.javalin.http.Context;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
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

import java.io.File;
import java.security.SecureRandom;
import java.util.Arrays;

@Slf4j
public class JupyterPlugin extends Plugin {

    private String host;
    private String token;
    private final int port = 14141;
    public static final String SERVER_TARGET_PATH = "/home/jovyan/notebooks";
    private DockerManager.Container container;
    private JupyterProxy proxy;
    private boolean pluginLoaded = false;


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
        proxy = new JupyterProxy( new JupyterClient( token, host, port ) );
        registerEndpoints();
        pluginLoaded = true;
    }


    private boolean startContainer() {
        token = generateToken();
        log.trace( "Token: {}", token );
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        File rootPath = fileSystemManager.registerNewFolder( "data/jupyter" );
        try {
            int adapterId = -1;
            DockerManager.Container container = new DockerManager.ContainerBuilder( adapterId, "polypheny/polypheny-jupyter-server", "jupyter-container", 0 )
                    .withMappedPort( 8888, port )
                    .withBindMount( rootPath.getAbsolutePath(), SERVER_TARGET_PATH )
                    .withInitCommands( Arrays.asList( "start-notebook.sh", "--IdentityProvider.token=" + token ) )
                    .withReadyTest( this::testConnection, 20000 )
                    .build();
            this.container = container;
            DockerManager.getInstance().initialize( container ).start();
            this.host = container.getIpAddress();
            log.info( "Jupyter container has been deployed." );
            return true;

        } catch ( Exception e ) {
            e.printStackTrace();
            log.warn( "Unable to deploy Jupyter container." );
            return false;
        }
    }


    private void stopContainer() {
        if ( container != null ) {
            DockerInstance.getInstance().destroy( container );
        }
    }


    public void restartContainer( Context ctx, Crud crud ) {
        stopContainer();
        JupyterSessionManager.getInstance().reset();
        log.info( "Restarting Jupyter container..." );
        if ( startContainer() ) {
            proxy.setClient( new JupyterClient( token, host, port ) );
            ctx.status( 200 ).json( "restart ok" );
        } else {
            container = null;
            pluginLoaded = false;
            ctx.status( 500 ).json( "failed to restart container" );
        }
    }


    public void pluginStatus( Context ctx, Crud crud ) {
        if ( pluginLoaded ) {
            ctx.status( 200 ).json( "plugin is loaded correctly" );
        } else {
            ctx.status( 500 ).json( "plugin is not loaded correctly" );
        }
    }


    private void registerEndpoints() {
        HttpServer server = HttpServer.getInstance();
        final String REST_PATH = "/notebooks";

        server.addWebsocket( REST_PATH + "/webSocket/{kernelId}", new JupyterWebSocket() );

        server.addSerializedRoute( REST_PATH + "/contents/<path>", proxy::contents, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/sessions", proxy::sessions, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxy::session, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/kernels", proxy::kernels, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/kernelspecs", proxy::kernelspecs, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/file/<path>", proxy::file, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/plugin/status", this::pluginStatus, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/status", proxy::connectionStatus, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/export/<path>", proxy::export, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/connections", proxy::openConnections, HandlerType.GET );

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
        JupyterClient client = new JupyterClient( token, host, port );
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
            if ( plugin.startContainer() ) {
                JupyterSessionManager.getInstance().setTransactionManager( manager );
                plugin.onContainerRunning();
            }
        }

    }

}
