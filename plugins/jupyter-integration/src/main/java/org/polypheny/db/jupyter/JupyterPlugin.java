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
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
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

    private String token;
    private final int port = 8888;
    public static final String SERVER_TARGET_PATH = "/home/jovyan/notebooks";  // notebook storage location inside container
    private DockerContainer container;
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


    /**
     * Deploys the docker container with polypheny-jupyter-server image.
     * For storing the notebooks in the Polypheny Home directory, a bind mount is used.
     *
     * @return true if the jupyter server was successfully deployed, false otherwise
     */
    private boolean startContainer() {
        token = generateToken();
        log.trace( "Token: {}", token );
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        File rootPath = fileSystemManager.registerNewFolder( "data/jupyter" );
        try {
            // Just take the first docker instance
            DockerInstance dockerInstance = DockerManager.getInstance().getDockerInstances().values().stream().findFirst().get();
            this.container = dockerInstance.newBuilder( "polypheny/polypheny-jupyter-server", "jupyter-container" )
                    //.withBindMount( rootPath.getAbsolutePath(), SERVER_TARGET_PATH )
                    .withCommand( Arrays.asList( "start-notebook.sh", "--IdentityProvider.token=" + token ) )
                    .createAndStart();

            if ( !container.waitTillStarted( this::testConnection, 20000 ) ) {
                container.destroy();
                throw new RuntimeException( "Failed to start jupyter container" );
            }
            log.info( "Jupyter container has been deployed." );
            return true;

        } catch ( Exception e ) {
            e.printStackTrace();
            log.warn( "Unable to deploy Jupyter container." );
            return false;
        }
    }


    public void onContainerRunning() {
        HostAndPort hostAndPort = container.connectToContainer( port );
        proxy = new JupyterProxy( new JupyterClient( token, hostAndPort.getHost(), hostAndPort.getPort() ) );
        registerEndpoints();
        pluginLoaded = true;
    }


    private void stopContainer() {
        if ( container != null ) {
            container.destroy();
        }
    }


    public void restartContainer( Context ctx, Crud crud ) {
        stopContainer();
        JupyterSessionManager.getInstance().reset();
        log.info( "Restarting Jupyter container..." );
        if ( startContainer() ) {
            HostAndPort hostAndPort = container.connectToContainer( port );
            proxy.setClient( new JupyterClient( token, hostAndPort.getHost(), hostAndPort.getPort() ) );
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


    /**
     * Adds all REST and websocket endpoints required for the notebook functionality to the HttpServer.
     */
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


    /**
     * Test connection to the jupyter server
     *
     * @return true, if the container has been deployed and the jupyter server is reachable, false otherwise
     */
    private boolean testConnection() {
        if ( container == null ) {
            return false;
        }
        HostAndPort hostAndPort = container.connectToContainer( port );
        JupyterClient client = new JupyterClient( token, hostAndPort.getHost(), hostAndPort.getPort() );
        return client.testConnection();
    }


    /**
     * Generate a secure random token for authentication with the jupyter server.
     *
     * @return A 24 byte hexadecimal token formatted as a string
     */
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
