/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.notebooks;

import io.javalin.http.Context;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.ConfigString;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.notebooks.model.JupyterSessionManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.HttpServer;

@Slf4j
public class NotebooksPlugin extends PolyPlugin {

    private final static String CONFIG_CONTAINER_KEY = "notebooks/jupyter_container_id";
    private final static String CONFIG_TOKEN_KEY = "notebooks/jupyter_token";
    private static final int JUPYTER_PORT = 8888;
    private String token;
    private DockerContainer container;
    private JupyterProxy proxy;

    private final JupyterContentServer fs = new JupyterContentServer();

    private boolean pluginLoaded = false;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public NotebooksPlugin( PluginContext context ) {
        super( context );
        Config tokenConfig = new ConfigString( CONFIG_TOKEN_KEY, "" );
        ConfigManager.getInstance().registerConfig( tokenConfig );
        Config containerIdConfig = new ConfigString( CONFIG_CONTAINER_KEY, "" );
        ConfigManager.getInstance().registerConfig( containerIdConfig );
    }


    @Override
    public void afterTransactionInit( TransactionManager manager ) {
        JupyterSessionManager.getInstance().setTransactionManager( manager );
        registerEndpoints();
        checkContainer();
        log.info( "Notebooks plugin was started!" );
        pluginLoaded = true;
    }


    @Override
    public void stop() {
        JupyterSessionManager.getInstance().reset();
        stopContainer();
        log.info( "Notebooks plugin was stopped!" );
        pluginLoaded = false;
    }


    private boolean checkContainer() {
        String containerUUID = ConfigManager.getInstance().getConfig( CONFIG_CONTAINER_KEY ).getString();
        if ( containerUUID.isEmpty() ) {
            return false;
        }

        Optional<DockerContainer> maybeContainer = DockerContainer.getContainerByUUID( ConfigManager.getInstance().getConfig( CONFIG_CONTAINER_KEY ).getString() );
        if ( maybeContainer.isPresent() ) {
            this.container = maybeContainer.get();
            onContainerRunning();
            return true;
        }
        return false;
    }


    private void createContainerRequest( Context ctx ) {
        int dockerId = Integer.parseInt( Objects.requireNonNull( ctx.queryParam( "dockerInstance" ) ) );
        DockerInstance dockerInstance = DockerManager.getInstance().getInstanceById( dockerId ).orElseThrow();
        if ( createContainer( dockerInstance ) ) {
            ctx.status( 200 );
        } else {
            ctx.status( 500 );
        }
    }


    private void destroyContainerRequest( Context ctx ) {
        stopContainer();
    }


    private void getDockerInstances( Context ctx ) {
        List<Map<String, Object>> result = DockerManager.getInstance().getDockerInstances().values().stream().filter( DockerInstance::isConnected ).map( DockerInstance::getMap ).toList();
        ctx.status( 200 ).json( result );
    }


    /**
     * Deploys the docker container with polypheny-jupyter-server image.
     * For storing the notebooks in the Polypheny Home directory, a bind mount is used.
     *
     * @return true if the jupyter server was successfully deployed, false otherwise
     */
    private boolean createContainer( DockerInstance dockerInstance ) {
        token = ConfigManager.getInstance().getConfig( CONFIG_TOKEN_KEY ).getString();
        if ( token.isEmpty() ) {
            token = generateToken();
            ConfigManager.getInstance().getConfig( CONFIG_TOKEN_KEY ).setString( token );
        }
        try {
            Optional<DockerContainer> maybeContainer = DockerContainer.getContainerByUUID( ConfigManager.getInstance().getConfig( CONFIG_CONTAINER_KEY ).getString() );
            if ( maybeContainer.isEmpty() ) {
                this.container = dockerInstance.newBuilder( "polypheny/polypheny-jupyter-server", "jupyter-container" )
                        .withCommand( Arrays.asList( "start-notebook.sh", "--IdentityProvider.token=" + token ) )
                        .createAndStart();

                if ( !this.container.waitTillStarted( this::testConnection, 20000 ) ) {
                    this.container.destroy();
                    throw new GenericRuntimeException( "Failed to start Jupyter Server container" );
                }
                ConfigManager.getInstance().getConfig( CONFIG_CONTAINER_KEY ).setString( this.container.getContainerId() );
                log.info( "Jupyter Server container has been deployed" );
            } else {
                this.container = maybeContainer.get();
            }
            onContainerRunning();
            return true;
        } catch ( Exception e ) {
            log.warn( "Unable to deploy Jupyter Server container", e );
            return false;
        }
    }


    public void onContainerRunning() {
        HostAndPort hostAndPort = container.connectToContainer( JUPYTER_PORT );
        proxy = new JupyterProxy( new JupyterClient( token, hostAndPort.host(), hostAndPort.port() ) );
    }


    private void stopContainer() {
        if ( container != null ) {
            ConfigManager.getInstance().getConfig( CONFIG_CONTAINER_KEY ).setString( "" );
            ConfigManager.getInstance().getConfig( CONFIG_TOKEN_KEY ).setString( "" );
            container.destroy();
            proxy = null;
        }
    }


    public void restartContainer( Context ctx ) {
        if ( container == null ) {
            ctx.status( 500 ).json( "cannot restart container: no container present" );
        }
        DockerInstance dockerInstance = DockerManager.getInstance().getInstanceForContainer( container.getContainerId() ).get();
        stopContainer();
        JupyterSessionManager.getInstance().reset();
        log.info( "Restarting Jupyter container..." );
        if ( createContainer( dockerInstance ) ) {
            HostAndPort hostAndPort = container.connectToContainer( JUPYTER_PORT );
            proxy.setClient( new JupyterClient( token, hostAndPort.host(), hostAndPort.port() ) );
            ctx.status( 200 ).json( "restart ok" );
        } else {
            container = null;
            ctx.status( 500 ).json( "failed to restart container" );
        }
    }


    public void pluginStatus( Context ctx, Crud crud ) {
        if ( pluginLoaded ) {
            ctx.status( 200 );
        } else {
            ctx.status( 500 );
        }
    }


    private Consumer<Context> proxyOrError( Function<JupyterProxy, Consumer<Context>> endpoint ) {
        return ctx -> {
            if ( proxy != null ) {
                endpoint.apply( proxy ).accept( ctx );
            } else {
                ctx.status( 500 );
                ctx.json( "Not connected to Jupyter" );
            }
        };
    }


    private Consumer<Context> proxyOrEmpty( Function<JupyterProxy, Consumer<Context>> endpoint ) {
        return ctx -> {
            if ( proxy != null ) {
                endpoint.apply( proxy ).accept( ctx );
            } else {
                ctx.status( 200 );
                ctx.json( List.of() );
            }
        };
    }


    /**
     * Adds all REST and websocket endpoints required for the notebook functionality to the HttpServer.
     */
    private void registerEndpoints() {
        HttpServer server = HttpServer.getInstance();
        final String PATH = "/notebooks";

        server.addWebsocketRoute( PATH + "/webSocket/{kernelId}", new JupyterWebSocket() );

        server.addSerializedRoute( PATH + "/contents/<path>", fs::contents, HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions", proxyOrEmpty( proxy -> proxy::sessions ), HandlerType.GET );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", proxyOrError( proxy -> proxy::session ), HandlerType.GET );
        server.addSerializedRoute( PATH + "/kernels", proxyOrEmpty( proxy -> proxy::kernels ), HandlerType.GET );
        server.addSerializedRoute( PATH + "/kernelspecs", ctx -> {
            if ( proxy != null ) {
                proxy.kernelspecs( ctx );
            } else {
                ctx.status( 200 ).json( Map.of( "default", "", "kernelspecs", Map.of() ) );
            }
        }, HandlerType.GET );
        server.addSerializedRoute( PATH + "/file/<path>", fs::file, HandlerType.GET );
        server.addSerializedRoute( PATH + "/plugin/status", this::pluginStatus, HandlerType.GET );
        server.addSerializedRoute( PATH + "/status", ctx -> {
            if ( proxy != null ) {
                proxy.connectionStatus( ctx );
            } else {
                ctx.status( 200 ).result( "null" );
            }
        }, HandlerType.GET );
        server.addSerializedRoute( PATH + "/export/<path>", fs::export, HandlerType.GET );
        server.addSerializedRoute( PATH + "/connections", proxyOrEmpty( proxy -> proxy::openConnections ), HandlerType.GET );
        server.addSerializedRoute( PATH + "/container/getDockerInstances", this::getDockerInstances, HandlerType.GET );

        server.addSerializedRoute( PATH + "/contents/<parentPath>", fs::createFile, HandlerType.POST );
        server.addSerializedRoute( PATH + "/sessions", proxyOrError( proxy -> proxy::createSession ), HandlerType.POST );
        server.addSerializedRoute( PATH + "/kernels/{kernelId}/interrupt", proxyOrError( proxy -> proxy::interruptKernel ), HandlerType.POST );
        server.addSerializedRoute( PATH + "/kernels/{kernelId}/restart", proxyOrError( proxy -> proxy::restartKernel ), HandlerType.POST );
        server.addSerializedRoute( PATH + "/container/restart", this::restartContainer, HandlerType.POST );
        server.addSerializedRoute( PATH + "/container/create", this::createContainerRequest, HandlerType.POST );
        server.addSerializedRoute( PATH + "/container/destroy", this::destroyContainerRequest, HandlerType.POST );

        server.addSerializedRoute( PATH + "/contents/<filePath>", fs::moveFile, HandlerType.PATCH );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", proxyOrError( proxy -> proxy::patchSession ), HandlerType.PATCH );

        server.addSerializedRoute( PATH + "/contents/<filePath>", fs::uploadFile, HandlerType.PUT );

        server.addSerializedRoute( PATH + "/contents/<filePath>", fs::deleteFile, HandlerType.DELETE );
        server.addSerializedRoute( PATH + "/sessions/{sessionId}", proxyOrError( proxy -> proxy::deleteSession ), HandlerType.DELETE );
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
        HostAndPort hostAndPort = container.connectToContainer( JUPYTER_PORT );
        JupyterClient client = new JupyterClient( token, hostAndPort.host(), hostAndPort.port() );
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

}
