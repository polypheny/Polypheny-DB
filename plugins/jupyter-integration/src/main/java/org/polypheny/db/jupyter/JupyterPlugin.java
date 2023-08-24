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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.ConfigString;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.jupyter.model.JupyterSessionManager;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.HttpServer.HandlerType;

import java.security.SecureRandom;
import java.util.Arrays;

@Slf4j
public class JupyterPlugin extends Plugin {

    private final static String CONFIG_CONTAINER_KEY = "jupyter/container_id";
    private final static String CONFIG_TOKEN_KEY = "jupyter/token";
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
    public JupyterPlugin( PluginWrapper wrapper ) {
        super( wrapper );
        Config tokenConfig = new ConfigString( CONFIG_TOKEN_KEY, "" );
        ConfigManager.getInstance().registerConfig( tokenConfig );
        Config containerIdConfig = new ConfigString( CONFIG_CONTAINER_KEY, "" );
        ConfigManager.getInstance().registerConfig( containerIdConfig );
    }


    @Override
    public void start() {
        log.info( "Jupyter Plugin was started!" );
        TransactionExtension.REGISTER.add( new JupyterStarter( this ) );
        pluginLoaded = true;
    }


    @Override
    public void stop() {
        JupyterSessionManager.getInstance().reset();
        stopContainer();
        log.info( "Jupyter Plugin was stopped!" );
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
        int dockerId = Integer.parseInt( ctx.queryParam( "dockerInstance" ) );
        DockerInstance dockerInstance = DockerManager.getInstance().getInstanceById( dockerId ).get();
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
        List<Map<String, Object>> result = DockerManager.getInstance().getDockerInstances().values().stream().filter( DockerInstance::isConnected ).map( DockerInstance::getMap ).collect( Collectors.toList() );
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
                    throw new RuntimeException( "Failed to start jupyter container" );
                }
                ConfigManager.getInstance().getConfig( CONFIG_CONTAINER_KEY ).setString( this.container.getContainerId() );
                log.info( "Jupyter container has been deployed." );
            } else {
                this.container = maybeContainer.get();
            }
            onContainerRunning();
            return true;
        } catch ( Exception e ) {
            e.printStackTrace();
            log.warn( "Unable to deploy Jupyter container." );
            return false;
        }
    }


    public void onContainerRunning() {
        HostAndPort hostAndPort = container.connectToContainer( JUPYTER_PORT );
        proxy = new JupyterProxy( new JupyterClient( token, hostAndPort.getHost(), hostAndPort.getPort() ) );
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
            proxy.setClient( new JupyterClient( token, hostAndPort.getHost(), hostAndPort.getPort() ) );
            ctx.status( 200 ).json( "restart ok" );
        } else {
            container = null;
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
        final String REST_PATH = "/notebooks";

        server.addWebsocket( REST_PATH + "/webSocket/{kernelId}", new JupyterWebSocket() );

        server.addSerializedRoute( REST_PATH + "/contents/<path>", fs::contents, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/sessions", proxyOrEmpty( proxy -> proxy::sessions ), HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxyOrError( proxy -> proxy::session ), HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/kernels", proxyOrEmpty( proxy -> proxy::kernels ), HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/kernelspecs", ctx -> {
            if ( proxy != null ) {
                proxy.kernelspecs( ctx );
            } else {
                ctx.status( 200 ).json( Map.of( "default", "", "kernelspecs", Map.of() ) );
            }
        }, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/file/<path>", fs::file, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/plugin/status", this::pluginStatus, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/status", ctx -> {
            if ( proxy != null ) {
                proxy.connectionStatus( ctx );
            } else {
                ctx.status( 200 ).result( "null" );
            }
        }, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/export/<path>", fs::export, HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/connections", proxyOrEmpty( proxy -> proxy::openConnections ), HandlerType.GET );
        server.addSerializedRoute( REST_PATH + "/container/getDockerInstances", this::getDockerInstances, HandlerType.GET );

        server.addSerializedRoute( REST_PATH + "/contents/<parentPath>", fs::createFile, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/sessions", proxyOrError( proxy -> proxy::createSession ), HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/kernels/{kernelId}/interrupt", proxyOrError( proxy -> proxy::interruptKernel ), HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/kernels/{kernelId}/restart", proxyOrError( proxy -> proxy::restartKernel ), HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/container/restart", this::restartContainer, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/container/create", this::createContainerRequest, HandlerType.POST );
        server.addSerializedRoute( REST_PATH + "/container/destroy", this::destroyContainerRequest, HandlerType.POST );

        server.addSerializedRoute( REST_PATH + "/contents/<filePath>", fs::moveFile, HandlerType.PATCH );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxyOrError( proxy -> proxy::patchSession ), HandlerType.PATCH );

        server.addSerializedRoute( REST_PATH + "/contents/<filePath>", fs::uploadFile, HandlerType.PUT );

        server.addSerializedRoute( REST_PATH + "/contents/<filePath>", fs::deleteFile, HandlerType.DELETE );
        server.addSerializedRoute( REST_PATH + "/sessions/{sessionId}", proxyOrError( proxy -> proxy::deleteSession ), HandlerType.DELETE );
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
            JupyterSessionManager.getInstance().setTransactionManager( manager );
            plugin.registerEndpoints();
            plugin.checkContainer();
        }

    }

}
