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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.File;

import java.security.SecureRandom;
import java.util.Arrays;
import lombok.Setter;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.jupyter.model.JupyterKernel;
import org.polypheny.db.jupyter.model.JupyterKernelSubscriber;
import org.polypheny.db.jupyter.model.JupyterSessionManager;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class JupyterPlugin extends Plugin {

    private String host;
    private String token;
    private int dockerInstanceId = 0;
    private int adapterId = 123456;
    private final String UNIQUE_NAME = "jupyter-container";
    private final String SERVER_TARGET_PATH = "/home/jovyan/work";
    private DockerManager.Container container;
    private File rootPath;
    @Setter
    private JupyterClient connection;


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
        stopContainer();
        log.info( "Jupyter Plugin was stopped!" );
    }


    private void startContainer() {
        token = generateToken();
        log.error( "Token: {}", token );
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        rootPath = fileSystemManager.registerNewFolder( "data/jupyter" );

        log.info( "trying to start jupyter container..." );
        DockerManager.Container container = new DockerManager.ContainerBuilder( adapterId, "polypheny/jupyter-server", UNIQUE_NAME, dockerInstanceId )
                .withMappedPort( 8888, 8888 )
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


    private boolean testConnection() {
        JupyterClient client = null;
        if ( container == null ) {
            return false;
        }
        container.updateIpAddress();
        host = container.getIpAddress();
        if ( host == null ) {
            return false;
        }
        client = new JupyterClient( token );
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
            JupyterClient client = new JupyterClient( plugin.token );
            plugin.setConnection( client );
            log.error( "Testing the functionality of JuypterClient..." );

            JupyterSessionManager jsm = JupyterSessionManager.getInstance();
            client.getKernelspecs();
            String sessionId = client.createSession( jsm.getDefaultKernel(), "test.ipynb", "work/test.ipynb" );
            client.getSessions();
            log.error( jsm.getOverview() );


            // Testing the interface
            JupyterKernelSubscriber sub = new JupyterKernelSubscriber() {
                @Override
                public void onText( CharSequence data ) {
                    Gson gson = new Gson();
                    JsonObject json = gson.fromJson( data.toString(), JsonObject.class );
                    String msgType = json.get( "msg_type" ).getAsString();
                    if ( msgType.equals( "stream" ) ) {
                        log.error( "Output ({}): {}",
                                json.get( "content" ).getAsJsonObject().get( "name" ).getAsString(),
                                json.get( "content" ).getAsJsonObject().get( "text" ).getAsString() );
                    }
                }


                @Override
                public void onClose() {

                }
            };

            JupyterKernel kernel = jsm.getKernelFromSession( sessionId );
            kernel.subscribe( sub );
            kernel.execute( "print('The answer is:', 6 * 7)" );
        }

    }

}
