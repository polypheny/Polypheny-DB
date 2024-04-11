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

package org.polypheny.db.avatica;


import com.jakewharton.byteunits.BinaryByteUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;


/**
 *
 */
@Slf4j
public class HttpServerDispatcher {

    private int threadPoolMinThreads = 10;
    private int threadPoolMaxThreads = 100;
    private int threadPoolIdleTimeoutMillis = (int) Math.min( TimeUnit.MINUTES.toMillis( 1 ), Integer.MAX_VALUE );

    private Server jettyServer;
    private int port;

    private final int httpMaxAllowedHeaderSize = (int) Math.min( BinaryByteUnit.KIBIBYTES.toBytes( 64 ), Integer.MAX_VALUE );

    private final int connectionIdleTimeoutMillis = (int) Math.min( TimeUnit.MINUTES.toMillis( 1 ), Integer.MAX_VALUE );

    private final AvaticaHandler handler;


    public HttpServerDispatcher( int port, final AvaticaHandler handler ) throws SQLException {
        this.port = port;
        this.handler = handler;
    }


    public void start() throws Exception {
        // TODO: Get the ThreadPool from the ThreadManager component
        final QueuedThreadPool threadPool = new QueuedThreadPool( threadPoolMaxThreads, threadPoolMinThreads, threadPoolIdleTimeoutMillis );
        threadPool.setDaemon( true );
        threadPool.setName( "HttpDispatcher" );

        jettyServer = new Server( threadPool );

        final HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.setRequestHeaderSize( httpMaxAllowedHeaderSize );

        final ServerConnector connector = new ServerConnector( jettyServer, new HttpConnectionFactory( httpConfiguration ) );
        connector.setIdleTimeout( connectionIdleTimeoutMillis );
        connector.setPort( port );

        jettyServer.setConnectors( new Connector[]{ connector } );

        final HandlerList handlers = new HandlerList();
        handlers.setHandlers( new Handler[]{ handler, new DefaultHandler() } );

        jettyServer.setHandler( handlers );

        jettyServer.start();

        if ( port != connector.getLocalPort() ) {
            throw new GenericRuntimeException( "HTTP Dispatcher was not started on the specified port. It would have listended on " + port );
        }

        String host = connector.getHost();
        if ( null == host ) {
            // "null" means binding to all interfaces, we need to pick one so the client gets a real address and not "0.0.0.0" or similar.
            // TODO: Get that info from the config or try to get a "public" IP address!
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch ( UnknownHostException e ) {
                throw new GenericRuntimeException( e );
            }
        }
        handler.setServerRpcMetadata( new Service.RpcMetadataResponse( String.format( Locale.ROOT, "%s:%d", host, port ) ) );
    }


    public void stop() throws Exception {
        jettyServer.stop();
    }


    public void join() throws InterruptedException {
        jettyServer.join();
    }

}
