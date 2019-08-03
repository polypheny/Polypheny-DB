/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import com.jakewharton.byteunits.BinaryByteUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class HttpServerDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger( HttpServerDispatcher.class );

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
            throw new Exception( "HTTP Dispatcher was not started on the specified port. It would have listended on " + port );
        }

        if ( LOGGER.isInfoEnabled() ) {
            LOGGER.info( "HTTP Dispatcher is listening on port {}", connector.getLocalPort() );
        } else {
            System.out.println( "HTTP Dispatcher is listening on port " + connector.getLocalPort() );
        }

        String host = connector.getHost();
        if ( null == host ) {
            // "null" means binding to all interfaces, we need to pick one so the client gets a real address and not "0.0.0.0" or similar.
            // TODO: Get that info from the config or try to get a "public" IP address!
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch ( UnknownHostException e ) {
                throw new Exception( e );
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
