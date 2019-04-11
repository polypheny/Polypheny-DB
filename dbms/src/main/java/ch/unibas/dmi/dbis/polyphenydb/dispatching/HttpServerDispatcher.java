/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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
 */

package ch.unibas.dmi.dbis.polyphenydb.dispatching;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.DbmsService;
import com.jakewharton.byteunits.BinaryByteUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.HandlerFactory;
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


    protected int threadPoolMinThreads = 10;
    protected int threadPoolMaxThreads = 100;
    protected int threadPoolIdleTimeoutMillis = (int) Math.min( TimeUnit.MINUTES.toMillis( 1 ), Integer.MAX_VALUE );

    protected Server jettyServer;
    protected int port;

    protected final int httpMaxAllowedHeaderSize = (int) Math.min( BinaryByteUnit.KIBIBYTES.toBytes( 64 ), Integer.MAX_VALUE );

    protected final int connectionIdleTimeoutMillis = (int) Math.min( TimeUnit.MINUTES.toMillis( 1 ), Integer.MAX_VALUE );

    protected final AvaticaHandler handler;


    public HttpServerDispatcher( int port ) throws SQLException, ClassNotFoundException {
        this.port = port;

        Class.forName( "ch.unibas.dmi.dbis.polyphenydb.jdbc.EmbeddedDriver" );
        this.handler = new HandlerFactory().getHandler(
                new DbmsService(),
                Serialization.PROTOBUF,
                NoopMetricsSystemConfiguration.getInstance() );
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
        connector.setSoLingerTime( -1 ); // -1 = "disabled"
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
