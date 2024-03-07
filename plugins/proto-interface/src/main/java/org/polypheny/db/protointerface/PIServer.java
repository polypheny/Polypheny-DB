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

package org.polypheny.db.protointerface;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import org.polypheny.db.util.Util;

@Slf4j
public class PIServer {

    private final List<ServerSocket> servers = new ArrayList<>();
    private final int port = 20590; // TODO: configurable


    public PIServer( ClientManager clientManager ) throws IOException {
        startServer( port, clientManager, PlainTransport::accept );

        // used to handle unexpected shutdown of the JVM
        Runtime.getRuntime().addShutdownHook( getShutdownHook() );
    }


    void startServer( int port, ClientManager clientManager, Function<Socket, Transport> createConnection ) throws IOException {
        ServerSocket server = new ServerSocket( port, 16, InetAddress.getLoopbackAddress() );
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface server started on port {}", port );
        }
        Thread acceptor = new Thread( () -> acceptLoop( server, clientManager, createConnection ) );
        acceptor.start();
        servers.add( server );
    }


    void acceptLoop( ServerSocket server, ClientManager clientManager, Function<Socket, Transport> createConnection ) {
        while ( true ) {
            try {
                Socket s = server.accept();
                Thread t = new Thread( () -> PIService.acceptConnection( createConnection.apply( s ), clientManager ) );
                t.start();
            } catch ( IOException e ) {
                log.error( e.getMessage() );
                break;
            } catch ( Throwable t ) {
                // For debug purposes
                log.error( "Unhandled exception", t );
                break;
            }
        }
    }


    public void shutdown() throws InterruptedException, IOException {
        servers.forEach( Util::closeNoThrow );
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface server shutdown requested" );
        }
    }


    private Thread getShutdownHook() {
        return new Thread( () -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println( "shutting down proto interface since JVM is shutting down" );
            try {
                shutdown();
            } catch ( IOException | InterruptedException e ) {
                e.printStackTrace( System.err );
            }
            System.err.println( "server shut down" );
        } );
    }

}
