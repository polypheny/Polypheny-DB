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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.protointerface.PIPlugin.ProtoInterface;
import org.polypheny.db.protointerface.transport.PlainTransport;
import org.polypheny.db.protointerface.transport.Transport;
import org.polypheny.db.protointerface.transport.UnixTransport;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.Util;

@Slf4j
public class PIServer {

    private final ServerSocketChannel server;
    private final static AtomicLong ID_COUNTER = new AtomicLong();


    private PIServer( ServerSocketChannel server, ClientManager clientManager, String name, Function<SocketChannel, Transport> createTransport ) throws IOException {
        this.server = server;
        log.info( "Proto Interface started and is listening for {} connections on {}", name.toLowerCase(), server.getLocalAddress() );
        Thread acceptor = new Thread( () -> acceptLoop( server, clientManager, name, createTransport ), "ProtoInterface" + name + "Server" );
        acceptor.start();

    }


    static PIServer startServer( ClientManager clientManager, ProtoInterface.Transport transport, Map<String, String> settings ) throws IOException {
        return switch ( transport ) {
            case PLAIN -> new PIServer( createInetServer( Integer.parseInt( settings.get( "port" ) ) ), clientManager, "Plain", PlainTransport::accept );
            case UNIX -> new PIServer( createUnixServer( settings.get( "path" ) ), clientManager, "Unix", UnixTransport::accept );
        };
    }


    private static ServerSocketChannel createInetServer( int port ) throws IOException {
        return ServerSocketChannel.open( StandardProtocolFamily.INET )
                .bind( new InetSocketAddress( Inet4Address.getLoopbackAddress(), port ) );
    }


    private static ServerSocketChannel createUnixServer( String path ) throws IOException {
        File socket;
        if ( !path.endsWith( ".sock" ) ) {
            throw new IOException( "Socket paths must end with .sock" );
        }
        Path p = Paths.get( path );
        if ( p.isAbsolute() ) {
            socket = p.toFile();
        } else {
            if ( p.getNameCount() != 1 ) {
                throw new IOException( "Relative socket paths may not contain directory separators" );
            }
            PolyphenyHomeDirManager phm = PolyphenyHomeDirManager.getInstance();
            socket = phm.registerNewFile( path );
        }
        socket.delete();
        ServerSocketChannel s = ServerSocketChannel.open( StandardProtocolFamily.UNIX )
                .bind( UnixDomainSocketAddress.of( socket.getAbsolutePath() ) );
        socket.setWritable( true, false );
        return s;
    }


    private String getRemoteAddressOrNone( SocketChannel s ) {
        try {
            return s.getRemoteAddress().toString();
        } catch ( IOException ignore ) {
            return "unknown";
        }
    }


    private void acceptConnection( SocketChannel s, String name, long connectionId, Function<SocketChannel, Transport> createTransport, ClientManager clientManager ) {
        try {
            log.info( "accept {} connection with id {} from {}", name.toLowerCase(), connectionId, getRemoteAddressOrNone( s ) );
            PIService.acceptConnection( createTransport.apply( s ), connectionId, clientManager );
        } catch ( GenericRuntimeException e ) {
            if ( e.getCause() instanceof EOFException ) {
                return;
            }
            if ( e.getCause() instanceof IOException ) {
                log.error( "accept {} connection: {}", name, e.getCause().getMessage() );
                return;
            }
            throw e;
        }
    }


    private void acceptLoop( ServerSocketChannel server, ClientManager clientManager, String name, Function<SocketChannel, Transport> createTransport ) {
        while ( true ) {
            try {
                SocketChannel s = server.accept();
                long connectionId = ID_COUNTER.getAndIncrement();
                Thread t = new Thread( () -> acceptConnection( s, name, connectionId, createTransport, clientManager ), String.format( "ProtoInterface" + name + "ClientConnection%d", connectionId ) );
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
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface server shutdown requested" );
        }
        Util.closeNoThrow( server );
    }

}
