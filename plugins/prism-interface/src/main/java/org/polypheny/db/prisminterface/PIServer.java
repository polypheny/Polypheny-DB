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

package org.polypheny.db.prisminterface;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prisminterface.PIPlugin.PrismInterface;
import org.polypheny.db.prisminterface.transport.PlainTransport;
import org.polypheny.db.prisminterface.transport.Transport;
import org.polypheny.db.prisminterface.transport.UnixTransport;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.Util;

@Slf4j
class PIServer {

    private final ServerSocketChannel server;
    private final static AtomicLong ID_COUNTER = new AtomicLong();
    private final ServerAndLock fileLock; // Needed for unix servers to keep a lock on the socket
    private final PIRequestReader reader;
    private AtomicBoolean shutdown = new AtomicBoolean( false );


    private PIServer( ServerSocketChannel server, ClientManager clientManager, String name, Function<SocketChannel, Transport> createTransport, @Nullable ServerAndLock fileLock ) throws IOException {
        this.server = server;
        this.fileLock = fileLock;
        String location;
        if ( server.getLocalAddress() instanceof InetSocketAddress ) {
            location = "port " + ((InetSocketAddress) server.getLocalAddress()).getPort();
        } else {
            location = server.getLocalAddress().toString();
        }
        log.info( "Prism Interface started and is listening for {} connections on {}", name.toLowerCase(), location );
        this.reader = new PIRequestReader( name );
        Thread acceptor = new Thread( () -> acceptLoop( server, clientManager, name, createTransport ), "PrismInterface" + name + "Server" );
        acceptor.start();
        Runtime.getRuntime().addShutdownHook( new Thread( this::shutdownHook ) );
    }


    private PIServer( ServerSocketChannel server, ClientManager clientManager, String name, Function<SocketChannel, Transport> createTransport ) throws IOException {
        this( server, clientManager, name, createTransport, null );
    }


    static PIServer startServer( ClientManager clientManager, PrismInterface.Transport transport, Map<String, String> settings ) throws IOException {
        return switch ( transport ) {
            case PLAIN -> new PIServer( createInetServer( Integer.parseInt( settings.get( "port" ) ) ), clientManager, "Plain", PlainTransport::accept );
            case UNIX -> {
                ServerAndLock sl = createUnixServer( settings.get( "path" ) );
                yield new PIServer( sl.server, clientManager, "Unix", UnixTransport::accept, sl );
            }
        };
    }


    private static ServerSocketChannel createInetServer( int port ) throws IOException {
        return ServerSocketChannel.open( StandardProtocolFamily.INET )
                .bind( new InetSocketAddress( Inet4Address.getLoopbackAddress(), port ) );
    }


    private static ServerAndLock createUnixServer( String path ) throws IOException {
        File socket;
        if ( !path.endsWith( ".sock" ) ) {
            throw new IOException( "Socket paths must end with .sock" );
        }
        Path p = Paths.get( path );
        Path lockPath;
        if ( p.isAbsolute() ) {
            socket = p.toFile();
            lockPath = Paths.get( path + ".lock" );
        } else {
            if ( p.getNameCount() != 1 ) {
                throw new IOException( "Relative socket paths may not contain directory separators" );
            }
            PolyphenyHomeDirManager phm = PolyphenyHomeDirManager.getInstance();
            socket = phm.registerNewFile( path );
            lockPath = phm.registerNewFile( path + ".lock" ).toPath();
        }
        FileChannel fileChannel = FileChannel.open( lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE );
        Optional<FileLock> fileLock = Optional.ofNullable( fileChannel.tryLock() );
        if ( fileLock.isPresent() ) {
            socket.delete();
            ServerSocketChannel s = ServerSocketChannel.open( StandardProtocolFamily.UNIX )
                    .bind( UnixDomainSocketAddress.of( socket.getAbsolutePath() ) );
            socket.setWritable( true, false );
            return new ServerAndLock( s, fileLock.get(), socket, lockPath );
        } else {
            throw new IOException( "There is already a Polypheny instance listening at " + socket.getPath() );
        }
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
            PIService.acceptConnection( createTransport.apply( s ), connectionId, clientManager, reader );
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
                Thread t = new Thread( () -> acceptConnection( s, name, connectionId, createTransport, clientManager ), String.format( "PrismInterface" + name + "ClientConnection%d", connectionId ) );
                t.start();
            } catch ( IOException e ) {
                if ( e instanceof AsynchronousCloseException && shutdown.get() ) {
                    return;
                }
                log.error( "acceptLoop", e );
                break;
            } catch ( Throwable t ) {
                // For debug purposes
                log.error( "Unhandled exception", t );
                break;
            }
        }
    }


    void shutdown() throws InterruptedException, IOException {
        if ( shutdown.getAndSet( true ) ) {
            return;
        }
        if ( log.isTraceEnabled() ) {
            log.trace( "prism-interface server shutdown requested" );
        }
        if ( fileLock != null ) {
            fileLock.socket.delete();
            fileLock.lockFile.toFile().delete();
            fileLock.lock.release();
        }
        Util.closeNoThrow( server );
        Util.closeNoThrow( reader );
    }


    private void shutdownHook() {
        try {
            shutdown();
        } catch ( IOException | InterruptedException e ) {
            log.info( "Shutdown hook failed: ", e );
        }
    }


    private record ServerAndLock( ServerSocketChannel server, FileLock lock, File socket, Path lockFile ) {

    }

}
