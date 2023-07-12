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

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PIServer {

    private final Server server;
    private final int port;


    public PIServer(int port, PIService service, ClientManager clientManager ) {
        this.port = port;
        ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort( port, InsecureServerCredentials.create() );
        server = serverBuilder
                .addService( service )
                .intercept( new ClientMetaInterceptor( clientManager ) )
                .intercept( new ExceptionHandler() )
                .build();
    }


    public void start() throws IOException {
        server.start();
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface server started on port {}", port );
        }
        // used to handle unexpected shutdown of the JVM
        Runtime.getRuntime().addShutdownHook( getShutdownHook() );
    }


    public void shutdown() throws InterruptedException {
        if ( server == null ) {
            return;
        }
        if ( log.isTraceEnabled() ) {
            log.trace( "proto-interface server shutdown requested" );
        }
        server.shutdown().awaitTermination( 30, TimeUnit.SECONDS );
    }


    private Thread getShutdownHook() {
        return new Thread( () -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println( "shutting down gRPC server since JVM is shutting down" );
            try {
                PIServer.this.shutdown();
            } catch ( InterruptedException e ) {
                e.printStackTrace( System.err );
            }
            System.err.println( "server shut down" );
        } );
    }

}
