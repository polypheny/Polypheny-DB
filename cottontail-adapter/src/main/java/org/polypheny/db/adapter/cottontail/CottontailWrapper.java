/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail;


import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.vitrivr.cottontail.grpc.CottonDDLGrpc;
import org.vitrivr.cottontail.grpc.CottonDDLGrpc.CottonDDLBlockingStub;
import org.vitrivr.cottontail.grpc.CottonDDLGrpc.CottonDDLFutureStub;
import org.vitrivr.cottontail.grpc.CottonDMLGrpc;
import org.vitrivr.cottontail.grpc.CottonDMLGrpc.CottonDMLStub;
import org.vitrivr.cottontail.grpc.CottonDQLGrpc;
import org.vitrivr.cottontail.grpc.CottonDQLGrpc.CottonDQLBlockingStub;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DeleteMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Empty;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Index;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage;


@Slf4j
public class CottontailWrapper implements AutoCloseable {

    private static final CottontailGrpc.Status INTERRUPTED_INSERT = CottontailGrpc.Status.newBuilder().setSuccess( false ).build();

    private final ManagedChannel channel;
    private final CottonDDLFutureStub definitionFutureStub;
    private final CottonDMLStub managementStub;
    private final CottonDMLStub insertStub;

    public static final int maxMessageSize = 150_000_000;
    private static final long MAX_QUERY_CALL_TIMEOUT = 300_000; // TODO expose to config
    private static final long MAX_CALL_TIMEOUT = 5000; // TODO expose to config


    public CottontailWrapper( ManagedChannel channel ) {
        this.channel = channel;
        this.definitionFutureStub = CottonDDLGrpc.newFutureStub( channel );
        this.managementStub = CottonDMLGrpc.newStub( channel );
        this.insertStub = CottonDMLGrpc.newStub( channel );
    }


    public synchronized ListenableFuture<CottontailGrpc.Status> createEntity( EntityDefinition createMessage ) {
        final CottonDDLFutureStub stub = CottonDDLGrpc.newFutureStub( this.channel );
        return stub.createEntity( createMessage );
    }


    public synchronized boolean createEntityBlocking( EntityDefinition createMessage ) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
        try {
            stub.createEntity( createMessage );
            return true;
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                log.warn( "Entity {} was not created because it already exists", createMessage.getEntity().getName() );
            } else {
                log.error( "Caught exception", e );
            }
            return false;
        }
    }


    public synchronized void createIndexBlocking( IndexDefinition createMessage ) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
        try {
            stub.createIndex( createMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                log.warn( "Index on {}.{} was not created because it already exists", createMessage.getIndex().getEntity().getName(), createMessage.getColumnsList().toString() );
                return;
            }
            log.error( "Caught exception", e );
        }
    }


    public synchronized void dropIndexBlocking( Index dropMessage ) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
        try {
            stub.dropIndex( dropMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.warn( "Unable to drop index {}", dropMessage.getEntity().getName() );
                return;
            }
            log.error( "Caught exception", e );
        }
    }


    public synchronized void dropEntityBlocking( Entity entity ) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
        try {
            stub.dropEntity( entity );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.debug( "entity {} was not dropped because it does not exist", entity.getName() );
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public synchronized void truncateEntityBlocking( Entity entity ) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
        try {
            stub.truncate( entity );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.debug( "entity {} was not truncated because it does not exist", entity.getName() );
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public synchronized void checkedCreateSchemaBlocking( Schema schema ) {
        CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel );
        Iterator<Schema> schemas = stub.listSchemas( Empty.newBuilder().build() );
        while ( schemas.hasNext() ) {
            Schema existingSchema = schemas.next();
            if ( schema.getName().equals( existingSchema.getName() ) ) {
                return;
            }
        }

        this.createSchemaBlocking( schema );
    }


    public synchronized ListenableFuture<CottontailGrpc.Status> createSchema( Schema schema ) {
        final CottonDDLFutureStub stub = CottonDDLGrpc.newFutureStub( this.channel );
        return stub.createSchema( schema );
    }


    public synchronized void createSchemaBlocking( Schema schema ) {
        ListenableFuture<CottontailGrpc.Status> future = this.createSchema( schema );
        try {
            future.get();
        } catch ( InterruptedException | ExecutionException e ) {
            log.error( "error in createSchemaBlocking", e );
        }
    }


    public Long delete( DeleteMessage message ) {

        final AtomicLong count = new AtomicLong( 0 );
        final AtomicBoolean errored = new AtomicBoolean( false );
        final AtomicBoolean completed = new AtomicBoolean( false );

        final StreamObserver<CottontailGrpc.QueryResponseMessage> observer = new StreamObserver<QueryResponseMessage>() {
            @Override
            public void onNext( QueryResponseMessage value ) {
                if ( value.getResultsList().size() != 0 ) {
                    count.addAndGet( value.getResultsList().get( 0 ).getDataMap().get( "deleted" ).getLongData() );
                }
            }


            @Override
            public void onError( Throwable t ) {
                errored.set( true );
            }


            @Override
            public void onCompleted() {
                completed.set( true );
            }
        };

        this.managementStub.delete( message, observer );

        while ( !completed.get() && !errored.get() ) {
            Thread.yield();
        }

        return errored.get() ? -1 : count.get();
    }


    public Long update( UpdateMessage message ) {

        final AtomicLong count = new AtomicLong( 0 );
        final AtomicBoolean errored = new AtomicBoolean( false );
        final AtomicBoolean completed = new AtomicBoolean( false );

        final StreamObserver<CottontailGrpc.QueryResponseMessage> observer = new StreamObserver<QueryResponseMessage>() {
            @Override
            public void onNext( QueryResponseMessage value ) {
                if ( value.getResultsList().size() != 0 ) {
                    count.addAndGet( value.getResultsList().get( 0 ).getDataMap().get( "updated" ).getLongData() );
                }
            }


            @Override
            public void onError( Throwable t ) {
                errored.set( true );
            }


            @Override
            public void onCompleted() {
                completed.set( true );
            }
        };

        this.managementStub.update( message, observer );

        while ( !completed.get() && !errored.get() ) {
            Thread.yield();
        }

        return errored.get() ? -1 : count.get();
    }


    public boolean insert( List<InsertMessage> messages ) {

        final boolean[] status = { false, false }; /* {done, error}. */
        final StreamObserver<CottontailGrpc.Status> observer = new StreamObserver<CottontailGrpc.Status>() {

            @Override
            public void onNext( CottontailGrpc.Status value ) {
                log.trace( "Tuple received: {}", value.getTimestamp() );
            }


            @Override
            public void onError( Throwable t ) {
                status[0] = true;
                status[1] = true;
                log.error( "Error during insert. Everything was rolled back: {}", t.getMessage() );
            }


            @Override
            public void onCompleted() {
                status[0] = true;
                log.trace( "Insert successful. Changes were committed!" );
            }
        };

        try {
            /* Start data transfer. */
            final StreamObserver<InsertMessage> sink = this.managementStub.insert( observer );
            for ( InsertMessage message : messages ) {
                sink.onNext( message );
            }
            sink.onCompleted(); /* Send commit message. */

            while ( !status[0] ) {
                Thread.yield();
            }
        } catch ( Exception e ) {
            log.error( "Caught exception", e );
        }
        return !status[1];
    }


    /*public CottontailGrpc.Status insertBlocking(List<InsertMessage> message) {
        ListenableFuture<CottontailGrpc.Status> future = this.insert(message);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("error in insertBlocking", e );
            return INTERRUPTED_INSERT;
        }
    }*/


    /**
     * Issues a single query to the Cottontail DB endpoint in a blocking fashion.
     *
     * @return The query results (unprocessed).
     */
    public Iterator<QueryResponseMessage> query( QueryMessage query ) {
        final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( MAX_QUERY_CALL_TIMEOUT, TimeUnit.MILLISECONDS );
        try {
            return stub.query( query );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "CottontailWrapper.batchedQuery has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT );
            } else {
                log.error( "Error occurred during invocation of CottontailWrapper.batchedQuery: {}", e.getMessage() );
            }
            throw new RuntimeException( e );
        }
    }


    /**
     * Issues a batched query to the Cottontail DB endpoint in a blocking fashion.
     *
     * @return The query results (unprocessed).
     */
    public Iterator<QueryResponseMessage> batchedQuery( BatchedQueryMessage query ) {
        final ArrayList<QueryResponseMessage> results = new ArrayList<>();
        final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( MAX_QUERY_CALL_TIMEOUT, TimeUnit.MILLISECONDS );
        try {
            return stub.batchedQuery( query );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "CottontailWrapper.batchedQuery has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT );
            } else {
                log.error( "Error occurred during invocation of CottontailWrapper.batchedQuery: {}", e.getMessage() );
            }
            throw new RuntimeException( e );
        }
    }


    /**
     * Pings the Cottontail DB endpoint and returns true on success and false otherwise.
     *
     * @return True on success, false otherwise.
     */
    public boolean ping() {
        final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( MAX_CALL_TIMEOUT, TimeUnit.MILLISECONDS );
        try {
            final CottontailGrpc.Status status = stub.ping( Empty.newBuilder().build() );
            return true;
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "CottontailWrapper.ping has timed out." );
            } else {
                log.error( "Error occurred during invocation of CottontailWrapper.ping: {}", e.getMessage() );

            }
            return false;
        }
    }


    /**
     * Uses the Cottontail DB endpoint to list all entities.
     *
     * @param schema Schema for which to list entities.
     * @return List of entities.
     */
    public List<Entity> listEntities( Schema schema ) {
        ArrayList<Entity> entities = new ArrayList<>();
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( MAX_CALL_TIMEOUT, TimeUnit.MILLISECONDS );
        try {
            stub.listEntities( schema ).forEachRemaining( entities::add );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "CottontailWrapper.listEntities has timed out (timeout = {}ms).", MAX_CALL_TIMEOUT );
            } else {
                log.error( "Error occurred during invocation of CottontailWrapper.listEntities: {}", e.getMessage() );
            }
        }
        return entities;
    }


    @Override
    public void close() {
        this.channel.shutdown();
    }

}
