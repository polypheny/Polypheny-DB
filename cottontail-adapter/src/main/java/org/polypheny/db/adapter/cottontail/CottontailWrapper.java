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


import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CreateEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CreateIndexMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CreateSchemaMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DeleteMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DropEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DropIndexMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ListSchemaMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage.Tuple;
import org.vitrivr.cottontail.grpc.CottontailGrpc.TransactionId;
import org.vitrivr.cottontail.grpc.CottontailGrpc.TruncateEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage;
import org.vitrivr.cottontail.grpc.DDLGrpc;
import org.vitrivr.cottontail.grpc.DDLGrpc.DDLBlockingStub;
import org.vitrivr.cottontail.grpc.DMLGrpc;
import org.vitrivr.cottontail.grpc.DMLGrpc.DMLBlockingStub;
import org.vitrivr.cottontail.grpc.DQLGrpc;
import org.vitrivr.cottontail.grpc.DQLGrpc.DQLBlockingStub;
import org.vitrivr.cottontail.grpc.TXNGrpc;
import org.vitrivr.cottontail.grpc.TXNGrpc.TXNBlockingStub;


@Slf4j
public class CottontailWrapper implements AutoCloseable {

    private final ManagedChannel channel;
    public static final int maxMessageSize = 150_000_000;
    private static final long MAX_QUERY_CALL_TIMEOUT = 300_000; // TODO expose to config
    private static final long MAX_CALL_TIMEOUT = 5000; // TODO expose to config


    public CottontailWrapper( ManagedChannel channel ) {
        this.channel = channel;
    }


    /**
     * Begins a new transaction and returns its {@link TransactionId}.
     *
     * @return {@link TransactionId} or null, if transaction couldn't be started.
     */
    public TransactionId begin() {
        try {
            final TXNBlockingStub stub = TXNGrpc.newBlockingStub( this.channel );
            return stub.begin( Empty.getDefaultInstance() );
        } catch ( StatusRuntimeException e ) {
            log.error( "Could not start transaction due to error", e );
            return null;
        }
    }


    /**
     * Commits a transaction for the given {@link TransactionId}.
     *
     * @param txId {@link TransactionId} of transaction to commit.
     */
    public void commit( TransactionId txId ) {
        try {
            final TXNBlockingStub stub = TXNGrpc.newBlockingStub( this.channel );
            stub.commit( txId );
        } catch ( StatusRuntimeException e ) {
            log.error( "Could not COMMIT transaction {} due to error.", txId.getValue(), e );
        }
    }


    /**
     * Rolls back the transaction for the given {@link TransactionId}.
     *
     * @param txId {@link TransactionId} of transaction to commit.
     */
    public void rollback( TransactionId txId ) {
        try {
            final TXNBlockingStub stub = TXNGrpc.newBlockingStub( this.channel );
            stub.rollback( txId );
        } catch ( StatusRuntimeException e ) {
            log.error( "Could not ROLLBACK transaction {} due to error.", txId.getValue(), e );
        }
    }


    public boolean createEntityBlocking( CreateEntityMessage createMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        try {
            stub.createEntity( createMessage );
            return true;
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                log.warn( "Entity {} was not created because it already exists", toString( createMessage.getDefinition().getEntity() ) );
            } else {
                log.error( "Caught exception", e );
            }
            return false;
        }
    }


    public synchronized void createIndexBlocking( CreateIndexMessage createMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        try {
            stub.createIndex( createMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                log.warn( "Index on {} was not created because it already exists.", toString( createMessage.getDefinition().getColumnsList().get( 0 ) ) );
                return;
            }
            log.error( "Caught exception", e );
        }
    }


    public synchronized void dropIndexBlocking( DropIndexMessage dropMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        try {
            stub.dropIndex( dropMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.warn( "Unable to drop index {}", toString( dropMessage.getIndex() ) );
                return;
            }
            log.error( "Caught exception", e );
        }
    }


    public synchronized void dropEntityBlocking( DropEntityMessage dropMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        try {
            stub.dropEntity( dropMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.debug( "entity {} was not dropped because it does not exist", toString( dropMessage.getEntity() ) );
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public synchronized void truncateEntityBlocking( TruncateEntityMessage truncateMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        try {
            stub.truncateEntity( truncateMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.debug( "entity {} was not truncated because it does not exist", toString( truncateMessage.getEntity() ) );
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public synchronized void checkedCreateSchemaBlocking( CreateSchemaMessage createMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        final Iterator<QueryResponseMessage> schemas = stub.listSchemas( ListSchemaMessage.getDefaultInstance() );
        while ( schemas.hasNext() ) {
            final QueryResponseMessage next = schemas.next();
            for ( Tuple t : next.getTuplesList() ) {
                if ( t.getData( 0 ).getStringData().equals( createMessage.getSchema().getName() ) ) {
                    return;
                }
            }
        }

        this.createSchemaBlocking( createMessage );
    }


    public synchronized void createSchemaBlocking( CreateSchemaMessage createMessage ) {
        final DDLBlockingStub stub = DDLGrpc.newBlockingStub( this.channel );
        try {
            stub.createSchema( createMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                log.debug( "Schema {} was not created because it does already exist.", createMessage.getSchema().getName() );
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public long delete( DeleteMessage message ) {
        final DMLBlockingStub stub = DMLGrpc.newBlockingStub( this.channel );
        try {
            final QueryResponseMessage response = stub.delete( message );
            return response.getTuplesList().get( 0 ).getData( 0 ).getLongData(); /* Number of deletions as returned by Cottontail DB. */
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                log.debug( "Deletion failed due to user error: {}", e.getMessage() );
            } else {
                log.error( "Caught exception", e );
            }
            return -1L; /* Signal error. */
        }
    }


    public long update( UpdateMessage message ) {
        final DMLBlockingStub stub = DMLGrpc.newBlockingStub( this.channel );
        try {
            final QueryResponseMessage response = stub.update( message );
            return response.getTuplesList().get( 0 ).getData( 0 ).getLongData(); /* Number of updates as returned by Cottontail DB. */
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                log.debug( "Update failed due to user error: {}", e.getMessage() );
            } else {
                log.error( "Caught exception", e );
            }
            return -1L; /* Signal error. */
        }
    }


    public boolean insert( InsertMessage message ) {
        final DMLBlockingStub stub = DMLGrpc.newBlockingStub( this.channel );
        try {
            stub.insert( message );
            return true;
        } catch ( StatusRuntimeException e ) {
            log.error( "Caught exception", e );
            return false;
        }
    }


    public boolean insert( List<InsertMessage> messages ) {
        final DMLBlockingStub stub = DMLGrpc.newBlockingStub( this.channel );
        try {
            for ( InsertMessage m : messages ) {
                stub.insert( m );
            }
            return true;
        } catch ( StatusRuntimeException e ) {
            log.error( "Caught exception", e );
            return false;
        }
    }


    public Iterator<QueryResponseMessage> query( QueryMessage query ) {
        final DQLBlockingStub stub = DQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( MAX_QUERY_CALL_TIMEOUT, TimeUnit.MILLISECONDS );
        try {
            return stub.query( query );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                log.debug( "Query failed due to user error: {}", e.getMessage() );
            } else if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "Query has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT );
            } else {
                log.error( "Caught exception", e );
            }
            throw e;
        }
    }


    public Iterator<QueryResponseMessage> batchedQuery( BatchedQueryMessage query ) {
        final DQLBlockingStub stub = DQLGrpc.newBlockingStub( this.channel ).withDeadlineAfter( MAX_QUERY_CALL_TIMEOUT, TimeUnit.MILLISECONDS );
        try {
            return stub.batchQuery( query );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                log.debug( "Query failed due to user error: {}", e.getMessage() );
            } else if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "Query has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT );
            } else {
                log.error( "Caught exception", e );
            }
            throw new RuntimeException( e );
        }
    }


    @Override
    public void close() {
        try {
            this.channel.shutdown();
            this.channel.awaitTermination( 3, TimeUnit.SECONDS );
        } catch ( InterruptedException e ) {
            log.error( "Thread was interrupted while awaiting termination of gRPC channel." );
        }
    }


    private static String toString( EntityName name ) {
        return name.getSchema().getName() + "." + name.getName();
    }


    private static String toString( IndexName name ) {
        return name.getEntity().getSchema().getName() + "." + name.getEntity().getName() + "." + name.getName();
    }


    private static String toString( ColumnName name ) {
        return name.getEntity().getSchema().getName() + "." + name.getEntity().getName() + "." + name.getName();
    }

}
