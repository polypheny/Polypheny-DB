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

package org.polypheny.db.adapter.cottontail;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Transaction;
import org.vitrivr.cottontail.client.SimpleClient;
import org.vitrivr.cottontail.client.iterators.Tuple;
import org.vitrivr.cottontail.client.iterators.TupleIterator;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchInsertMessage;
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
import org.vitrivr.cottontail.grpc.CottontailGrpc.TruncateEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage;


/**
 * A wrapper class that provides all functionality exposed by the Cottontail DB gRPC endpoint.
 */
@Slf4j
public class CottontailWrapper implements AutoCloseable {

    private static final long MAX_QUERY_CALL_TIMEOUT = 300_000; // TODO expose to config

    /**
     * The {@link ManagedChannel} used by this {@link CottontailWrapper}.
     */
    private final ManagedChannel channel;

    /**
     * The {@link SimpleClient} used by this {@link CottontailWrapper}.
     */
    private final SimpleClient client;

    /**
     * A map of all the {@link PolyXid} and the Cottontail DB {@link PolyXid} of all ongoing transactions.
     */
    private final ConcurrentHashMap<PolyXid, Long> transactions = new ConcurrentHashMap<>();

    /**
     * Reference to the {@link CottontailStore} this {@link CottontailWrapper} belongs to.
     */
    private final CottontailStore store;


    /**
     * Default constructor.
     *
     * @param channel The {@link ManagedChannel} this {@link CottontailWrapper} is created with.
     * @param store The {@link CottontailStore} this {@link CottontailWrapper} is created for.
     */
    public CottontailWrapper( ManagedChannel channel, CottontailStore store ) {
        this.store = store;
        this.channel = channel;
        this.client = new SimpleClient( this.channel );
    }


    /**
     * Begins a new transaction and returns its {@link PolyXid}.
     *
     * @param transaction The {@link Transaction} to begin / continue.
     * @return The Cottontail DB {@link PolyXid}.
     */
    public long beginOrContinue( Transaction transaction ) {
        final PolyXid xid = transaction.getXid();
        transaction.registerInvolvedAdapter( this.store );
        return this.transactions.computeIfAbsent( xid, polyXid -> {
            try {
                return this.client.begin();
            } catch ( StatusRuntimeException e ) {
                log.error( "Could not start transaction due to error", e );
                return -1L;
            }
        } );
    }


    /**
     * Commits a transaction for the given {@link PolyXid}.
     *
     * @param xid {@link PolyXid} of transaction to commit.
     */
    public void commit( PolyXid xid ) {
        final Long txId = this.transactions.get( xid );
        if ( txId != null ) {
            try {
                this.client.commit( txId );
                this.transactions.remove( xid );
            } catch ( StatusRuntimeException e ) {
                log.error( "Could not COMMIT Cottontail DB transaction {} due to error; trying rollback", txId, e );
                throw new GenericRuntimeException( e );
            }
        } else {
            log.warn( "No Cottontail DB transaction for Xid {} could be found.", xid );
        }
    }


    /**
     * Rolls back the transaction for the given {@link PolyXid}.
     *
     * @param xid {@link PolyXid} of transaction to commit.
     */
    public void rollback( PolyXid xid ) {
        final Long txId = this.transactions.get( xid );
        if ( txId != null ) {
            try {
                this.client.rollback( txId );
                this.transactions.remove( xid );
            } catch ( StatusRuntimeException e ) {
                log.error( "Could not ROLLBACK Cottontail DB transaction {} due to error.", txId, e );
                throw new GenericRuntimeException( e );
            }
        } else {
            log.warn( "No Cottontail DB transaction for Xid {} could be found.", xid );
        }
    }


    public boolean createEntityBlocking( CreateEntityMessage createMessage ) {
        try {
            this.client.create( createMessage );
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
        try {
            this.client.create( createMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                log.warn( "Index on {} was not created because it already exists.", toString( createMessage.getDefinition().getColumnsList().get( 0 ) ) );
                return;
            }
            log.error( "Caught exception", e );
        }
    }


    public synchronized void dropIndexBlocking( DropIndexMessage dropMessage ) {
        try {
            this.client.drop( dropMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                log.warn( "Unable to drop index {}", toString( dropMessage.getIndex() ) );
                return;
            }
            log.error( "Caught exception", e );
        }
    }


    public synchronized void dropEntityBlocking( DropEntityMessage dropMessage ) {
        try {
            this.client.drop( dropMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "entity {} was not dropped because it does not exist", toString( dropMessage.getEntity() ) );
                }
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public synchronized void truncateEntityBlocking( TruncateEntityMessage truncateMessage ) {
        try {
            this.client.truncate( truncateMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.NOT_FOUND.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "entity {} was not truncated because it does not exist", toString( truncateMessage.getEntity() ) );
                }
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public synchronized void checkedCreateSchemaBlocking( CreateSchemaMessage createMessage ) {
        final TupleIterator schemas = this.client.list( ListSchemaMessage.getDefaultInstance() );
        while ( schemas.hasNext() ) {
            while ( schemas.hasNext() ) {
                final Tuple tuple = schemas.next();
                final String value = tuple.asString( 0 );
                if ( value != null && value.equals( createMessage.getSchema().getName() ) ) {
                    return;
                }
            }
        }
        this.createSchemaBlocking( createMessage );
    }


    public synchronized void createSchemaBlocking( CreateSchemaMessage createMessage ) {
        try {
            this.client.create( createMessage );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Schema {} was not created because it does already exist.", createMessage.getSchema().getName() );
                }
            } else {
                log.error( "Caught exception", e );
            }
        }
    }


    public long delete( DeleteMessage message ) {
        try {
            final TupleIterator response = this.client.delete( message );
            final Long results = response.next().asLong( 0 );
            return Objects.requireNonNullElse( results, -1L ); /* Number of deletions as returned by Cottontail DB. */
        } catch ( Throwable e ) {
            if ( e instanceof StatusRuntimeException exception && exception.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Deletion failed due to user error: {}", e.getMessage() );
                }
            } else {
                log.error( "Caught exception", e );
            }
            return -1L; /* Signal error. */
        }
    }


    public long update( UpdateMessage message ) {
        try {
            final TupleIterator response = this.client.update( message );
            final Long results = response.next().asLong( 0 );
            return Objects.requireNonNullElse( results, -1L );  /* Number of updates as returned by Cottontail DB. */
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Update failed due to user error: {}", e.getMessage() );
                }
            } else {
                log.error( "Caught exception", e );
            }
            return -1L; /* Signal error. */
        }
    }


    public boolean insert( InsertMessage message ) {
        try {
            this.client.insert( message );
            return true;
        } catch ( StatusRuntimeException e ) {
            log.error( "Caught exception", e );
            return false;
        }
    }


    public boolean insert( BatchInsertMessage message ) {
        try {
            this.client.insert( message );
            return true;
        } catch ( StatusRuntimeException e ) {
            log.error( "Caught exception", e );
            return false;
        }
    }


    public TupleIterator query( QueryMessage query ) {
        try {
            return this.client.query( query );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Query failed due to user error: {}", e.getMessage() );
                }
            } else if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "Query has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT );
            } else {
                log.error( "Caught exception", e );
            }
            throw e;
        }
    }


    public TupleIterator batchedQuery( CottontailGrpc.BatchedQueryMessage query ) {
        try {
            return this.client.batchedQuery( query );
        } catch ( StatusRuntimeException e ) {
            if ( e.getStatus().getCode() == Status.INVALID_ARGUMENT.getCode() ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Batched query failed due to user error: {}", e.getMessage() );
                }
            } else if ( e.getStatus() == Status.DEADLINE_EXCEEDED ) {
                log.error( "Batched query has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT );
            } else {
                log.error( "Caught exception", e );
            }
            throw e;
        }
    }


    @Override
    public void close() {
        try {
            this.channel.shutdown();
            this.channel.awaitTermination( 1000, TimeUnit.MILLISECONDS );
        } catch ( InterruptedException e ) {
            log.warn( "Thread was interrupted while awaiting shutdown of managed channel." );
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
