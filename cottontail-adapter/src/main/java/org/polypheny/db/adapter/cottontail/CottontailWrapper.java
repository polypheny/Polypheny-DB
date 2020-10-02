/*
 * Copyright 2019-2020 The Polypheny Project
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


import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc.CottonDDLBlockingStub;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc.CottonDDLFutureStub;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc.CottonDDLStub;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc.CottonDMLFutureStub;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc.CottonDMLStub;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc;
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc.CottonDQLBlockingStub;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.BatchedQueryMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.CreateEntityMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.CreateIndexMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Empty;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Entity;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.InsertMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.InsertStatus;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.QueryMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Schema;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.SuccessStatus;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CottontailWrapper implements AutoCloseable {

    private static final InsertStatus INTERRUPTED_INSERT = InsertStatus.newBuilder().setSuccess(false).build();

    private final ManagedChannel channel;
    private final CottonDDLFutureStub definitionFutureStub;
    private final CottonDMLFutureStub managementStub;
    private final CottonDMLStub insertStub;

    public static final int maxMessageSize = 10_000_000;
    private static final long MAX_QUERY_CALL_TIMEOUT = 300_000; //TODO expose to config
    private static final long MAX_CALL_TIMEOUT = 5000; //TODO expose to config

    public CottontailWrapper( ManagedChannel channel ) {
        this.channel = channel;
        this.definitionFutureStub = CottonDDLGrpc.newFutureStub(channel);
        this.managementStub = CottonDMLGrpc.newFutureStub(channel);
        this.insertStub = CottonDMLGrpc.newStub(channel);
    }

    public synchronized ListenableFuture<SuccessStatus> createEntity(CreateEntityMessage createMessage) {
        final CottonDDLFutureStub stub = CottonDDLGrpc.newFutureStub(this.channel);
        return stub.createEntity(createMessage);
    }

    public synchronized void createEntityBlocking(CreateEntityMessage createMessage) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub(this.channel);
        try {
            stub.createEntity(createMessage);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode()) {
                log.warn("Entity {} was not created because it already exists", createMessage.getEntity().getName());
            } else {
                e.printStackTrace();
            }
        }
    }

    public synchronized void createIndexBlocking(CreateIndexMessage createMessage) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub(this.channel);
        try {
            stub.createIndex(createMessage);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode()) {
                log.warn("Index on {}.{} was not created because it already exists", createMessage.getIndex().getEntity().getName(), createMessage.getColumnsList().toString());
                return;
            }
            e.printStackTrace();
        }
    }

    public synchronized void dropEntityBlocking(Entity entity) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub(this.channel);
        try {
            stub.dropEntity(entity);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                log.debug("entity {} was not dropped because it does not exist", entity.getName());
            } else {
                e.printStackTrace();
            }
        }
    }


    public synchronized void truncateEntityBlocking( Entity entity ) {
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub(this.channel);
        try {
            stub.truncateEntity( entity );
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                log.debug("entity {} was not truncated because it does not exist", entity.getName());
            } else {
                e.printStackTrace();
            }
        }
    }


    public synchronized ListenableFuture<SuccessStatus> createSchema( Schema schema) {
        final CottonDDLFutureStub stub = CottonDDLGrpc.newFutureStub(this.channel);
        return stub.createSchema( schema );
    }

    public synchronized void createSchemaBlocking( Schema schema) {
        ListenableFuture<SuccessStatus> future = this.createSchema(schema);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("error in createSchemaBlocking", e );
        }
    }

    public ListenableFuture<InsertStatus> insert( InsertMessage message) {
        return this.managementStub.insert(message);
    }

    public InsertStatus insertBlocking(InsertMessage message) {
        ListenableFuture<InsertStatus> future = this.insert(message);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("error in insertBlocking", e );
            return INTERRUPTED_INSERT;
        }
    }

    /**
     * Issues a single query to the Cottontail DB endpoint in a blocking fashion.
     *
     * @return The query results (unprocessed).
     */
    public Iterator<QueryResponseMessage> query( QueryMessage query) {
        final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub(this.channel).withDeadlineAfter(MAX_QUERY_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        try {
            return stub.query(query);
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.DEADLINE_EXCEEDED) {
                log.error("CottontailWrapper.batchedQuery has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT);
            } else {
                log.error("Error occurred during invocation of CottontailWrapper.batchedQuery: {}", e.getMessage());
            }
            throw new RuntimeException( e );
        }
    }

    /**
     * Issues a batched query to the Cottontail DB endpoint in a blocking fashion.
     *
     * @return The query results (unprocessed).
     */
    public List<QueryResponseMessage> batchedQuery( BatchedQueryMessage query) {
        final ArrayList<QueryResponseMessage> results = new ArrayList<>();
        final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub(this.channel).withDeadlineAfter(MAX_QUERY_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        try {
            stub.batchedQuery(query).forEachRemaining(results::add);
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.DEADLINE_EXCEEDED) {
                log.error("CottontailWrapper.batchedQuery has timed out (timeout = {}ms).", MAX_QUERY_CALL_TIMEOUT);
            } else {
                log.error("Error occurred during invocation of CottontailWrapper.batchedQuery: {}", e.getMessage());
            }
        }
        return results;
    }

    /**
     * Pings the Cottontail DB endpoint and returns true on success and false otherwise.
     *
     * @return True on success, false otherwise.
     */
    public boolean ping() {
        final CottonDQLBlockingStub stub = CottonDQLGrpc.newBlockingStub(this.channel).withDeadlineAfter(MAX_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        try {
            final SuccessStatus status = stub.ping( Empty.newBuilder().build());
            return true;
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.DEADLINE_EXCEEDED) {
                log.error("CottontailWrapper.ping has timed out.");
            } else {
                log.error("Error occurred during invocation of CottontailWrapper.ping: {}", e.getMessage());

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
    public List<Entity> listEntities(Schema schema) {
        ArrayList<Entity> entities = new ArrayList<>();
        final CottonDDLBlockingStub stub = CottonDDLGrpc.newBlockingStub(this.channel).withDeadlineAfter(MAX_CALL_TIMEOUT, TimeUnit.MILLISECONDS);
        try {
            stub.listEntities(schema).forEachRemaining(entities::add);
        } catch (StatusRuntimeException e) {
            if (e.getStatus() == Status.DEADLINE_EXCEEDED) {
                log.error("CottontailWrapper.listEntities has timed out (timeout = {}ms).", MAX_CALL_TIMEOUT);
            } else {
                log.error("Error occurred during invocation of CottontailWrapper.listEntities: {}", e.getMessage());
            }
        }
        return entities;
    }

    /**
     *
     */
    @Override
    public void close() {
        this.channel.shutdown();
    }

}
