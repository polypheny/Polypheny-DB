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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.UnknownFieldSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ConnectionProperties;
import org.polypheny.prism.ConnectionRequest;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.IndexedParameters;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;

@Tag("adapter")
@Timeout(20)
class PrismQueryTimeoutTest {

    private static final String NAMESPACE = Catalog.DEFAULT_NAMESPACE_NAME;
    private static final String TABLE = "prism_query_timeout_test";
    private static final String UPDATE = "UPDATE " + TABLE + " SET payload = payload + ";


    @BeforeAll
    static void createTable() throws Exception {
        TestHelper.getInstance();
        try ( PrismConnection admin = new PrismConnection( true, 5 ) ) {
            admin.execute( NAMESPACE, "sql", "CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, payload INTEGER)", -1 );
            admin.execute( NAMESPACE, "sql", "INSERT INTO " + TABLE + " VALUES (1, 0)", -1 );
        }
    }


    @AfterAll
    static void dropTable() throws Exception {
        try ( PrismConnection admin = new PrismConnection( true, 5 ) ) {
            admin.execute( NAMESPACE, "sql", "DROP TABLE IF EXISTS " + TABLE, -1 );
        }
    }


    @Test
    void timesOutDirectExecutionAndKeepsTheSessionUsable() throws Exception {
        verifyTimeoutAndReuse( Execution.DIRECT );
    }


    @Test
    void timesOutDirectBatchExecutionAndKeepsTheSessionUsable() throws Exception {
        verifyTimeoutAndReuse( Execution.DIRECT_BATCH );
    }


    @Test
    void timesOutPreparedExecutionAndKeepsTheHandleUsable() throws Exception {
        verifyTimeoutAndReuse( Execution.PREPARED );
    }


    @Test
    void timesOutPreparedBatchExecutionAndKeepsTheHandleUsable() throws Exception {
        verifyTimeoutAndReuse( Execution.PREPARED_BATCH );
    }


    private static void verifyTimeoutAndReuse( Execution execution ) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try ( PrismConnection client = new PrismConnection( true, 5 ) ) {
            Request.Builder operation = request( client, execution );
            int previous = readPayload( client );
            Transaction holder = TestHelper.getInstance().getTransactionManager().startTransaction( Catalog.defaultUserId, null, "Prism timeout test" );
            try {
                Lockable resource = execution == Execution.DIRECT
                        ? LockablesRegistry.INSTANCE.getOrCreateLockable( Catalog.snapshot().rel().getTable( NAMESPACE, TABLE ).orElseThrow() )
                        : LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE;
                holder.acquireLockable( resource, LockType.EXCLUSIVE );
                Request.Builder timed = withTimeout( operation.clone() );
                Response failure = executor.submit( () -> client.send( timed ) ).get( 4, TimeUnit.SECONDS );
                assertEquals( Response.TypeCase.ERROR_RESPONSE, failure.getTypeCase() );
                assertEquals( "57014", failure.getErrorResponse().getDetails().getState() );
            } finally {
                holder.rollback( null );
            }
            assertTrue( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE.getCopyOfOwners().stream().allMatch( Transaction::isActive ),
                    "A canceled lock wait must release its transaction's inherited schema lock." );
            assertEquals( previous, readPayload( client ) );
            Response successful = client.send( operation );
            assertEquals( execution.responseType, successful.getTypeCase(), () -> successful.getErrorResponse().getMessage() );
            assertEquals( previous + 1, readPayload( client ) );
        } finally {
            executor.shutdownNow();
        }
    }


    @Test
    void queuedTimeoutDoesNotExecuteOrRollBackEarlierManualWork() throws Exception {
        try ( PrismConnection observer = new PrismConnection( true, 5 ) ) {
            int previous = readPayload( observer );
            PlainTransport socket = new PlainTransport( "localhost", 20590 );
            try {
                connectManual( socket );

                Transaction holder = TestHelper.getInstance().getTransactionManager().startTransaction( Catalog.defaultUserId, null, "Prism timeout test" );
                try {
                    holder.acquireLockable( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, LockType.EXCLUSIVE );
                    send( socket, 2, Request.newBuilder().setExecuteUnparameterizedStatementRequest( directUpdate( 1 ) ) );
                    Response acknowledgment = Response.parseFrom( socket.receiveMessage() );
                    assertEquals( 2, acknowledgment.getId() );
                    assertTrue( acknowledgment.hasStatementResponse() && !acknowledgment.getLast() );
                    send( socket, 3, withTimeout( Request.newBuilder().setExecuteUnparameterizedStatementRequest( directUpdate( 10 ) ) ) );
                    TimeUnit.MILLISECONDS.sleep( 500 );
                } finally {
                    holder.rollback( null );
                }
                assertEquals( 1, receive( socket, 2 ).getStatementResponse().getResult().getScalar() );
                Response expired = receive( socket, 3 );
                assertEquals( Response.TypeCase.ERROR_RESPONSE, expired.getTypeCase() );
                assertEquals( "57014", expired.getErrorResponse().getDetails().getState() );
                send( socket, 4, Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
                assertEquals( Response.TypeCase.COMMIT_RESPONSE, receive( socket, 4 ).getTypeCase() );
                assertEquals( previous + 1, readPayload( observer ) );
            } finally {
                socket.close();
            }
        }
    }


    @Test
    void streamedTimeoutDoesNotExecuteOrRollBackEarlierManualWork() throws Exception {
        try ( PrismConnection observer = new PrismConnection( true, 5 ); FragmentedTransport socket = new FragmentedTransport() ) {
            int previous = readPayload( observer );
            connectManual( socket );
            send( socket, 2, Request.newBuilder().setExecuteUnparameterizedStatementRequest( directUpdate( 1 ) ) );
            assertEquals( 1, receive( socket, 2 ).getStatementResponse().getResult().getScalar() );

            Request timed = withTimeout( Request.newBuilder().setExecuteUnparameterizedStatementRequest( directUpdate( 10 ) ) ).setId( 3 ).build();
            socket.sendWithSplitHeader( timed );
            Response expired = Response.parseFrom( socket.receiveMessage() );
            assertEquals( 3, expired.getId() );
            assertTrue( expired.getLast() );
            assertEquals( Response.TypeCase.ERROR_RESPONSE, expired.getTypeCase() );
            assertEquals( "57014", expired.getErrorResponse().getDetails().getState() );

            send( socket, 4, Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
            assertEquals( Response.TypeCase.COMMIT_RESPONSE, receive( socket, 4 ).getTypeCase() );
            assertEquals( previous + 1, readPayload( observer ) );
        }
    }


    private static void connectManual( PlainTransport socket ) throws Exception {
        send( socket, 1, Request.newBuilder().setConnectionRequest( ConnectionRequest.newBuilder()
                .setMajorApiVersion( 2 ).setMinorApiVersion( 5 ).setUsername( "pa" ).setPassword( "" )
                .setConnectionProperties( ConnectionProperties.newBuilder().setIsAutoCommit( false ) ) ) );
        Response handshake = receive( socket, 1 );
        assertTrue( handshake.getConnectionResponse().getIsCompatible() );
        assertEquals( List.of( 1L ), handshake.getConnectionResponse().getUnknownFields().getField( 50001 ).getVarintList() );
    }


    private static Request.Builder withTimeout( Request.Builder request ) {
        return request.setUnknownFields( UnknownFieldSet.newBuilder()
                .addField( 50001, UnknownFieldSet.Field.newBuilder().addVarint( 250 ).build() ).build() );
    }


    private static void send( PlainTransport socket, long id, Request.Builder request ) throws Exception {
        socket.sendMessage( request.setId( id ).build().toByteArray() );
    }


    private static Response receive( PlainTransport socket, long id ) throws Exception {
        Response response;
        do {
            response = Response.parseFrom( socket.receiveMessage() );
            assertEquals( id, response.getId() );
        } while ( !response.getLast() );
        return response;
    }


    private static ExecuteUnparameterizedStatementRequest directUpdate( int amount ) {
        return ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( NAMESPACE ).setLanguageName( "sql" ).setStatement( UPDATE + amount ).build();
    }


    private static int readPayload( PrismConnection client ) throws Exception {
        return client.execute( NAMESPACE, "sql", "SELECT payload FROM " + TABLE, -1 ).getResult()
                .getFrame().getRelationalFrame().getRows( 0 ).getValues( 0 ).getInteger().getInteger();
    }


    private static Request.Builder request( PrismConnection client, Execution execution ) throws Exception {
        ExecuteUnparameterizedStatementRequest direct = directUpdate( 1 );
        if ( execution == Execution.DIRECT ) {
            return Request.newBuilder().setExecuteUnparameterizedStatementRequest( direct );
        }
        if ( execution == Execution.DIRECT_BATCH ) {
            return Request.newBuilder().setExecuteUnparameterizedStatementBatchRequest(
                    ExecuteUnparameterizedStatementBatchRequest.newBuilder().addStatements( direct ) );
        }
        Response prepared = client.send( Request.newBuilder().setPrepareIndexedStatementRequest( PrepareStatementRequest.newBuilder()
                .setNamespaceName( NAMESPACE ).setLanguageName( "sql" ).setStatement( UPDATE + "?" ) ) );
        assertEquals( Response.TypeCase.PREPARED_STATEMENT_SIGNATURE, prepared.getTypeCase(), () -> prepared.getErrorResponse().getMessage() );
        int statementId = prepared.getPreparedStatementSignature().getStatementId();
        IndexedParameters parameters = IndexedParameters.newBuilder().addParameters( ProtoValue.newBuilder()
                .setInteger( ProtoInteger.newBuilder().setInteger( 1 ) ) ).build();
        if ( execution == Execution.PREPARED ) {
            return Request.newBuilder().setExecuteIndexedStatementRequest( ExecuteIndexedStatementRequest.newBuilder()
                    .setStatementId( statementId ).setParameters( parameters ) );
        }
        return Request.newBuilder().setExecuteIndexedStatementBatchRequest( ExecuteIndexedStatementBatchRequest.newBuilder()
                .setStatementId( statementId ).addAllParameters( List.of( parameters ) ) );
    }


    private enum Execution {
        DIRECT( Response.TypeCase.STATEMENT_RESPONSE ),
        DIRECT_BATCH( Response.TypeCase.STATEMENT_BATCH_RESPONSE ),
        PREPARED( Response.TypeCase.STATEMENT_RESULT ),
        PREPARED_BATCH( Response.TypeCase.STATEMENT_BATCH_RESPONSE );

        final Response.TypeCase responseType;

        Execution( Response.TypeCase responseType ) {
            this.responseType = responseType;
        }
    }


    private static final class FragmentedTransport extends PlainTransport implements AutoCloseable {

        private FragmentedTransport() throws IOException {
            super( "localhost", 20590 );
        }


        private void sendWithSplitHeader( Request request ) throws IOException, InterruptedException {
            byte[] payload = request.toByteArray();
            ByteBuffer frame = ByteBuffer.allocate( Long.BYTES + payload.length ).order( ByteOrder.LITTLE_ENDIAN );
            frame.putLong( payload.length ).put( payload ).flip();
            ByteBuffer prefix = frame.slice();
            prefix.limit( 1 );
            writeEntireBuffer( prefix );
            TimeUnit.MILLISECONDS.sleep( 750 );
            frame.position( prefix.limit() );
            writeEntireBuffer( frame );
        }

    }

}
