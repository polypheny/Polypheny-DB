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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;

@Tag("adapter")
@Timeout(30)
class ConcurrentPrismDdlTest {

    private static final String ABORTED_GRAPH = "catalogownershipabortedgraph";
    private static final String FAILED_GRAPH = "catalogownershipfailedgraph";
    private static final String RECOVERY_GRAPH = "catalogownershiprecoverygraph";
    private static final String FIRST_GRAPH = "catalogownershipfirstgraph";
    private static final String SECOND_GRAPH = "catalogownershipsecondgraph";


    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @AfterAll
    static void dropGraphs() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String graph : List.of( ABORTED_GRAPH, FAILED_GRAPH, RECOVERY_GRAPH, FIRST_GRAPH, SECOND_GRAPH ) ) {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + graph, -1 );
            }
        }
    }


    @Test
    void committingAnIndependentReadDoesNotPersistAbortedDdl() throws Exception {
        Catalog catalog = Catalog.getInstance();
        CountDownLatch graphAllocated = new CountDownLatch( 1 );
        CountDownLatch allowGraphAbort = new CountDownLatch( 1 );
        AtomicBoolean pausedOnce = new AtomicBoolean();
        PropertyChangeListener pauseAndFailGraphDdl = change -> {
            if ( !(change.getNewValue() instanceof Snapshot snapshot) ) {
                return;
            }
            snapshot.getNamespace( ABORTED_GRAPH ).filter( namespace -> !snapshot.alloc().getFromLogical( namespace.id ).isEmpty() ).ifPresent( namespace -> {
                if ( pausedOnce.compareAndSet( false, true ) ) {
                    graphAllocated.countDown();
                    try {
                        assertTrue( allowGraphAbort.await( 10, TimeUnit.SECONDS ) );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError( e );
                    }
                    throw new IllegalStateException( "Deliberate graph CREATE failure" );
                }
            } );
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try ( PrismConnection ddl = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( false, 3 ) ) {
            reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "SELECT 1", -1 );
            catalog.addObserver( pauseAndFailGraphDdl );
            var createGraph = executor.submit( () -> ddl.send( createGraphRequest( ABORTED_GRAPH ) ) );
            try {
                assertTrue( graphAllocated.await( 10, TimeUnit.SECONDS ) );
                Response committed = reader.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
                assertEquals( Response.TypeCase.COMMIT_RESPONSE, committed.getTypeCase() );
            } finally {
                allowGraphAbort.countDown();
            }
            Response result = createGraph.get( 10, TimeUnit.SECONDS );
            assertEquals( Response.TypeCase.ERROR_RESPONSE, result.getTypeCase() );
            assertTrue( result.getErrorResponse().getMessage().contains( "Deliberate graph CREATE failure" ) );
            assertTrue( Catalog.snapshot().getNamespace( ABORTED_GRAPH ).isEmpty(),
                    "The failed graph remained in the catalog after an unrelated read committed" );
        } finally {
            allowGraphAbort.countDown();
            catalog.removeObserver( pauseAndFailGraphDdl );
            executor.shutdownNow();
        }
    }

    @Test
    void failedGraphDdlRestoresTheLiveAdapterCatalog() throws Exception {
        Catalog catalog = Catalog.getInstance();
        AtomicReference<AllocationEntity> createdAllocation = new AtomicReference<>();
        PropertyChangeListener failGraphDdl = change -> {
            if ( !(change.getNewValue() instanceof Snapshot snapshot) ) {
                return;
            }
            snapshot.getNamespace( FAILED_GRAPH ).stream()
                    .flatMap( namespace -> snapshot.alloc().getFromLogical( namespace.id ).stream() )
                    .findFirst().ifPresent( allocation -> {
                        if ( createdAllocation.compareAndSet( null, allocation ) ) {
                            throw new IllegalStateException( "DDL probe failure after creating the physical tables" );
                        }
                    } );
        };
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            Response result;
            long nextAllocationId = IdBuilder.getInstance().getAllocId().get();
            catalog.addObserver( failGraphDdl );
            try {
                result = connection.send( createGraphRequest( FAILED_GRAPH ) );
            } finally {
                catalog.removeObserver( failGraphDdl );
            }
            assertEquals( Response.TypeCase.ERROR_RESPONSE, result.getTypeCase() );
            assertTrue( result.getErrorResponse().getMessage().contains( "DDL probe failure" ) );
            assertTrue( Catalog.snapshot().getNamespace( FAILED_GRAPH ).isEmpty(), "Failed graph was still visible in the logical catalog" );
            AllocationEntity allocation = createdAllocation.get();
            var physicals = catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow().getPhysicalsFromAllocs( allocation.id );
            assertTrue( physicals == null || physicals.isEmpty(),
                    () -> "Abort removed the graph from the logical catalog but retained " + physicals.size() + " physical mappings" );
            assertTrue( IdBuilder.getInstance().getAllocId().get() > nextAllocationId,
                    "Rolling back DDL must preserve the allocation IDs already issued to the adapter" );
            Response nextCreate = connection.send( createGraphRequest( RECOVERY_GRAPH ) );
            assertEquals( Response.TypeCase.STATEMENT_RESPONSE, nextCreate.getTypeCase(), () -> nextCreate.getErrorResponse().getMessage() );
        }
    }

    @Test
    void rollbackOfAnIndependentReadDoesNotDiscardPendingGraphDdl() throws Exception {
        Catalog catalog = Catalog.getInstance();
        CountDownLatch graphAllocated = new CountDownLatch( 1 );
        CountDownLatch allowGraphCommit = new CountDownLatch( 1 );
        AtomicBoolean pausedOnce = new AtomicBoolean();
        AtomicLong allocCounterBeforeRollback = new AtomicLong();
        PropertyChangeListener pauseGraphDdl = change -> {
            if ( !(change.getNewValue() instanceof Snapshot snapshot) ) {
                return;
            }
            snapshot.getNamespace( FIRST_GRAPH ).filter( namespace -> !snapshot.alloc().getFromLogical( namespace.id ).isEmpty() ).ifPresent( namespace -> {
                if ( pausedOnce.compareAndSet( false, true ) ) {
                    allocCounterBeforeRollback.set( IdBuilder.getInstance().getAllocId().get() );
                    graphAllocated.countDown();
                    try {
                        assertTrue( allowGraphCommit.await( 10, TimeUnit.SECONDS ) );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError( e );
                    }
                }
            } );
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try ( PrismConnection ddl = new PrismConnection( true, 3 ); PrismConnection reader = new PrismConnection( false, 3 ) ) {
            reader.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "SELECT 1", -1 );
            catalog.addObserver( pauseGraphDdl );
            var createGraph = executor.submit( () -> ddl.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + FIRST_GRAPH, -1 ) );
            long allocCounterAfterRollback;
            try {
                assertTrue( graphAllocated.await( 10, TimeUnit.SECONDS ), "Graph allocation did not reach the snapshot published just before commit" );
                Response rolledBack = reader.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) );
                assertEquals( Response.TypeCase.ROLLBACK_RESPONSE, rolledBack.getTypeCase() );
                allocCounterAfterRollback = IdBuilder.getInstance().getAllocId().get();
            } finally {
                allowGraphCommit.countDown();
                catalog.removeObserver( pauseGraphDdl );
            }

            assertEquals( 1, createGraph.get( 10, TimeUnit.SECONDS ).getResult().getScalar() );
            boolean committedGraphVisible = Catalog.snapshot().getNamespace( FIRST_GRAPH ).isPresent();
            Response nextCreate = ddl.send( createGraphRequest( SECOND_GRAPH ) );
            assertAll(
                    () -> assertTrue( allocCounterAfterRollback >= allocCounterBeforeRollback.get(),
                            () -> "The independent read rollback rewound the allocation ID counter from " + allocCounterBeforeRollback.get() + " to " + allocCounterAfterRollback ),
                    () -> assertTrue( committedGraphVisible, "The CREATE returned success but the graph is absent" ),
                    () -> assertEquals( Response.TypeCase.STATEMENT_RESPONSE, nextCreate.getTypeCase(),
                            () -> nextCreate.getErrorResponse().getMessage() ) );
        } finally {
            allowGraphCommit.countDown();
            catalog.removeObserver( pauseGraphDdl );
            executor.shutdownNow();
        }
    }

    private static Request.Builder createGraphRequest( String graph ) {
        return Request.newBuilder().setExecuteUnparameterizedStatementRequest(
                ExecuteUnparameterizedStatementRequest.newBuilder()
                        .setNamespaceName( Catalog.DEFAULT_NAMESPACE_NAME )
                        .setLanguageName( "sql" )
                        .setStatement( "CREATE GRAPH NAMESPACE " + graph )
                        .setFetchSize( -1 ) );
    }


}
