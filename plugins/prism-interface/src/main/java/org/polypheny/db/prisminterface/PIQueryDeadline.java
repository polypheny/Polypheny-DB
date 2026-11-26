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

import com.google.protobuf.UnknownFieldSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.transaction.QueryExecutionScope;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.Request;

final class PIQueryDeadline extends QueryExecutionScope {

    // The Prism envelope carries this opt-in extension without changing the released protobuf schema.
    private static final int QUERY_TIMEOUT_FIELD = 50001;
    private static final long MAX_TIMEOUT_MILLIS = Integer.MAX_VALUE * 1000L;
    private static final ScheduledThreadPoolExecutor DEADLINES = new ScheduledThreadPoolExecutor( 1, task -> {
        Thread thread = new Thread( task, "Prism query deadlines" );
        thread.setDaemon( true );
        return thread;
    } );

    static {
        DEADLINES.setRemoveOnCancelPolicy( true );
        DEADLINES.setKeepAliveTime( 30, TimeUnit.SECONDS );
        DEADLINES.allowCoreThreadTimeOut( true );
    }

    private final Thread owner = Thread.currentThread();
    private final Set<Transaction> transactions = new LinkedHashSet<>();
    private final PIClient client;
    private final long deadlineNanos;
    private final ScheduledFuture<?> timer;
    private boolean executing = true;
    private boolean finished;
    private boolean expired;
    private boolean interrupted;
    private int interruptionPauses;
    private PIServiceException timeoutFailure;


    private PIQueryDeadline( PIClient client, long timeoutMillis, long receivedAtNanos ) {
        this.client = client;
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos( timeoutMillis );
        deadlineNanos = receivedAtNanos + timeoutNanos;
        timer = DEADLINES.schedule( this::expire, Math.max( 0L, deadlineNanos - System.nanoTime() ), TimeUnit.NANOSECONDS );
        activate();
    }


    static void advertise( ConnectionResponse.Builder response ) {
        response.mergeUnknownFields( UnknownFieldSet.newBuilder()
                .addField( QUERY_TIMEOUT_FIELD, UnknownFieldSet.Field.newBuilder().addVarint( 1 ).build() ).build() );
    }


    static @Nullable PIQueryDeadline start( Request request, @Nullable PIClient client, long receivedAtNanos ) {
        if ( client == null || !client.supportsQueryTimeouts() ) {
            return null;
        }
        switch ( request.getTypeCase() ) {
            case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, EXECUTE_INDEXED_STATEMENT_REQUEST,
                    EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST, EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST -> {
                List<Long> values = request.getUnknownFields().getField( QUERY_TIMEOUT_FIELD ).getVarintList();
                if ( values.size() == 1 ) {
                    long timeout = values.get( 0 );
                    if ( timeout > 0 && timeout <= MAX_TIMEOUT_MILLIS ) {
                        return new PIQueryDeadline( client, timeout, receivedAtNanos );
                    }
                }
            }
            default -> {
            }
        }
        return null;
    }


    private synchronized void expire() {
        if ( executing && !finished ) {
            expired = true;
            if ( interruptionPauses == 0 ) {
                transactions.stream().filter( Transaction::isActive ).forEach( transaction -> transaction.getCancelFlag().set( true ) );
                interrupted = true;
                owner.interrupt();
            }
        }
    }


    void check() {
        remainingMillis();
    }


    @Override
    protected synchronized long remainingMillis() {
        if ( !executing || finished || interruptionPauses != 0 ) {
            return 0;
        }
        long remaining = deadlineNanos - System.nanoTime();
        if ( expired || remaining <= 0 ) {
            expired = true;
            throw new PIServiceException( "Query timed out.", "57014" );
        }
        return 1 + TimeUnit.NANOSECONDS.toMillis( remaining - 1 );
    }


    @Override
    protected synchronized void beginStatement( Transaction transaction ) {
        if ( interruptionPauses == 0 && !finished ) {
            transactions.add( transaction );
            // Auto-commit batches can continue with a new member after a protected commit.
            executing = true;
            check();
        }
    }


    @Override
    protected synchronized InterruptionPause beginTransactionFinish( boolean committing ) {
        if ( committing && interruptionPauses == 0 ) {
            check();
            // Once commit starts, its actual outcome wins. A subsequent execution can use the remaining budget.
            executing = false;
        }
        return pauseInterruptions();
    }


    synchronized InterruptionPause pauseInterruptions() {
        interruptionPauses++;
        clearInterrupt();
        return this::resumeInterruptions;
    }


    private synchronized void resumeInterruptions() {
        interruptionPauses--;
    }


    private void clearInterrupt() {
        if ( interrupted ) {
            Thread.interrupted();
        }
    }


    void complete() {
        PIServiceException cancellation = finish( null );
        if ( cancellation != null ) {
            throw cancellation;
        }
    }


    @Nullable PIServiceException finish( @Nullable Throwable failure ) {
        synchronized ( this ) {
            if ( finished ) {
                return timeoutFailure;
            }
            expired |= executing && deadlineNanos - System.nanoTime() <= 0;
            stop();
            if ( !expired ) {
                return null;
            }
        }
        timeoutFailure = new PIServiceException( "Query timed out.", "57014", failure );
        for ( Transaction transaction : transactions ) {
            try {
                client.rollbackTimedOutQuery( transaction );
            } catch ( RuntimeException cleanupFailure ) {
                timeoutFailure.addSuppressed( cleanupFailure );
            }
        }
        return timeoutFailure;
    }


    private synchronized void stop() {
        finished = true;
        timer.cancel( false );
        clearInterrupt();
    }


    @Override
    public void close() {
        stop();
        super.close();
    }

}
