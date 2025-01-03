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

package org.polypheny.db.workflow.engine.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo.ExecutionState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;
import org.polypheny.db.workflow.models.responses.WsResponse;

@Slf4j
public class GlobalScheduler {

    private static GlobalScheduler INSTANCE;
    public static final int GLOBAL_WORKERS = 20; // TODO: use config value, allow to change it when no scheduler is running

    private final Map<UUID, WorkflowScheduler> schedulers = new HashMap<>();
    private final Map<UUID, Set<ExecutionSubmission>> activeSubmissions = new ConcurrentHashMap<>(); // used for interrupting the execution
    private final Set<UUID> interruptedSessions = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor;
    private final CompletionService<ExecutionResult> completionService;

    private Thread resultProcessor; // When no workflow is being executed, the thread may die and be replaced by a new thread when execution starts again


    private GlobalScheduler() {
        executor = new ThreadPoolExecutor( 0, GLOBAL_WORKERS,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>() );
        completionService = new ExecutorCompletionService<>( executor );
    }


    public static GlobalScheduler getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new GlobalScheduler();
        }
        return INSTANCE;
    }


    public synchronized void startExecution( Workflow workflow, StorageManager sm, @Nullable UUID targetActivity ) throws Exception {
        startExecution( workflow, sm, targetActivity, null );
    }


    public synchronized ExecutionMonitor startExecution( Workflow workflow, StorageManager sm, @Nullable UUID targetActivity, Consumer<WsResponse> monitoringCallback ) throws Exception {
        UUID sessionId = sm.getSessionId();
        if ( schedulers.containsKey( sessionId ) ) {
            throw new GenericRuntimeException( "Cannot execute a workflow that is already being executed." );
        }
        interruptedSessions.remove( sessionId );
        ExecutionMonitor monitor = new ExecutionMonitor( workflow, targetActivity, monitoringCallback );
        WorkflowScheduler scheduler;
        List<ExecutionSubmission> submissions;
        try {
            scheduler = new WorkflowScheduler( workflow, sm, monitor, GLOBAL_WORKERS, targetActivity );
            submissions = scheduler.startExecution();
        } catch ( Exception e ) {
            workflow.setState( WorkflowState.IDLE );
            monitor.stop();
            throw e;
        }
        if ( submissions.isEmpty() ) {
            throw new GenericRuntimeException( "At least one activity needs to be executable when submitting a workflow for execution" );
        }
        schedulers.put( sessionId, scheduler );

        submit( submissions );

        if ( resultProcessor == null || !resultProcessor.isAlive() ) {
            resultProcessor = startResultProcessor();
        }
        return monitor;
    }


    public synchronized void interruptExecution( UUID sessionId ) {
        WorkflowScheduler scheduler = schedulers.get( sessionId );
        if ( scheduler == null ) {
            return;
        }
        scheduler.interruptExecution(); // prevent new submissions

        interruptedSessions.add( sessionId ); // stop already enqueued submissions from starting to execute
        for ( ExecutionSubmission submission : activeSubmissions.get( sessionId ) ) {
            submission.getExecutor().interrupt(); // speed up termination of currently running executors
        }

    }


    public synchronized void shutdownNow() {
        for ( UUID sessionId : schedulers.keySet() ) {
            interruptExecution( sessionId );
        }
        resultProcessor.interrupt();
        try {
            resultProcessor.join( 5000 );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
        executor.shutdownNow();

    }


    public void awaitResultProcessor( long millis ) throws InterruptedException {
        if ( resultProcessor == null ) {
            return;
        }
        resultProcessor.join( millis );
    }


    private void submit( List<ExecutionSubmission> submissions ) {
        for ( ExecutionSubmission submission : submissions ) {
            log.info( "Submitting {}", submission );
            UUID sessionId = submission.getSessionId();

            completionService.submit( () -> {
                log.info( "Begin actual execution {}", submission );
                submission.getInfo().setState( ExecutionState.EXECUTING );
                if ( interruptedSessions.contains( sessionId ) ) {
                    return new ExecutionResult( submission, new ExecutorException( "Execution was interrupted before it started" ) );
                }
                if ( submission.getCommonType() != CommonType.NONE && !schedulers.get( sessionId ).isCommonActive( submission.getCommonType() ) ) {
                    return new ExecutionResult( submission, new ExecutorException( "Common transaction was aborted" ) );
                }

                activeSubmissions.computeIfAbsent( sessionId, k -> ConcurrentHashMap.newKeySet() ).add( submission );

                ExecutionResult result;
                try {
                    submission.getExecutor().call();
                    result = new ExecutionResult( submission );
                } catch ( ExecutorException e ) {
                    result = new ExecutionResult( submission, e );
                } catch ( Throwable e ) {
                    result = new ExecutionResult( submission, new ExecutorException( "Unexpected exception", e ) );
                }
                activeSubmissions.get( sessionId ).remove( submission );
                submission.getInfo().setState( ExecutionState.AWAIT_PROCESSING );
                return result;
            } );
        }
    }


    private Thread startResultProcessor() {
        Thread t = new Thread( () -> {
            while ( true ) {
                List<ExecutionSubmission> nextSubmissions;
                ExecutionInfo info = null;
                try {
                    ExecutionResult result = completionService.take().get();
                    log.info( "processing next result: " + result );
                    info = result.getInfo();
                    info.setState( ExecutionState.PROCESSING_RESULT );
                    WorkflowScheduler scheduler = schedulers.get( result.getSessionId() );
                    nextSubmissions = scheduler.handleExecutionResult( result );

                    if ( scheduler.isFinished() ) {
                        schedulers.remove( result.getSessionId() );
                    }
                } catch ( InterruptedException e ) {
                    break; // interrupted by shutdownNow
                } catch ( ExecutionException e ) {
                    log.warn( "Scheduler result processor has encountered an unhandled exception: ", e );
                    throw new RuntimeException( e );
                } finally {
                    if ( info != null ) {
                        info.setState( ExecutionState.DONE );
                        log.info( info.toString() );
                    }
                }

                if ( nextSubmissions == null || nextSubmissions.isEmpty() ) {
                    if ( schedulers.isEmpty() ) {
                        break; // thread stops when no more schedulers are active
                    }
                } else {
                    submit( nextSubmissions );
                }
            }
            log.info( "Processor is finished" );
        } );
        t.start();
        return t;
    }

}
