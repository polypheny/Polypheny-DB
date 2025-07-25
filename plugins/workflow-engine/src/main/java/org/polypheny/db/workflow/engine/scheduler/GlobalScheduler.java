/*
 * Copyright 2019-2025 The Polypheny Project
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo.ExecutionState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;
import org.polypheny.db.workflow.models.responses.WsResponse;
import org.polypheny.db.workflow.session.NestedSessionManager;

@Slf4j
public class GlobalScheduler {

    private static final GlobalScheduler INSTANCE = new GlobalScheduler();
    private int globalWorkerCount = Math.max( 1, RuntimeConfig.WORKFLOWS_WORKERS.getInteger() );

    private final Map<UUID, WorkflowScheduler> schedulers = new HashMap<>();
    private final Map<UUID, Set<ExecutionSubmission>> activeSubmissions = new ConcurrentHashMap<>(); // used for interrupting the execution
    private final Set<UUID> interruptedSessions = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor executor;
    private final CompletionService<ExecutionResult> completionService;
    private final ScheduledExecutorService timeoutService;

    private Thread resultProcessor; // When no workflow is being executed, the thread may die and be replaced by a new thread when execution starts again


    private GlobalScheduler() {
        executor = new ThreadPoolExecutor( 0, globalWorkerCount,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>() );
        completionService = new ExecutorCompletionService<>( executor );
        timeoutService = new ScheduledThreadPoolExecutor( 1 );
    }


    public static GlobalScheduler getInstance() {
        return INSTANCE;
    }


    public synchronized void startExecution( Workflow workflow, StorageManager sm, @Nullable UUID targetActivity ) throws Exception {
        startExecution( workflow, sm, null, targetActivity, null );
    }


    public synchronized ExecutionMonitor startExecution( Workflow workflow, StorageManager sm, @Nullable NestedSessionManager nestedManager, @Nullable UUID targetActivity, Consumer<WsResponse> monitoringCallback ) throws Exception {
        UUID sessionId = sm.getSessionId();
        if ( schedulers.containsKey( sessionId ) ) {
            throw new GenericRuntimeException( "Cannot execute a workflow that is already being executed." );
        }
        interruptedSessions.remove( sessionId );
        ExecutionMonitor monitor = new ExecutionMonitor( workflow, targetActivity, monitoringCallback );

        if ( schedulers.isEmpty() ) {
            updateWorkerCount(); // only update thread pool size if nothing is being executed
        }
        WorkflowScheduler scheduler;
        List<ExecutionSubmission> submissions;
        try {
            scheduler = new WorkflowScheduler( workflow, sm, nestedManager, monitor, globalWorkerCount, targetActivity );
            submissions = scheduler.startExecution();
        } catch ( Exception e ) {
            workflow.resetFailedExecutionInit( sm );
            monitor.stop( false );
            monitor.setReadyForNextExecution();
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


    public synchronized void forceInterruptExecution( UUID sessionId ) {
        if ( !interruptedSessions.contains( sessionId ) ) {
            throw new GenericRuntimeException( "Force Interrupt can only be called after a regular interrupt attempt" );
        }
        WorkflowScheduler scheduler = schedulers.get( sessionId );
        if ( scheduler == null ) {
            return;
        }
        for ( ExecutionSubmission submission : activeSubmissions.get( sessionId ) ) {
            submission.getExecutor().forceInterrupt();
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
            throw new GenericRuntimeException( e );
        }
        executor.shutdownNow();

    }


    public void awaitResultProcessor( long millis ) throws InterruptedException {
        if ( resultProcessor == null ) {
            return;
        }
        resultProcessor.join( millis );
    }


    public boolean awaitExecutionFinish( UUID sessionId, int seconds ) throws InterruptedException {
        WorkflowScheduler scheduler = schedulers.get( sessionId );
        if ( scheduler == null || scheduler.isFinished() ) {
            return true;
        }
        return scheduler.awaitFinish( seconds );
    }


    private void submit( List<ExecutionSubmission> submissions ) {
        for ( ExecutionSubmission submission : submissions ) {
            UUID sessionId = submission.getSessionId();

            completionService.submit( () -> {
                submission.getInfo().setState( ExecutionState.EXECUTING );
                if ( interruptedSessions.contains( sessionId ) ) {
                    return new ExecutionResult( submission, new ExecutorException( "Execution was interrupted before it started" ) );
                }
                if ( submission.getCommonType() != CommonType.NONE && !schedulers.get( sessionId ).isCommonActive( submission.getCommonType() ) ) {
                    return new ExecutionResult( submission, new ExecutorException( "Common transaction was aborted" ) );
                }

                activeSubmissions.computeIfAbsent( sessionId, k -> ConcurrentHashMap.newKeySet() ).add( submission );

                int timeoutSeconds = submission.getTimeoutSeconds();
                if ( timeoutSeconds > 0 ) {
                    timeoutService.schedule( () -> {
                        if ( submission.getInfo().getState() == ExecutionState.EXECUTING ) {
                            submission.getExecutor().interrupt();
                        }
                    }, timeoutSeconds, TimeUnit.SECONDS );
                }

                ExecutionResult result;
                try {
                    submission.getExecutor().call();
                    result = new ExecutionResult( submission );
                } catch ( ExecutorException e ) {
                    result = new ExecutionResult( submission, e );
                } catch ( Throwable e ) {
                    result = new ExecutionResult( submission, new ExecutorException( "Unexpected exception: " + e.getMessage(), e ) );
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
                    info = result.getInfo();
                    info.setState( ExecutionState.PROCESSING_RESULT );
                    WorkflowScheduler scheduler = schedulers.get( result.getSessionId() );
                    nextSubmissions = scheduler.handleExecutionResult( result );

                    if ( scheduler.isFinished() ) {
                        schedulers.remove( result.getSessionId() );
                        scheduler.getExecutionMonitor().setReadyForNextExecution();
                    }
                } catch ( InterruptedException e ) {
                    break; // interrupted by shutdownNow
                } catch ( ExecutionException e ) {
                    log.warn( "Scheduler result processor has encountered an unhandled exception: ", e );
                    throw new GenericRuntimeException( e );
                } finally {
                    if ( info != null ) {
                        info.setState( ExecutionState.DONE );
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
        } );
        t.start();
        return t;
    }


    private void updateWorkerCount() {
        globalWorkerCount = Math.max( 1, RuntimeConfig.WORKFLOWS_WORKERS.getInteger() );
        if ( !executor.isShutdown() ) {
            executor.setMaximumPoolSize( globalWorkerCount );
        }
    }

}
