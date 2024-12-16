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

package org.polypheny.db.workflow.engine.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointOutputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.QueuePipe;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

/**
 * Each activity is executed as a separate thread.
 * Data is moved between threads using pipes.
 * Checkpoints are read and written in the activity thread that uses them, using special checkpoint pipes.
 */
@Slf4j
public class PipeExecutor extends Executor {

    private final AttributedDirectedGraph<UUID, ExecutionEdge> execTree;
    private final UUID rootId;
    private final Map<UUID, QueuePipe> outQueues = new HashMap<>(); // maps activities to their (only!) output queue
    private final Map<UUID, Settings> settingsSnapshot = new HashMap<>();
    private final int queueCapacity;

    private boolean hasDetectedAbort = false;
    private ExecutorService executor;


    public PipeExecutor( StorageManager sm, Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execTree, UUID rootId, int queueCapacity ) {
        super( sm, workflow );
        this.execTree = execTree;
        this.rootId = rootId;
        this.queueCapacity = queueCapacity;

        assert execTree.vertexSet().size() > 1 : "A PipeExecutor is not suited for the execution of a single activity, since the CheckpointPipes do not check for interrupts.";
    }


    @Override
    void execute() throws ExecutorException {
        List<Callable<Void>> callables;
        try {
            callables = getCallables();
        } catch ( Exception e ) {
            throw new ExecutorException( e );
        }

        // Start tasks
        ExecutorService executor = Executors.newFixedThreadPool( callables.size() ); // we need as many threads as activities, otherwise we could get a deadlock
        CompletionService<Void> completionService = new ExecutorCompletionService<>( executor );
        List<Future<Void>> futures = new ArrayList<>();
        for ( Callable<Void> callable : callables ) {
            log.info( "1. Pipe submits callable " + callable );
            futures.add( completionService.submit( callable ) );
        }
        this.executor = executor; // store in field for manual interrupt

        // Wait for tasks to complete (either succeed or throw an exception)
        ExecutorException abortReason = null;
        while ( !futures.isEmpty() ) {
            try {
                Future<Void> f = completionService.take();
                futures.remove( f );
                log.info( "3.1 Processing next completed pipe activity " );

                try {
                    f.get();
                } catch ( ExecutionException e ) {
                    if ( !hasDetectedAbort ) {
                        log.warn( "First pipe has detected abort, cancelling all" );
                        // At this point, we cannot be sure if there are multiple failed tasks.
                        // This is not a problem, we just handle the first one we encounter and use it as a reason for the abort.
                        hasDetectedAbort = true;
                        futures.forEach( future -> future.cancel( true ) ); // ensure all remaining tasks are cancelled
                        abortReason = new ExecutorException( e.getCause() );
                    }
                    // only the first task to throw an exception is relevant
                } catch ( CancellationException ignored ) {
                    log.warn( "Another pipe has detected abort" );
                    assert hasDetectedAbort; // we already cancelled the other tasks and don't have to do anything
                }
                log.info( "3.2 Pipe activity processing finished" );

            } catch ( InterruptedException ignored ) {
                // THe PipeExecutor thread itself should never be interrupted
            }

        }

        executor.shutdownNow();
        log.info( "4. All threads have finished" );

        if ( abortReason != null ) {
            throw abortReason; // we only throw now to ensure threads are all shut down.
        }
    }


    @Override
    public ExecutorType getType() {
        return ExecutorType.PIPE;
    }


    @Override
    public void interrupt() {
        super.interrupt();
        if ( !hasDetectedAbort && executor != null ) {
            executor.shutdownNow();
        }
    }


    private AlgDataType registerOutputPipes( UUID root ) throws Exception {
        ActivityWrapper wrapper = workflow.getActivity( root );

        List<ExecutionEdge> inEdges = execTree.getInwardEdges( root );
        AlgDataType[] inTypes = new AlgDataType[wrapper.getDef().getInPorts().length];
        boolean isInnerNode = false;
        for ( ExecutionEdge edge : inEdges ) {
            assert !edge.isControl() : "Execution tree for pipelining must not contain control edges";
            inTypes[edge.getToPort()] = registerOutputPipes( edge.getSource() );
            isInnerNode = true;
        }
        for ( int i = 0; i < inTypes.length; i++ ) {
            if ( inTypes[i] == null ) {
                // existing checkpoint
                CheckpointReader reader = getReader( wrapper, i );
                inTypes[i] = reader == null ? null : reader.getTupleType(); // null implies inactive data edge
            }
        }
        if ( isInnerNode ) { // leaf nodes already have correct variables from scheduler
            workflow.recomputeInVariables( root );
        }

        Settings settings = wrapper.resolveSettings();
        settingsSnapshot.put( root, settings ); // store current state of settings for later use
        Pipeable activity = (Pipeable) wrapper.getActivity();

        AlgDataType outType = activity.lockOutputType( Arrays.asList( inTypes ), settings );
        if ( outType != null ) {
            outQueues.put( root, new QueuePipe( queueCapacity, outType ) ); // TODO: adapt queue capacity to tuple size?
            wrapper.setOutTypePreview( List.of( Optional.of( outType ) ) );
        } else {
            // we are at the actual root of the tree, and it's an activity with no outputs.
            wrapper.setOutTypePreview( List.of() );
        }
        return outType;
    }


    private OutputPipe getCheckpointWriterPipe( UUID rootId, AlgDataType rootType ) {
        if ( rootType == null ) {
            // This could be a LOAD activity. It generally has no outputs and instead uses side effects to load the data -> no pipe required
            return null;
        }
        ActivityWrapper wrapper = workflow.getActivity( rootId );
        DataModel model = wrapper.getDef().getOutPortTypes()[0].getDataModel(); // TODO: handle ANY DataModel
        String store = wrapper.getConfig().getPreferredStore( 0 );

        CheckpointWriter writer = sm.createCheckpoint( rootId, 0, rootType, true, store, model );
        return new CheckpointOutputPipe( rootType, writer );
    }


    private List<Callable<Void>> getCallables() throws Exception {

        AlgDataType rootType = registerOutputPipes( rootId );

        List<Callable<Void>> callables = new ArrayList<>();
        for ( UUID currentId : TopologicalOrderIterator.of( execTree ) ) {
            ActivityWrapper wrapper = workflow.getActivity( currentId );
            List<ExecutionEdge> inEdges = execTree.getInwardEdges( currentId );

            InputPipe[] inPipesArr = new InputPipe[wrapper.getDef().getInPorts().length];
            for ( ExecutionEdge edge : inEdges ) {
                assert !edge.isControl() : "Execution tree for pipelining must not contain control edges";
                inPipesArr[edge.getToPort()] = outQueues.get( edge.getSource() );
            }
            for ( int i = 0; i < inPipesArr.length; i++ ) {
                if ( inPipesArr[i] == null ) {
                    // add remaining pipes for existing checkpoints
                    CheckpointReader reader = getReader( wrapper, i );
                    inPipesArr[i] = reader == null ? null : new CheckpointInputPipe( reader );
                }
            }

            List<InputPipe> inPipes = Arrays.asList( inPipesArr );
            OutputPipe outPipe = currentId.equals( rootId ) ? getCheckpointWriterPipe( rootId, rootType ) : outQueues.get( wrapper.getId() );
            callables.add( getCallable( wrapper, inPipes, outPipe ) );
        }
        return callables;
    }


    private Callable<Void> getCallable( ActivityWrapper wrapper, List<InputPipe> inPipes, OutputPipe outPipe ) {
        Settings settings = settingsSnapshot.get( wrapper.getId() );
        Pipeable activity = (Pipeable) wrapper.getActivity();
        PipeExecutionContext ctx = new ExecutionContextImpl( wrapper, sm );
        return () -> {
            log.info( "2.1 Starting execution of " + wrapper );
            try {
                if ( outPipe != null ) {
                    try ( outPipe ) { // try-with-resource to close pipe (in case of the CheckpointOutputPipe, this closes the checkpoint writer)
                        activity.pipe( inPipes, outPipe, settings, ctx );
                    }
                } else {
                    activity.pipe( inPipes, null, settings, ctx );
                }
            } finally {
                for ( InputPipe inputPipe : inPipes ) {
                    if ( inputPipe instanceof CheckpointInputPipe closeable ) {
                        // Closes any checkpoint readers that this activity is responsible for
                        try {
                            closeable.close();
                        } catch ( Exception ignored ) {
                        }
                    }
                }
                ctx.updateProgress( 1 );
            }
            log.info( "2.2 Finished execution of " + wrapper );
            return null;
        };
    }

}
