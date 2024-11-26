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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

/**
 * An executor is responsible for executing a connected subgraph of a workflow.
 * Predecessor activities of activities in this subgraph are guaranteed to have finished their execution (and written their results to checkpoints)
 * or are itself part of this subgraph.
 * After the Executor gets instantiated, the workflow structure remains static at least until execution finishes.
 * The execution of the subgraph is atomic: it is either completely successful or fails (=throws an exception).
 * An executor is responsible for updating the variables of its activities (including before execution starts).
 * An executor does not modify the state of any activities. The scheduler is responsible for this.
 */
public abstract class Executor implements Callable<Void> {

    final StorageManager sm;
    final Workflow workflow;


    // TODO: add reference to monitor for monitoring the progress of activities
    protected Executor( StorageManager sm, Workflow workflow ) {
        this.sm = sm;
        this.workflow = workflow;
    }


    abstract void execute() throws ExecutorException;

    /**
     * Tries to halt the execution in a best-effort manner.
     * Only when call() returns or throws an exception is the execution really terminated.
     */
    public abstract void interrupt();


    @Override
    public Void call() throws ExecutorException {
        execute();
        return null;
    }


    /**
     * Merges all the input variableStores of the target activity and updates the target activity variableStore accordingly.
     * In the process, the target variableStore is completely reset.
     *
     * @param targetId the identifier of the target activity whose variables are going to be set based on its inputs.
     */
    void mergeInputVariables( UUID targetId ) {
        List<Edge> edges = workflow.getInEdges( targetId );

        ActivityWrapper target = workflow.getActivity( targetId );
        WritableVariableStore targetVariables = target.getVariables();
        targetVariables.clear();
        for ( Edge edge : edges ) {
            if ( edge.isActive() ) {
                ReadableVariableStore inputVariables = edge.getFrom().getVariables();
                targetVariables.merge( inputVariables );
            }
        }
    }


    List<CheckpointReader> getReaders( ActivityWrapper target ) {
        CheckpointReader[] inputs = new CheckpointReader[target.getDef().getInPorts().length];
        for ( Edge edge : workflow.getInEdges( target.getId() ) ) {
            if ( edge instanceof DataEdge dataEdge ) {
                CheckpointReader reader = sm.readCheckpoint( dataEdge.getFrom().getId(), dataEdge.getFromPort() );
                inputs[dataEdge.getToPort()] = reader;
            }
        }
        return List.of( inputs );
    }


    CheckpointReader getReader( ActivityWrapper target, int toPort ) {
        DataEdge edge = workflow.getDataEdge( target.getId(), toPort );
        if ( edge == null ) {
            return null;
        }
        return sm.readCheckpoint( edge.getFrom().getId(), edge.getFromPort() );
    }


    public static class ExecutorException extends Exception {

        // TODO: implement ExecutorException
        public ExecutorException( Throwable cause ) {
            super( cause );
        }

    }


    /**
     * Convenience class to be able to use try-with-resource with a list of readers
     */
    static class CloseableList implements AutoCloseable {

        private final List<? extends AutoCloseable> resources;


        public CloseableList( List<? extends AutoCloseable> resources ) {
            this.resources = resources;
        }


        @Override
        public void close() {
            for ( AutoCloseable resource : resources ) {
                try {
                    if ( resource != null ) {
                        resource.close();
                    }
                } catch ( Exception ignored ) {
                }
            }
        }

    }

}
