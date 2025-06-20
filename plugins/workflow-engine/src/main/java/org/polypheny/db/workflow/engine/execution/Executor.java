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

package org.polypheny.db.workflow.engine.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

/**
 * An executor is responsible for executing a connected subgraph of a workflow.
 * Predecessor activities of activities in this subgraph are guaranteed to have finished their execution (and written their results to checkpoints and merged their variables)
 * or are itself part of this subgraph.
 * After the Executor gets instantiated, the workflow structure remains static at least until execution finishes.
 * The execution of the subgraph is atomic: it is either completely successful or fails (=throws an exception).
 * An executor is responsible for updating the variables of its non-leaf activities (including before execution starts) and the outTypePreview of all activities (in case the execution is successful).
 * An executor does not modify the state of any activities. The scheduler is responsible for this.
 */
public abstract class Executor implements Callable<Void> {

    final StorageManager sm;
    final Workflow workflow;
    final ExecutionInfo info;
    boolean isInterrupted;
    private final List<CheckpointReader> activeReaders = new ArrayList<>();


    protected Executor( StorageManager sm, Workflow workflow, ExecutionInfo info ) {
        this.sm = sm;
        this.workflow = workflow;
        this.info = info;
    }


    abstract void execute() throws ExecutorException;

    public abstract ExecutorType getType();


    /**
     * Tries to halt the execution in a best-effort manner.
     * Only when call() returns or throws an exception is the execution really terminated.
     */
    public void interrupt() {
        isInterrupted = true;
    }


    public void forceInterrupt() {
        if ( !isInterrupted ) {
            interrupt();
        }
        activeReaders.forEach( CheckpointReader::close );
    }


    @Override
    public Void call() throws ExecutorException {
        if ( !isInterrupted ) {
            execute();
        }
        return null;
    }


    List<CheckpointReader> getReaders( ActivityWrapper target ) throws ExecutorException {
        List<Edge> inEdges = workflow.getInEdges( target.getId() );
        CheckpointReader[] inputs = new CheckpointReader[target.getDef().getDynamicInPortCount( inEdges )];
        for ( Edge edge : inEdges ) {
            if ( edge instanceof DataEdge dataEdge && dataEdge.getState() != EdgeState.INACTIVE ) {
                CheckpointReader reader = sm.readCheckpoint( dataEdge.getFrom().getId(), dataEdge.getFromPort() );
                activeReaders.add( reader );
                PortType toPortType = target.getDef().getInPortType( dataEdge.getToPort() );
                inputs[dataEdge.getToPort()] = reader;
                if ( !toPortType.couldBeCompatibleWith( reader.getDataModel() ) ) {
                    for ( CheckpointReader r : inputs ) {
                        if ( r != null ) {
                            r.close();
                        }
                    }
                    throw new ExecutorException( "Detected incompatible data models: " + toPortType.getDataModel() + " and " + reader.getDataModel() );
                }
            }
        }
        return Arrays.asList( inputs );
    }


    CheckpointReader getReader( ActivityWrapper target, int toPort ) throws ExecutorException {
        DataEdge edge = workflow.getDataEdge( target.getId(), toPort );
        if ( edge == null || edge.getState() == EdgeState.INACTIVE ) {
            return null;
        }
        CheckpointReader reader = sm.readCheckpoint( edge.getFrom().getId(), edge.getFromPort() );
        activeReaders.add( reader );
        PortType toPortType = target.getDef().getInPortType( toPort );
        if ( !toPortType.couldBeCompatibleWith( reader.getDataModel() ) ) {
            reader.close();
            throw new ExecutorException( "Detected incompatible data models: " + toPortType.getDataModel() + " and " + reader.getDataModel() );
        }
        return reader;
    }


    public enum ExecutorType {
        DEFAULT( DefaultExecutor.class ),
        FUSION( FusionExecutor.class ),
        PIPE( PipeExecutor.class ),
        VARIABLE_WRITER( VariableWriterExecutor.class );

        public final Class<? extends Executor> clazz;


        ExecutorType( Class<? extends Executor> clazz ) {
            this.clazz = clazz;
        }
    }


    public static class ExecutorException extends Exception {

        private final UUID origin;


        public ExecutorException( String message ) {
            super( message );
            this.origin = null;
        }


        public ExecutorException( String message, UUID origin ) {
            super( message );
            this.origin = origin;
        }


        public ExecutorException( Throwable cause ) {
            super( cause.getMessage() );
            this.origin = null;
        }


        public ExecutorException( Throwable cause, UUID origin ) {
            super( cause.getMessage() );
            this.origin = origin;
        }


        public ExecutorException( String message, Throwable cause ) {
            super( message, cause );
            this.origin = null;
        }


        public ObjectNode getVariableValue( Set<UUID> fallbackOrigin ) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put( "message", getMessage() );
            if ( getCause() != null ) {
                node.put( "cause", getCause().getMessage() );
            }
            JsonNode originNode;
            if ( origin != null ) {
                originNode = TextNode.valueOf( origin.toString() );
            } else {
                ArrayNode arr = mapper.createArrayNode();
                fallbackOrigin.forEach( n -> arr.add( n.toString() ) );
                originNode = arr;
            }
            node.set( "origin", originNode );
            return node;
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
