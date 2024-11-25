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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointOutputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.QueuePipe;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.scheduler.GraphUtils;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.StorageManager;

/**
 * Each activity is executed as a separate thread.
 * Data is moved between threads using pipes.
 * Checkpoints are read and written in the activity thread that uses them, using special checkpoint pipes.
 */
public class PipeExecutor extends Executor {

    private final AttributedDirectedGraph<UUID, ExecutionEdge> execTree; // TODO: just use Set<UUID> instead?
    private final Map<UUID, QueuePipe> outQueues = new HashMap<>(); // maps activities to their (only) output queue
    private final Map<UUID, Map<String, SettingValue>> settingsSnapshot = new HashMap<>();
    private final int queueCapacity;


    protected PipeExecutor( StorageManager sm, Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execTree, int queueCapacity ) {
        super( sm, workflow );
        this.execTree = execTree;
        this.queueCapacity = queueCapacity;
    }


    @Override
    public void execute() throws ExecutorException {
        UUID rootId = GraphUtils.findInvertedTreeRoot( execTree );

        AlgDataType rootType = registerOutputPipes( rootId );

        List<Thread> threads = new ArrayList<>();
        for ( UUID currentId : TopologicalOrderIterator.of( execTree ) ) {
            ActivityWrapper wrapper = workflow.getActivity( currentId );
            List<ExecutionEdge> inEdges = execTree.getInwardEdges( currentId );
            List<InputPipe> inPipes;
            if ( inEdges.isEmpty() ) {
                // leaf node

                // TODO: handle nodes with 1 input connected to other AlgNode, the other to checkpoint
                inPipes = getCheckpointReaderPipes( wrapper );
            } else {
                inPipes = inEdges.stream().map( e -> (InputPipe) outQueues.get( e.getSource() ) ).toList();
            }

            Pipeable activity = (Pipeable) wrapper.getActivity();
            ExecutionContext ctx = new ExecutionContext( wrapper, sm );

            OutputPipe outPipe = currentId.equals( rootId ) ? getCheckpointWriterPipe( rootId, rootType ) : outQueues.get( wrapper.getId() );

            threads.add( new Thread( () -> {
                try ( outPipe ) {
                    activity.pipe( inPipes, outPipe, settingsSnapshot.get( wrapper.getId() ), ctx );

                } catch ( Exception e ) {
                    // TODO: handle Exception, possibly use Callable instead
                    throw new RuntimeException( e );
                } finally {
                    for ( InputPipe inputPipe : inPipes ) {
                        if ( inputPipe instanceof CheckpointInputPipe closeable ) {
                            try {
                                closeable.close();
                            } catch ( Exception ignored ) {
                            }
                        }
                    }
                }
            } ) );
        }

        // start threads:
        for ( Thread t : threads ) {
            t.start();
        }
    }


    @Override
    public void interrupt() {
        throw new NotImplementedException();
    }


    private AlgDataType registerOutputPipes( UUID rootId ) {
        ActivityWrapper wrapper = workflow.getActivity( rootId );

        List<ExecutionEdge> inEdges = execTree.getInwardEdges( rootId );
        List<AlgDataType> inTypes;
        if ( inEdges.isEmpty() ) {
            // leaf node
            inTypes = getReaders( wrapper ).stream().map( CheckpointReader::getTupleType ).toList();
        } else {
            // inner node
            inTypes = new ArrayList<>();
            for ( ExecutionEdge e : inEdges ) {
                inTypes.add( registerOutputPipes( e.getSource() ) );
            }
        }

        mergeInputVariables( rootId );

        Map<String, SettingValue> settings = wrapper.resolveSettings();
        settingsSnapshot.put( rootId, settings ); // store current state of settings for later use, since updateVariables might change it
        Pipeable activity = (Pipeable) wrapper.getActivity();

        activity.updateVariables( inTypes, settings, wrapper.getVariables() );
        AlgDataType outType = activity.lockOutputType( inTypes, settings );
        outQueues.put( rootId, new QueuePipe( queueCapacity, outType ) );
        return outType;
    }


    private List<InputPipe> getCheckpointReaderPipes( ActivityWrapper leaf ) {
        List<InputPipe> pipes = new ArrayList<>();
        for ( CheckpointReader reader : getReaders( leaf ) ) {
            pipes.add( new CheckpointInputPipe( reader ) );
        }
        return pipes;
    }


    private OutputPipe getCheckpointWriterPipe( UUID rootId, AlgDataType rootType ) {
        ActivityWrapper wrapper = workflow.getActivity( rootId );
        DataModel model = wrapper.getDef().getOutPortTypes()[0].getDataModel();
        String store = wrapper.getConfig().getPreferredStore( 0 );

        CheckpointWriter writer = sm.createCheckpoint( rootId, 0, rootType, true, store, model );
        return new CheckpointOutputPipe( rootType, writer );
    }

}
