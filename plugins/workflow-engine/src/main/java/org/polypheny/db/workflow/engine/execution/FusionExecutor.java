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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

/**
 * Executes a subgraph representing a set of fused activities, meaning they implement {@link org.polypheny.db.workflow.dag.activities.Fusable}
 * and {@code canFuse} is {@code true}.
 * The subgraph has the structure of an inverted tree.
 * The root of this tree is therefore the only activity in the tree with no successors.
 * Any edge in the tree is guaranteed to be a data edge.
 */
public class FusionExecutor extends Executor {

    private final AttributedDirectedGraph<UUID, ExecutionEdge> execTree;
    private final UUID rootId;


    public FusionExecutor( StorageManager sm, Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execTree, UUID rootId ) {
        super( sm, workflow );
        this.execTree = execTree;
        this.rootId = rootId;
    }


    @Override
    void execute() throws ExecutorException {
        System.out.println( "Start execution fused tree: " + execTree );

        try {
            // TODO: implement after PolyAlgebra is merged
            //AlgNode node = constructAlgNode( rootId, cluster );

            // 0. (verify node does not perform data manipulation)
            // 1. exec node with TranslatedQueryContext
            // 2. get result iterator
            // 3. write result to checkpoint
        } catch ( Exception e ) {
            // TODO: handle exception
        }

        throw new NotImplementedException();

    }


    @Override
    ExecutorType getType() {
        return ExecutorType.FUSION;
    }


    @Override
    public void interrupt() {
        super.interrupt();
        throw new NotImplementedException();
    }


    private AlgNode constructAlgNode( UUID root, AlgCluster cluster ) throws Exception {
        ActivityWrapper wrapper = workflow.getActivity( root );
        List<ExecutionEdge> inEdges = execTree.getInwardEdges( root );
        AlgNode[] inputsArr = new AlgNode[wrapper.getDef().getInPorts().length];
        for ( ExecutionEdge edge : inEdges ) {
            assert !edge.isControl() : "Execution tree for fusion must not contain control edges";
            inputsArr[edge.getToPort()] = constructAlgNode( edge.getSource(), cluster );
        }
        for ( int i = 0; i < inputsArr.length; i++ ) {
            if ( inputsArr[i] == null ) {
                // add remaining inputs for existing checkpoints
                CheckpointReader reader = getReader( wrapper, i );
                inputsArr[i] = reader == null ? null : reader.getAlgNode( cluster );
            }
        }
        List<AlgNode> inputs = Arrays.asList( inputsArr );

        if ( !inEdges.isEmpty() ) {
            workflow.recomputeInVariables( root ); // inner nodes should get their variables merged
        }

        Settings settings = wrapper.resolveSettings();
        Fusable activity = (Fusable) wrapper.getActivity();

        AlgNode fused = activity.fuse( inputs, settings, cluster );
        wrapper.setOutTypePreview( List.of( Optional.of( fused.getTupleType() ) ) );
        return fused;
    }

}
