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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo.LogLevel;
import org.polypheny.db.workflow.engine.scheduler.ExecutionEdge;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

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


    public FusionExecutor( StorageManager sm, Workflow workflow, AttributedDirectedGraph<UUID, ExecutionEdge> execTree, UUID rootId, ExecutionInfo info ) {
        super( sm, workflow, info );
        this.execTree = execTree;
        this.rootId = rootId;
    }


    @Override
    void execute() throws ExecutorException {
        ActivityWrapper rootWrapper = workflow.getActivity( rootId );
        Transaction transaction = sm.getTransaction( rootId, rootWrapper.getConfig().getCommonType() );
        Statement statement = transaction.createStatement();
        AlgCluster cluster = AlgCluster.create(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ),
                null,
                statement.getDataContext().getSnapshot() );

        AlgRoot root;
        long estimatedTupleCount;
        try {
            Pair<AlgNode, Long> pair = constructAlgNode( rootId, cluster, transaction );
            root = AlgRoot.of( pair.left, Kind.SELECT );
            estimatedTupleCount = pair.right;
        } catch ( Exception e ) {
            e.printStackTrace();
            throw new ExecutorException( e );
        }
        DataModel model = root.getModel().dataModel();
        PortType definedType = rootWrapper.getDef().getOutPortType( 0 );
        if ( !definedType.couldBeCompatibleWith( model ) ) {
            throw new ExecutorException( "The data model of the fused AlgNode tree (" + model + ") is incompatible with the defined outPort type (" + definedType + ") of the root activity: " + execTree );
        }

        if ( !QueryUtils.validateAlg( root, false, null ) ) {
            throw new ExecutorException( "The fused AlgNode tree may not perform data manipulation: " + execTree );
        }

        ExecutedContext executedContext = QueryUtils.executeAlgRoot( root, statement );
        if ( executedContext.getException().isPresent() ) {
            throw new ExecutorException( "An error occurred while executing the fused activities: " + execTree );
        }

        long countDelta = Math.max( estimatedTupleCount / 100, 1 );
        long count = 0;
        Iterator<PolyValue[]> iterator = executedContext.getIterator().getIterator();
        try ( CheckpointWriter writer = sm.createCheckpoint( rootId, 0, root.validatedRowType, true, rootWrapper.getConfig().getPreferredStore( 0 ), model ) ) {
            while ( iterator.hasNext() ) {
                writer.write( Arrays.asList( iterator.next() ) );
                count++;
                if ( estimatedTupleCount > 0 && count % countDelta == 0 ) {
                    info.setProgress( (double) count / estimatedTupleCount ); // we estimate progress only by number of produced output tuples -> the true progress would be higher
                }
                if ( isInterrupted ) {
                    info.appendLog( rootId, LogLevel.WARN, "Fusion execution was interrupted." );
                    throw new ExecutorException( "Fusion execution was interrupted" );
                }
            }
            info.setProgress( 1 );
            info.setTuplesWritten( writer.getWriteCount() );
        } catch ( Exception e ) {
            e.printStackTrace();
            if ( e instanceof ExecutorException ex ) {
                throw ex;
            }
            throw new ExecutorException( e );
        } finally {
            executedContext.getIterator().close();
        }

    }


    @Override
    public ExecutorType getType() {
        return ExecutorType.FUSION;
    }


    @Override
    public void interrupt() {
        super.interrupt();
    }


    private Pair<AlgNode, Long> constructAlgNode( UUID root, AlgCluster cluster, Transaction transaction ) throws Exception {
        ActivityWrapper wrapper = workflow.getActivity( root );
        List<ExecutionEdge> inEdges = execTree.getInwardEdges( root );
        AlgNode[] inputsArr = new AlgNode[wrapper.getDef().getDynamicInPortCount( workflow.getInEdges( root ) )];
        Long[] inCountsArr = new Long[inputsArr.length];
        for ( ExecutionEdge edge : inEdges ) {
            assert !edge.isControl() : "Execution tree for fusion must not contain control edges";
            Pair<AlgNode, Long> pair = constructAlgNode( edge.getSource(), cluster, transaction );
            inputsArr[edge.getToPort()] = pair.left;
            inCountsArr[edge.getToPort()] = pair.right;

            PortType toPortType = wrapper.getDef().getInPortType( edge.getToPort() );
            DataModel fromModel = pair.left.getModel();
            if ( !toPortType.couldBeCompatibleWith( fromModel ) ) {
                throw new ExecutorException( "Detected incompatible data models for " + wrapper.getType() + ": " + toPortType.getDataModel() + " and " + fromModel );
            }
        }
        for ( int i = 0; i < inputsArr.length; i++ ) {
            if ( inputsArr[i] == null ) {
                // add remaining inputs for existing checkpoints
                try ( CheckpointReader reader = getReader( wrapper, i ) ) {
                    inputsArr[i] = reader == null ? null : reader.getAlgNode( cluster );
                    inCountsArr[i] = reader == null ? null : reader.getTupleCount();
                }
            }
        }
        List<AlgNode> inputs = Arrays.asList( inputsArr );
        List<AlgDataType> inTypes = inputs.stream().map( AlgNode::getTupleType ).toList();

        if ( !inEdges.isEmpty() ) {
            workflow.recomputeInVariables( root ); // inner nodes should get their variables merged
        }

        Settings settings = wrapper.resolveSettings();
        Fusable activity = (Fusable) wrapper.getActivity();

        List<Long> inCounts = Arrays.asList( inCountsArr );
        long tupleCount = activity.estimateTupleCount( inTypes, settings, inCounts, () -> transaction );
        info.appendLog( root, LogLevel.INFO, "Fusing with settings: " + settings.serialize() );
        try ( ExecutionContextImpl ctx = new ExecutionContextImpl( wrapper, sm, info, inCounts ) ) {
            // closing a FuseExecutionContext is currently not required, but we want to be sure.
            AlgNode fused = activity.fuse( inputs, settings, cluster, ctx );
            wrapper.mergeOutTypePreview( List.of( fused.getTupleType() ) );
            return Pair.of( fused, tupleCount );
        }
    }

}
