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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizer;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizer.SubmissionFactory;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizerImpl;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;

/**
 * The scheduler takes a workflow and a (optional) target activitiy.
 * It determines what activities have to be executed in order to execute the target activity or all leaf nodes of the workflow, if no target was specified.
 * The scheduler uses the fact that activities with checkpoints do not have to be executed again (if the entire workflow should be executed again, it first needs to be reset).
 * <p>
 * We can assume that only a single thread (the GlobalScheduler coordinator) is accessing methods of a
 * WorkflowScheduler instance after its construction.
 * Sole control over the provided Workflow and StorageManager is given to the scheduler from creation up to the point the execution is finished.
 */
@Slf4j
public class WorkflowScheduler {

    private final Workflow workflow;
    private final StorageManager sm;
    private final int maxWorkers;
    private final AttributedDirectedGraph<UUID, ExecutionEdge> execDag;
    private final WorkflowOptimizer optimizer;
    private final ExecutionMonitor executionMonitor;

    // TODO: define overall success or failure of workflow execution, e.g. with "mustSucceed" flag in activity
    private boolean isAborted; // by interruption
    private boolean isFinished;

    private int pendingCount = 0; // current number of unfinished submissions
    private final Set<UUID> remainingActivities = new HashSet<>(); // activities that have not finished execution
    private final Map<CommonType, Partition> partitions = new HashMap<>();
    private Partition activePartition;


    public WorkflowScheduler( Workflow workflow, StorageManager sm, ExecutionMonitor monitor, int globalWorkers, @Nullable UUID targetActivity ) throws Exception {
        log.info( "Instantiating WorkflowScheduler with target: {}", targetActivity );
        this.workflow = workflow;
        this.sm = sm;
        this.executionMonitor = monitor;
        this.maxWorkers = Math.min( workflow.getConfig().getMaxWorkers(), globalWorkers );

        if ( targetActivity != null && workflow.getActivity( targetActivity ).getState() == ActivityState.SAVED ) {
            throw new GenericRuntimeException( "A saved activity first needs to be reset before executing it" );
        }

        this.execDag = targetActivity == null ? prepareExecutionDag() : prepareExecutionDag( List.of( targetActivity ) );
        log.info( "ExecDag after initialization: {}", this.execDag );

        workflow.validateStructure( sm, this.execDag );
        log.info( "Structure is valid" );

        workflow.setState( WorkflowState.EXECUTING );
        this.optimizer = new WorkflowOptimizerImpl( workflow, execDag );

    }


    public List<ExecutionSubmission> startExecution() {
        List<ExecutionSubmission> submissions = computeNextSubmissions();
        executionMonitor.forwardStates();
        return submissions;
    }


    public void interruptExecution() {
        isAborted = true;
        workflow.setState( WorkflowState.INTERRUPTED );
        activePartition.abort();
    }


    /**
     * Check whether the results of all submitted execution submissions
     * have been handled and no more submissions will ever be made by
     * this scheduler.
     * <p>
     * Note that the scheduler is not immediately finished after interrupting its execution,
     * but only after handling the results of the existing submissions.
     *
     * @return true if this scheduler is finished
     */
    public boolean isFinished() {
        return isFinished;
    }


    public boolean isCommonActive( @NonNull CommonType commonType ) {
        return sm.isCommonActive( commonType );
    }


    private AttributedDirectedGraph<UUID, ExecutionEdge> prepareExecutionDag() throws Exception {
        List<UUID> targets = new ArrayList<>();
        for ( ActivityWrapper wrapper : workflow.getActivities() ) {
            UUID id = wrapper.getId();
            if ( workflow.getOutEdges( id ).isEmpty() && wrapper.getState() != ActivityState.SAVED ) {
                targets.add( id );
            }
        }
        return prepareExecutionDag( targets );
    }


    private AttributedDirectedGraph<UUID, ExecutionEdge> prepareExecutionDag( List<UUID> targets ) throws Exception {
        System.out.println( "targets: " + targets );
        if ( targets.isEmpty() ) {
            throw new GenericRuntimeException( "Cannot prepare executionDag for empty targets" );
        }
        Set<UUID> savedActivities = new HashSet<>();
        Queue<UUID> open = new LinkedList<>( targets );
        Set<UUID> visited = new HashSet<>();

        // perform reverse DFS from targets to saved nodes
        while ( !open.isEmpty() ) {
            UUID n = open.remove();
            if ( visited.contains( n ) ) {
                continue;
            }
            visited.add( n );

            ActivityWrapper nWrapper = workflow.getActivity( n );
            CommonType type = nWrapper.getConfig().getCommonType(); // TODO: what if common activities partially executed? -> execute all of them again or only new ones, or fail?
            if ( nWrapper.getState() == ActivityState.SAVED ) {
                savedActivities.add( n );
                continue;
            }

            nWrapper.resetExecution();
            nWrapper.setState( ActivityState.QUEUED );
            sm.dropCheckpoints( n );
            remainingActivities.add( n );

            List<Edge> inEdges = workflow.getInEdges( n );
            for ( Edge edge : inEdges ) {
                edge.setState( EdgeState.IDLE );
                open.add( edge.getFrom().getId() );
            }
        }

        AttributedDirectedGraph<UUID, ExecutionEdge> execDag = GraphUtils.getInducedSubgraph( workflow.toDag(), visited );

        // handle saved activities (= simulate that they finish their execution successfully)
        for ( UUID saved : savedActivities ) {
            updateGraph( true, Set.of( saved ), saved, execDag ); // result propagation needs to happen individually
        }

        for ( UUID n : TopologicalOrderIterator.of( execDag ) ) {
            workflow.updateValidPreview( n );
        }

        for ( CommonType type : List.of( CommonType.EXTRACT, CommonType.NONE, CommonType.LOAD ) ) {
            Partition partition = new Partition( type, execDag );
            partitions.put( type, partition );
            if ( activePartition == null && !partition.isFinished ) {
                activePartition = partition;
                activePartition.start();
            }
        }

        return execDag;
    }


    public List<ExecutionSubmission> handleExecutionResult( ExecutionResult result ) {
        pendingCount--;

        try {
            updateTransactions( result );
        } catch ( TransactionException e ) {
            result = new ExecutionResult( result.getSubmission(), new ExecutorException( "An error occurred while closing open transactions of executed activities", e ) );
        }

        if ( !result.isSuccess() ) {
            sm.dropCheckpoints( result.getRootId() ); // remove any created checkpoints
            // for debugging
            result.getException().printStackTrace();
            setErrorVariable( result.getActivities(), result.getException() );
        }
        log.info( "Root variables: " + workflow.getActivity( result.getRootId() ).getVariables() );

        updateGraph( result.isSuccess(), result.getActivities(), result.getRootId(), execDag );
        updatePartitions();
        executionMonitor.forwardStates();

        log.warn( "Remaining activities: " + remainingActivities );

        if ( remainingActivities.isEmpty() ) {
            assert pendingCount == 0;
            setFinished();
            return null;
        }

        if ( isAborted ) {
            if ( pendingCount == 0 ) {
                setFinished();
            }
            return null;
        }

        return computeNextSubmissions();
    }


    private void updateTransactions( ExecutionResult result ) throws TransactionException {
        CommonType type = result.getCommonType();
        Set<UUID> activities = result.getActivities();
        remainingActivities.removeAll( activities );

        assert type == activePartition.commonType;
        activePartition.setResolved( activities, result.isSuccess() );
        if ( activePartition.isAtomic ) {
            if ( result.isSuccess() && activePartition.hasFailed ) {
                throw new TransactionException( "Transaction has already been rolled back" ); // handle remaining activities after abort
            }
        } else {
            if ( result.isSuccess() ) {
                activities.forEach( sm::commitTransaction );
            } else {
                activities.forEach( sm::rollbackTransaction );
            }
        }
    }


    private List<ExecutionSubmission> computeNextSubmissions() {
        List<SubmissionFactory> factories = optimizer.computeNextTrees( maxWorkers - pendingCount, activePartition.commonType );
        pendingCount += factories.size();
        List<ExecutionSubmission> submissions = factories.stream().map( f -> f.create( sm, workflow ) ).toList();
        for ( ExecutionSubmission submission : submissions ) {
            setStates( submission.getActivities(), ActivityState.EXECUTING );
            executionMonitor.addInfo( submission.getInfo() );
        }

        return submissions;
    }


    private void setErrorVariable( Set<UUID> activities, ExecutorException exception ) {
        ObjectNode value = exception.getVariableValue();
        for ( UUID n : activities ) {
            workflow.getActivity( n ).getVariables().setError( value );
        }
    }


    private void updateGraph( boolean isSuccess, Set<UUID> activities, UUID rootId, AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
        // must not access this.execDag as it might be null at this point
        // TODO: any not yet executed activity whose input edges are all either Active or Inactive should have their variableStores updated -> reduce number of empty optionals in previews / canFuse etc.
        ActivityWrapper root = workflow.getActivity( rootId );
        boolean isInitialUpdate = root.getState() == ActivityState.SAVED;

        for ( UUID n : activities ) {
            ActivityWrapper wrapper = workflow.getActivity( n );
            if ( !isInitialUpdate ) {
                if ( isSuccess ) {
                    wrapper.setState( n == rootId ? ActivityState.SAVED : ActivityState.FINISHED );
                } else {
                    wrapper.setState( ActivityState.FAILED );
                }
            }

            for ( ExecutionEdge execEdge : dag.getOutwardEdges( n ) ) {
                Edge edge = workflow.getEdge( execEdge );
                if ( activities.contains( execEdge.getTarget() ) ) {
                    edge.setState( isSuccess ? EdgeState.ACTIVE : EdgeState.INACTIVE );
                } else if ( !(isInitialUpdate && edge.getTo().getState() == ActivityState.SAVED) ) { // already saved target activities do not have to be updated again
                    assert edge.isIgnored() || !edge.getTo().getState().isExecuted() :
                            "Encountered an activity that was executed before its predecessors: " + edge.getTo();

                    boolean isActive = isSuccess ?
                            !(edge instanceof ControlEdge control) || control.isOnSuccess() :
                            (edge instanceof ControlEdge control && !control.isOnSuccess());
                    propagateResult( isActive, edge, dag, isInitialUpdate );
                }
            }
        }

        // TODO: update entire workflow instead of dag?
        if ( !isInitialUpdate ) {
            for ( UUID n : GraphUtils.getTopologicalIterable( dag, rootId, false ) ) {
                workflow.updatePreview( n ); // TODO: use updateValidPreview instead? How to handle inconsistency?
            }
        }
    }


    private void propagateResult( boolean isActive, Edge edge, AttributedDirectedGraph<UUID, ExecutionEdge> dag, boolean ignorePartitions ) {
        // must not access this.execDag as it might be null at this point
        Partition sourcePartition = partitions.get( edge.getFrom().getConfig().getCommonType() );
        Partition targetPartition = partitions.get( edge.getTo().getConfig().getCommonType() );
        if ( !(ignorePartitions || sourcePartition.canPropagateTo( targetPartition )) ) {
            return;
        }
        edge.setState( isActive ? EdgeState.ACTIVE : EdgeState.INACTIVE );
        if ( edge.isIgnored() || (isActive && !(edge instanceof ControlEdge)) ) {
            return; // Cannot propagate activation of a data edge or ignored edge, since it cannot change the state of the activity
        }
        ActivityWrapper target = edge.getTo();
        assert !target.getState().isExecuted() : "Encountered an activity that was executed before its predecessors: " + target;

        List<Edge> inEdges = workflow.getInEdges( target.getId() );
        EdgeState canExecute = target.canExecute( inEdges );
        switch ( canExecute ) {
            case IDLE -> {
            }
            case ACTIVE -> {
                List<ControlEdge> controlEdges = inEdges.stream().filter( e -> e instanceof ControlEdge control && !control.isActive() ).map( e -> (ControlEdge) e ).toList();
                controlEdges.forEach( e -> e.setIgnored( true ) ); // the not active edges can be ignored, as they are no longer relevant
                if ( target.getState() == ActivityState.SKIPPED ) {
                    // previously skipped because of common extract that succeeded on its own, but transaction failed
                    target.setState( ActivityState.QUEUED );
                    remainingActivities.add( target.getId() );
                    log.warn( "Setting skipped to queued!" );
                }
            }
            case INACTIVE -> {
                target.setState( ActivityState.SKIPPED );
                remainingActivities.remove( target.getId() );
                targetPartition.setResolved( target.getId(), false ); // no need to catch the exception, as the transaction is already rolled back
                // a skipped activity does NOT count as failed -> onFail control edges also become INACTIVE
                dag.getOutwardEdges( target.getId() ).forEach( e -> propagateResult( false, workflow.getEdge( e ), dag, ignorePartitions ) ); // TODO: out edges from workflow or DAG?
            }
        }
    }


    private void updatePartitions() {
        while ( activePartition.isAllResolved() ) {
            activePartition.finish( execDag );
            if ( remainingActivities.isEmpty() ) {
                break;
            }

            activePartition = switch ( activePartition.commonType ) {
                case EXTRACT -> partitions.get( CommonType.NONE );
                case NONE -> partitions.get( CommonType.LOAD );
                case LOAD -> throw new IllegalStateException( "Cannot have remaining activities when load is finished" );
            };
            activePartition.start();
        }
    }


    private void setFinished() {
        workflow.setState( WorkflowState.IDLE );
        executionMonitor.stop();
        isFinished = true;
    }


    private void setStates( Set<UUID> activities, ActivityState state ) {
        activities.forEach( id -> workflow.getActivity( id ).setState( state ) );
    }


    /**
     * Currently, this class can be considered unnecessary. But it could be used as a starting point for implementing arbitrary partitions of workflows.
     * This could be useful for nested workflows.
     */
    private class Partition {

        private final CommonType commonType;
        private final boolean isAtomic;
        private final Set<UUID> activities;
        private final Set<UUID> remaining = new HashSet<>();
        private boolean hasFailed = false; // whether an activity of this partition was not successful (FAILED or SKIPPED)
        private boolean isStarted = false; // whether execution has started
        private boolean isFinished;


        private Partition( CommonType commonType, AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
            this.commonType = commonType;
            this.isAtomic = commonType != CommonType.NONE;
            this.activities = dag.vertexSet().stream().filter( n -> workflow.getActivity( n ).getConfig().getCommonType() == commonType ).collect( Collectors.toSet() );
            this.remaining.addAll( activities );
            this.isFinished = activities.isEmpty();
        }


        private void start() {
            assert !isStarted;
            if ( isAtomic && !hasFailed && !isAllResolved() ) {
                sm.startCommonTransaction( commonType );
            }

            if ( isAtomic && hasFailed ) {
                // partition failed before it was started
                for ( UUID n : activities ) {
                    if ( remaining.contains( n ) ) {
                        workflow.getActivity( n ).setState( ActivityState.SKIPPED );
                        remaining.remove( n );
                        remainingActivities.remove( n ); // also remove from workflow-wide remaining list
                        // TODO: also update inner edges?
                    }
                }
            }

            isStarted = true;
        }


        private boolean contains( UUID activity ) {
            return activities.contains( activity );
        }


        private boolean isAllResolved() {
            return remaining.isEmpty();
        }


        private void setResolved( UUID activity, boolean isSuccess ) throws TransactionException {
            remaining.remove( activity );

            if ( hasFailed ) {
                return;
            }
            if ( isSuccess ) {
                assert isStarted : "Cannot resolve the successful execution of an activity if its partition has not yet started";
                if ( isAtomic && isAllResolved() ) {
                    try {
                        sm.commitCommonTransaction( commonType );
                    } catch ( TransactionException e ) {
                        hasFailed = true; // TODO: is a manual rollback required?
                        throw e;
                    }
                }
                return;
            }

            hasFailed = true;
            if ( isAtomic ) {
                if ( isStarted ) {
                    sm.rollbackCommonTransaction( commonType );
                    // we do not drop already created checkpoints
                }
            }
        }


        private void setResolved( Set<UUID> activities, boolean isSuccess ) throws TransactionException {
            activities.forEach( n -> setResolved( n, isSuccess ) );
        }


        private void finish( AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
            if ( !isStarted || isFinished || !remaining.isEmpty() ) {
                throw new IllegalStateException( "Partition is not in a valid state to be finished" );
            }

            if ( isAtomic ) {
                for ( UUID n : activities ) {
                    ActivityWrapper wrapper = workflow.getActivity( n );

                    for ( ExecutionEdge execEdge : dag.getOutwardEdges( n ) ) {
                        if ( activities.contains( execEdge.getTarget() ) ) {
                            continue;
                        }
                        Edge edge = workflow.getEdge( execEdge );
                        assert !edge.getTo().getState().isExecuted() :
                                "Encountered an activity that was executed before the previous partition finished: " + edge.getTo();

                        boolean isActive = !hasFailed && wrapper.getState().isSuccess() ?
                                !(edge instanceof ControlEdge control) || control.isOnSuccess() :
                                (edge instanceof ControlEdge control && !control.isOnSuccess());
                        propagateResult( isActive, edge, dag, true ); // propagate across partition border
                    }
                }
            }
            isFinished = true;
        }


        private boolean canPropagateTo( Partition other ) {
            return this == other || !this.isAtomic;
        }


        private void abort() {
            if ( !isFinished ) {
                hasFailed = true;
                if ( isStarted && isAtomic ) {
                    sm.rollbackCommonTransaction( commonType );
                }
            }
        }

    }

}
