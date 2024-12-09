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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
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

    private boolean isAborted; // either by interruption or failure
    private boolean isFinished;

    private int pendingCount = 0; // current number of unfinished submissions
    private final Set<UUID> remainingActivities = new HashSet<>(); // activities that have not finished execution
    private final Set<UUID> remainingCommonExtract = new HashSet<>();
    private final Set<UUID> remainingCommonLoad = new HashSet<>();

    private final Map<UUID, List<Optional<AlgDataType>>> inTypePreviews = new HashMap<>(); // contains the (possibly not yet known) input types of execDag activities
    private final Map<UUID, SettingsPreview> settingsPreviews = new HashMap<>(); // contains the (possibly not yet known) settings of execDag activities


    public WorkflowScheduler( Workflow workflow, StorageManager sm, int globalWorkers, @Nullable UUID targetActivity ) throws Exception {
        log.info( "Instantiating WorkflowScheduler with target: {}", targetActivity );
        workflow.setState( WorkflowState.EXECUTING );
        this.workflow = workflow;
        this.sm = sm;
        this.maxWorkers = Math.min( workflow.getConfig().getMaxWorkers(), globalWorkers );

        if ( targetActivity != null && workflow.getActivity( targetActivity ).getState() == ActivityState.SAVED ) {
            throw new GenericRuntimeException( "A saved activity first needs to be reset before executing it" );
        }

        this.execDag = targetActivity == null ? prepareExecutionDag() : prepareExecutionDag( List.of( targetActivity ) );
        log.info( "ExecDag after initialization: {}", this.execDag );

        workflow.validateStructure( this.execDag );
        log.info( "Structure is valid" );

        this.optimizer = new WorkflowOptimizerImpl( workflow, execDag );

    }


    public List<ExecutionSubmission> startExecution() {
        return computeNextSubmissions();
    }


    public List<ExecutionSubmission> handleExecutionResult( ExecutionResult result ) {
        pendingCount--;

        try {
            updateRemaining( result );
        } catch ( TransactionException e ) {
            result = new ExecutionResult( result.getSubmission(), new ExecutorException( "An error occurred while closing open transactions of executed activities", e ) );
        }

        if ( !result.isSuccess() ) {
            Throwable cause = result.getException().getCause();

            // for debugging
            result.getException().printStackTrace();
            if ( cause != null ) {
                log.warn( "ExecutorException has inner exception", cause );
            }
            setErrorVariable( result.getActivities(), result.getException() );
        }

        updateGraph( result.isSuccess(), result.getActivities(), result.getRootId(), execDag );

        if ( remainingActivities.isEmpty() ) {
            assert pendingCount == 0;

            isFinished = true;
            workflow.setState( WorkflowState.IDLE );
            return null;
        }

        if ( isAborted ) {
            if ( pendingCount == 0 ) {
                isFinished = true;
                workflow.setState( WorkflowState.IDLE );
            }
            return null;
        }

        return computeNextSubmissions();
    }


    public void interruptExecution() {
        isAborted = true;
        workflow.setState( WorkflowState.INTERRUPTED );
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
            remainingActivities.add( n );
            if ( type == CommonType.EXTRACT ) {
                remainingCommonExtract.add( n );
            } else if ( type == CommonType.LOAD ) {
                remainingCommonLoad.add( n );
            }

            List<Edge> inEdges = workflow.getInEdges( n );
            for ( Edge edge : inEdges ) {
                edge.setState( EdgeState.IDLE );
                open.add( edge.getFrom().getId() );
            }
            if ( inEdges.isEmpty() ) {
                // TODO: also initialize any activities that are not successors to SAVED activity
                workflow.recomputeInVariables( n );
                inTypePreviews.put( n, List.of() );
                try {
                    SettingsPreview settings = nWrapper.updateOutTypePreview( List.of(), true );
                    settingsPreviews.put( n, settings );
                } catch ( ActivityException e ) {
                    // TODO: detected an inconsistency in the types and settings. Ignore?
                    e.printStackTrace();
                }

            }
        }

        AttributedDirectedGraph<UUID, ExecutionEdge> execDag = GraphUtils.getInducedSubgraph( workflow.toDag(), visited );

        // handle saved activities (= simulate that they finish their execution successfully)
        for ( UUID saved : savedActivities ) {
            updateGraph( true, Set.of( saved ), saved, execDag ); // result propagation needs to happen individually
        }

        return execDag;
    }


    private void updateRemaining( ExecutionResult result ) throws TransactionException {
        CommonType type = result.getSubmission().getCommonType();
        Set<UUID> activities = result.getActivities();
        remainingActivities.removeAll( activities );

        if ( type == CommonType.NONE ) {
            if ( result.isSuccess() ) {
                activities.forEach( sm::commitTransaction );
            } else {
                activities.forEach( sm::rollbackTransaction );
            }
        } else {
            Set<UUID> remainingCommon = type == CommonType.EXTRACT ? remainingCommonExtract : remainingCommonLoad;
            remainingCommon.removeAll( activities );
            if ( result.isSuccess() ) {
                if ( remainingCommon.isEmpty() ) {
                    sm.commitCommonTransaction( type );
                }
            } else {
                for ( UUID n : remainingCommon ) {
                    ActivityWrapper wrapper = workflow.getActivity( n );
                    if ( wrapper.getState() == ActivityState.QUEUED ) {
                        wrapper.setState( ActivityState.SKIPPED ); // TODO: only skip later when updating graph?
                    }
                }
                remainingCommon.clear();
                sm.rollbackCommonTransaction( type ); // TODO: only roll back when all executing common have finished?
            }
        }
    }


    private List<ExecutionSubmission> computeNextSubmissions() {
        List<SubmissionFactory> factories = optimizer.computeNextTrees( inTypePreviews, settingsPreviews, maxWorkers - pendingCount, getActiveCommonType() );
        pendingCount += factories.size();
        List<ExecutionSubmission> submissions = factories.stream().map( f -> f.create( sm, workflow ) ).toList();
        for ( ExecutionSubmission submission : submissions ) {
            setStates( submission.getActivities(), ActivityState.EXECUTING );
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
                } else {
                    assert edge.isIgnored() || !edge.getTo().getState().isExecuted() :
                            "Encountered an activity that was executed before its predecessors: " + edge.getTo();

                    boolean isActive = isSuccess ?
                            !(edge instanceof ControlEdge control) || control.isOnSuccess() :
                            (edge instanceof ControlEdge control && !control.isOnSuccess());
                    propagateResult( isActive, edge, dag );
                }
            }
        }

        // recompute successor typePreviews and settings
        // TODO: update entire workflow instead of dag?
        for ( UUID n : GraphUtils.getTopologicalIterable( dag, rootId, false ) ) {
            ActivityWrapper wrapper = workflow.getActivity( n );
            if ( isInitialUpdate && inTypePreviews.containsKey( n ) ) {
                continue; // only initialize once
            }
            if ( wrapper.getState() == ActivityState.QUEUED ) {
                workflow.recomputeInVariables( n );
                List<Optional<AlgDataType>> inTypes = workflow.getInputTypes( n );
                inTypePreviews.put( n, inTypes );
                try {
                    SettingsPreview settings = wrapper.updateOutTypePreview( inTypes, workflow.hasStableInVariables( n ) );
                    settingsPreviews.put( n, settings );
                } catch ( ActivityException e ) {
                    // TODO: detected an inconsistency in the types and settings. Ignore?
                    e.printStackTrace();
                }
            }
        }
    }


    private void propagateResult( boolean isActive, Edge edge, AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
        // must not access this.execDag as it might be null at this point
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
                List<ControlEdge> controlEdges = inEdges.stream().filter( e -> e instanceof ControlEdge ).map( e -> (ControlEdge) e ).toList();
                if ( !controlEdges.isEmpty() ) {
                    controlEdges.forEach( e -> e.setIgnored( true ) ); // even the active edges can be ignored, since they are not needed anymore
                }
            }
            case INACTIVE -> {
                target.setState( ActivityState.SKIPPED );
                remainingActivities.remove( target.getId() );
                remainingCommonExtract.remove( target.getId() ); // TODO: what if a common activity is skipped? workflow config?
                remainingCommonLoad.remove( target.getId() );
                // a skipped activity does NOT count as failed -> onFail control edges also become INACTIVE
                workflow.getOutEdges( target.getId() ).forEach( e -> propagateResult( false, e, dag ) );
            }
        }
    }


    private CommonType getActiveCommonType() {
        if ( !remainingCommonExtract.isEmpty() ) {
            return CommonType.EXTRACT;
        }
        if ( remainingActivities.size() > remainingCommonLoad.size() ) {
            return CommonType.NONE;
        }
        return CommonType.LOAD;
    }


    private void setStates( Set<UUID> activities, ActivityState state ) {
        activities.forEach( id -> workflow.getActivity( id ).setState( state ) );
    }

}
