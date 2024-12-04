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
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.TopologicalOrderIterator;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizer;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizer.SubmissionFactory;
import org.polypheny.db.workflow.engine.scheduler.optimizer.WorkflowOptimizerImpl;
import org.polypheny.db.workflow.engine.storage.StorageManager;

/**
 * The scheduler takes a workflow and a (optional) target activitiy.
 * It determines what activities have to be executed in order to execute the target activity or all leaf nodes of the workflow, if no target was specified.
 * The scheduler uses the fact that activities with checkpoints do not have to be executed again (if the entire workflow should be executed again, it first needs to be reset).
 * <p>
 * We can assume that only a single thread (the GlobalScheduler coordinator) is accessing methods of a
 * WorkflowScheduler instance after its construction.
 * Sole control over the provided Workflow and StorageManager is given to the scheduler from creation up to the point the execution is finished.
 */
public class WorkflowScheduler {

    private final Workflow workflow;
    private final StorageManager sm;
    private final int maxWorkers;
    private final AttributedDirectedGraph<UUID, ExecutionEdge> execDag;
    private final WorkflowOptimizer optimizer;

    private boolean isExtractFinished = false;
    private boolean isLoadFinished = false;
    private boolean isAborted; // either by interruption or failure
    private boolean isFinished;

    private int pendingCount = 0; // current number of unfinished submissions
    private final Set<UUID> remainingActivities = new HashSet<>(); // activities that have not finished execution
    private final Set<UUID> pendingActivities = new HashSet<>(); // contains activities submitted for execution

    private final Map<UUID, List<Optional<AlgDataType>>> typePreviews = new HashMap<>(); // contains the (possibly not yet known) output types of execDag activities
    private final Map<UUID, Map<String, Optional<SettingValue>>> settingsPreviews = new HashMap<>(); // contains the (possibly not yet known) settings of execDag activities


    public WorkflowScheduler( Workflow workflow, StorageManager sm, int globalWorkers, @Nullable UUID targetActivity ) throws Exception {
        this.workflow = workflow;
        this.sm = sm;
        this.maxWorkers = Math.min( workflow.getConfig().getMaxWorkers(), globalWorkers );

        if ( targetActivity != null && workflow.getActivity( targetActivity ).getState() == ActivityState.SAVED ) {
            throw new GenericRuntimeException( "A saved activity first needs to be reset before executing it" );
        }

        validateStructure();
        validateCommonExtract();
        validateCommonLoad();

        this.execDag = targetActivity == null ? prepareExecutionDag() : prepareExecutionDag( List.of( targetActivity ) );
        initPreviews();
        this.optimizer = new WorkflowOptimizerImpl( workflow, execDag );

    }


    public List<ExecutionSubmission> startExecution() {
        return computeNextSubmissions();
    }


    public List<ExecutionSubmission> handleExecutionResult( ExecutionResult result ) {
        pendingCount--;
        remainingActivities.removeAll( result.getActivities() );

        if ( remainingActivities.isEmpty() ) {
            assert pendingCount == 0;

            isFinished = true;
            return null;
        }

        if ( isAborted ) {
            if ( pendingCount == 0 ) {
                isFinished = true;
            }
            return null;
        }

        propagateResult( result.isSuccess(), result.getActivities() );

        return computeNextSubmissions();
    }


    public void interruptExecution() {
        isAborted = true;
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
            throw new GenericRuntimeException( "Cannot prepare executionDag for no targets" );
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
            if ( nWrapper.getState() == ActivityState.SAVED ) {
                savedActivities.add( n );
                continue;
            }

            nWrapper.setState( ActivityState.QUEUED );
            for ( Edge edge : workflow.getInEdges( n ) ) {
                edge.setState( EdgeState.IDLE );
                open.add( edge.getFrom().getId() );
            }
        }

        AttributedDirectedGraph<UUID, ExecutionEdge> execDag = GraphUtils.getInducedSubgraph( workflow.toDag(), visited );

        // handle saved activities (= simulate that they finish their execution successfully)
        for ( UUID saved : savedActivities ) {
            updateGraph( true, Set.of( saved ), execDag ); // result propagation needs to happen individually
        }

        return execDag;
    }


    private void validateStructure() throws Exception {
        // no cycles
        // compatible DataModels for edges
        // compatible settings
        // TODO: verify succesors of idle nodes are idle as well
        // TODO: ensure all nodes to be executed have an empty variable store
    }


    private void validateCommonExtract() throws Exception {
        isExtractFinished = true; // TODO: only if no common extract activities present
    }


    private void validateCommonLoad() throws Exception {
        isLoadFinished = true; // TODO: only if no common load activities present
    }


    private void initPreviews() throws ActivityException {
        for ( UUID n : TopologicalOrderIterator.of( execDag ) ) {
            ActivityWrapper wrapper = workflow.getActivity( n );
            Activity activity = wrapper.getActivity();
            ActivityState state = wrapper.getState();

            if ( state == ActivityState.SAVED ) {
                // settings are not required for already executed nodes
                typePreviews.put( n, sm.getTupleTypes( n ).stream().map( Optional::ofNullable ).toList() );

            } else if ( state == ActivityState.IDLE ) {
                List<Optional<AlgDataType>> inputTypes = new ArrayList<>();
                boolean allInputsSaved = true;
                for ( int i = 0; i < wrapper.getDef().getInPorts().length; i++ ) {
                    DataEdge dataEdge = workflow.getDataEdge( n, i );
                    ActivityWrapper inWrapper = dataEdge.getFrom();
                    if ( remainingActivities.contains( inWrapper.getId() ) ) {
                        allInputsSaved = false;
                    }
                    inputTypes.add( typePreviews.get( inWrapper.getId() ).get( dataEdge.getFromPort() ) );
                }
                // TODO: ensure control inputs are also saved, then merge variables correctly
                // Also change executor merge to be correct (correct order, only active)

                Map<String, Optional<SettingValue>> settings = wrapper.resolveAvailableSettings();
                settingsPreviews.put( n, settings );
                typePreviews.put( n, activity.previewOutTypes( inputTypes, settings ) );

            } else {
                throw new IllegalStateException( "Illegal state of activity while initiating scheduler: " + state + " for " + n );
            }

            switch ( state ) {

                case IDLE -> {
                }
                case QUEUED -> {
                }
                case EXECUTING -> {
                }
                case SKIPPED -> {
                }
                case FAILED -> {
                }
                case FINISHED -> {
                }
                case SAVED -> {
                }
            }
        }
    }


    private List<ExecutionSubmission> computeNextSubmissions() {
        // TODO: determine previews

        List<SubmissionFactory> factories = optimizer.computeNextTrees( null, null, maxWorkers - pendingCount, null );
        pendingCount += factories.size();
        return factories.stream().map( f -> f.create( sm, workflow ) ).toList();
    }


    private void propagateResult( boolean isSuccess, Set<UUID> activities ) {
        throw new NotImplementedException();
    }


    private void updateGraph( boolean isSuccess, Set<UUID> activities, AttributedDirectedGraph<UUID, ExecutionEdge> dag ) {
        // does not access this.execDag
        // TODO: any not yet executed activity whose input edges are all either Active or Inactive should have their variableStores updated -> reduce number of empty optionals in previews / canFuse etc.
        throw new NotImplementedException();
    }

}
