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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.workflow.WorkflowUtils;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionMonitor;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.engine.storage.StorageUtils;

class WorkflowSchedulerTest {

    private static final UUID sessionId = UUID.randomUUID();
    private static StorageManager sm;
    private static TestHelper testHelper;


    @BeforeAll
    public static void start() throws SQLException {
        testHelper = TestHelper.getInstance();
        StorageUtils.addHsqldbLocksStore( "locks" );
    }


    @BeforeEach
    public void init() {
        sm = new StorageManagerImpl( sessionId, StorageUtils.getDefaultStoreMap( "locks" ) );
    }


    @AfterEach
    public void cleanup() {
        try {
            sm.close();
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    void singleActivityTest() throws Exception {
        final int globalWorkers = 1;
                /*new EdgeModel( activities.get( 0 ).getId(), activities.get( 1 ).getId(), 0, 0, false, null ),
                new EdgeModel( activities.get( 1 ).getId(), activities.get( 2 ).getId(), 0, 0, false, null )
        );*/

        Workflow workflow = WorkflowUtils.getWorkflow1();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        assertEquals( WorkflowState.IDLE, workflow.getState() );
        assertEquals( ActivityState.IDLE, workflow.getActivity( ids.get( 0 ) ).getState() );

        WorkflowScheduler scheduler = new WorkflowScheduler( workflow, sm, new ExecutionMonitor(), globalWorkers, ids.get( 0 ) );
        assertEquals( WorkflowState.EXECUTING, workflow.getState() );
        assertEquals( ActivityState.QUEUED, workflow.getActivity( ids.get( 0 ) ).getState() );
        assertEquals( ActivityState.IDLE, workflow.getActivity( ids.get( 1 ) ).getState() );

        List<ExecutionSubmission> submissions = scheduler.startExecution();
        assertEquals( Math.min( ids.size(), globalWorkers ), submissions.size() );
        System.out.println( submissions );
        ExecutionSubmission submission = submissions.get( 0 );

        assertEquals( ids.get( 0 ), submission.getRootId() );
        assertEquals( 1, submission.getActivities().size() );
        assertEquals( ActivityState.EXECUTING, workflow.getActivity( ids.get( 0 ) ).getState() );
        try {
            submission.getExecutor().call();
        } catch ( Exception e ) {
            throw e;
        }
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ) );

        scheduler.handleExecutionResult( new ExecutionResult( submission ) );

        assertEquals( ActivityState.SAVED, workflow.getActivity( ids.get( 0 ) ).getState() );
        assertEquals( WorkflowState.IDLE, workflow.getState() );

    }

}
