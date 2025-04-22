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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.WorkflowUtils;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.engine.storage.StorageUtils;

class GlobalSchedulerTest {

    private static UUID sessionId;
    private static StorageManager sm;
    private static TestHelper testHelper;
    private static GlobalScheduler scheduler;


    @BeforeAll
    public static void start() throws SQLException {
        testHelper = TestHelper.getInstance();
        StorageUtils.addHsqldbLocksStore( "locks" );
        StorageUtils.addRelData();
        scheduler = GlobalScheduler.getInstance();
    }


    @BeforeEach
    public void init() {
        sessionId = UUID.randomUUID();
        sm = new StorageManagerImpl( sessionId, StorageUtils.getDefaultStoreMap( "locks" ) );
    }


    @AfterEach
    public void cleanup() {
        try {
            sm.close();
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        testHelper.checkAllTrxClosed();
    }


    @AfterAll
    public static void tearDown() {
        StorageUtils.dropData();
    }


    @Test
    void executeSimpleWorkflowTest() throws Exception {
        Workflow workflow = WorkflowUtils.getWorkflow1();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, ids.get( 1 ) );
        scheduler.awaitResultProcessor( 5000 );

        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ) );
    }


    @Test
    void executeUnionWorkflowTest() throws Exception {
        Workflow workflow = WorkflowUtils.getUnionWorkflow();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 2 ), 0 ) );

    }


    @Test
    void executeMergeWorkflowTest() throws Exception {
        Workflow workflow = WorkflowUtils.getMergeWorkflow( true );
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ) );
        assertFalse( sm.hasCheckpoint( ids.get( 2 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 3 ), 0 ) );
    }


    @Test
    void executeWorkflowInStepsTest() throws Exception {
        Workflow workflow = WorkflowUtils.getUnionWorkflow();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        for ( int i = 1; i < 3; i++ ) {
            scheduler.startExecution( workflow, sm, ids.get( i ) );
            scheduler.awaitResultProcessor( 5000 );
            System.out.println( StorageUtils.readCheckpoint( sm, ids.get( i ), 0 ) );
        }
        testHelper.checkAllTrxClosed();
    }


    @Test
    void simpleFusionTest() throws Exception {
        Workflow workflow = WorkflowUtils.getSimpleFusion();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );
        assertFalse( sm.hasCheckpoint( ids.get( 0 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ) );
    }


    @Test
    void advancedFusionTest() throws Exception {
        Pair<Workflow, List<UUID>> pair = WorkflowUtils.getAdvancedFusion();
        List<UUID> ids = pair.right;
        scheduler.startExecution( pair.left, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        assertFalse( sm.hasCheckpoint( ids.get( 0 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 2 ), 0 ) );
        {
            assertFalse( sm.hasCheckpoint( ids.get( 3 ), 0 ) );
        }
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 4 ), 0 ) );
    }


    @Test
    void RelValuesFusionTest() throws Exception {
        Workflow workflow = WorkflowUtils.getRelValuesFusion();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( ids.size() - 1 ), 0 ) );
    }


    @Test
    void relExtractTest() throws Exception {
        Workflow workflow = WorkflowUtils.getExtractWorkflow();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );
        assertFalse( sm.hasCheckpoint( ids.get( 0 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( ids.size() - 1 ), 0 ) );
    }


    @Test
    void simplePipeTest() throws Exception {
        Workflow workflow = WorkflowUtils.getSimplePipe();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );
        assertFalse( sm.hasCheckpoint( ids.get( 0 ), 0 ) );
        assertFalse( sm.hasCheckpoint( ids.get( 1 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( ids.size() - 1 ), 0 ) );
    }


    @Test
    void interruptPipeTest() throws Exception {
        Workflow workflow = WorkflowUtils.getLongRunningPipe( 2000 );
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        Thread.sleep( 1000 );
        scheduler.interruptExecution( sessionId );
        scheduler.awaitResultProcessor( 5000 );
        checkFailed( workflow, ids );
    }


    @Test
    void combinedFuseAndPipeTest() throws Exception {
        Pair<Workflow, List<UUID>> pair = WorkflowUtils.getCombinedFuseAndPipe();
        List<UUID> ids = pair.right;
        scheduler.startExecution( pair.left, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        assertFalse( sm.hasCheckpoint( ids.get( 0 ), 0 ) );
        assertFalse( sm.hasCheckpoint( ids.get( 1 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 2 ), 0 ) );
        assertFalse( sm.hasCheckpoint( ids.get( 3 ), 0 ) );
        assertFalse( sm.hasCheckpoint( ids.get( 4 ), 0 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 5 ), 0 ) );
    }


    @Test
    void variableWritingTest() throws Exception {
        Workflow workflow = WorkflowUtils.getVariableWritingWorkflow();
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );
        assertFalse( sm.hasCheckpoint( ids.get( 1 ), 0 ) );

        List<String> fieldNames = StorageUtils.readCheckpointType( sm, ids.get( 0 ), 0 ).getFieldNames();
        int colIdx = StorageUtils.readCheckpointType( sm, ids.get( ids.size() - 1 ), 0 ).getFieldNames().indexOf( "field_names" );
        List<List<PolyValue>> rows = StorageUtils.readCheckpoint( sm, ids.get( ids.size() - 1 ), 0 );
        System.out.println( rows );
        assertEquals( fieldNames.toString(), rows.get( 0 ).get( colIdx ).toString() );
    }


    @Test
    void concurrentActivityExecutionTest() throws Exception {
        int nBranches = 10;
        int delay = 1000;
        assert nBranches <= GlobalScheduler.GLOBAL_WORKERS;

        Workflow workflow = WorkflowUtils.getParallelBranchesWorkflow( nBranches, delay, nBranches );
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( delay + 2000 ); // not enough time if not executed concurrently

        for ( int i = 0; i <= nBranches; i++ ) { // also checks initial activity
            assertTrue( sm.hasCheckpoint( ids.get( i ), 0 ) );
        }
    }


    @Test
    void concurrentWorkflowExecutionTest() throws Exception {
        int nWorkflows = 10;
        int delay = 1000;
        assert nWorkflows <= GlobalScheduler.GLOBAL_WORKERS;

        List<Workflow> workflows = new ArrayList<>();
        List<StorageManager> storageManagers = new ArrayList<>();
        for ( int i = 0; i < nWorkflows; i++ ) {
            StorageManager storageManager = new StorageManagerImpl( UUID.randomUUID(), StorageUtils.getDefaultStoreMap( "locks" ) );
            Workflow workflow = WorkflowUtils.getLongRunningPipe( delay );
            workflows.add( workflow );
            storageManagers.add( storageManager );
            scheduler.startExecution( workflow, storageManager, null );
        }
        scheduler.awaitResultProcessor( delay + 2000 ); // not enough time if not executed concurrently

        for ( Pair<Workflow, StorageManager> entry : Pair.zip( workflows, storageManagers ) ) { // also checks initial activity
            List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( entry.left );
            assertTrue( entry.right.hasCheckpoint( ids.get( ids.size() - 1 ), 0 ) );
        }
    }


    @Test
    void commonTransactionsTest() throws Exception {
        Pair<Workflow, List<UUID>> pair = WorkflowUtils.getCommonTransactionsWorkflow( false );
        List<UUID> ids = pair.right;
        scheduler.startExecution( pair.left, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        for ( int i = 0; i < ids.size(); i++ ) {
            UUID id = ids.get( i );
            if ( i == 5 ) {
                assertFalse( sm.hasCheckpoint( id, 0 ) );
                assertEquals( ActivityState.SKIPPED, pair.left.getActivity( id ).getState() );
                continue;
            }
            System.out.println( i + ": " + StorageUtils.readCheckpoint( sm, id, 0 ) );
            assertEquals( ActivityState.SAVED, pair.left.getActivity( id ).getState() );
        }
    }


    @Test
    void commonTransactionFailsTest() throws Exception {
        Pair<Workflow, List<UUID>> pair = WorkflowUtils.getCommonTransactionsWorkflow( true );
        List<UUID> ids = pair.right;
        scheduler.startExecution( pair.left, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        checkFailed( pair.left, ids.get( 2 ) );

        for ( int i = 3; i < ids.size(); i++ ) {
            UUID id = ids.get( i );
            if ( i == 5 ) {
                System.out.println( i + ": " + StorageUtils.readCheckpoint( sm, id, 0 ) );
                continue;
            }
            assertFalse( sm.hasCheckpoint( id, 0 ) );
            assertEquals( ActivityState.SKIPPED, pair.left.getActivity( id ).getState() );
        }
    }


    @Test
    void commonExtractInnerActivityFailsTest() throws Exception {
        Pair<Workflow, List<UUID>> pair = WorkflowUtils.getCommonExtractSkipActivityWorkflow();
        List<UUID> ids = pair.right;
        scheduler.startExecution( pair.left, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ) ); // on its own the activity was successful
        checkFailed( pair.left, ids.get( 1 ) );
        checkSkipped( pair.left, ids.get( 2 ) );
        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 3 ), 0 ) ); // this activity was triggered by the common rollback
    }


    @Test
    void commonLoadGetsSkippedTest() throws Exception {
        Pair<Workflow, List<UUID>> pair = WorkflowUtils.getCommonLoadGetsSkippedWorkflow();
        List<UUID> ids = pair.right;
        scheduler.startExecution( pair.left, sm, null );
        scheduler.awaitResultProcessor( 15000 );

        System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ) );
        checkFailed( pair.left, ids.get( 1 ) );
        checkSkipped( pair.left, ids.get( 2 ) );
        checkSkipped( pair.left, ids.get( 3 ) );
    }


    @Test
    void documentWorkflowTest() throws Exception {
        int nDocs = 3;
        Workflow workflow = WorkflowUtils.getDocumentWorkflow( nDocs );
        List<UUID> ids = WorkflowUtils.getTopologicalActivityIds( workflow );
        scheduler.startExecution( workflow, sm, null );
        scheduler.awaitResultProcessor( 5000 );

        //System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ) );
        //System.out.println( StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ) );
        assertEquals( nDocs, StorageUtils.readCheckpoint( sm, ids.get( 0 ), 0 ).size() );
        assertEquals( nDocs, StorageUtils.readCheckpoint( sm, ids.get( 1 ), 0 ).size() );
    }


    private void checkFailed( Workflow workflow, List<UUID> failedActivityIds ) {
        for ( UUID id : failedActivityIds ) {
            checkFailed( workflow, id );
        }
    }


    private void checkFailed( Workflow workflow, UUID failedActivityId ) {
        assertFalse( sm.hasCheckpoint( failedActivityId, 0 ), failedActivityId + " still has a checkpoint" );
        assertEquals( ActivityState.FAILED, workflow.getActivity( failedActivityId ).getState() );
    }


    private void checkSkipped( Workflow workflow, UUID activityId ) {
        assertFalse( sm.hasCheckpoint( activityId, 0 ), activityId + " still has a checkpoint" );
        assertEquals( ActivityState.SKIPPED, workflow.getActivity( activityId ).getState() );
    }

}
