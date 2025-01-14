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

package org.polypheny.db.workflow.repo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.WorkflowModel;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;

public class WorkflowRepoTest {

    private static WorkflowRepo repo;


    @BeforeAll
    public static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        repo = WorkflowRepoImpl.getInstance();
    }


    @AfterEach
    public void cleanup() {
        PolyphenyHomeDirManager.getInstance().recursiveDeleteFolder( WorkflowRepoImpl.WORKFLOWS_PATH );
        PolyphenyHomeDirManager.getInstance().registerNewFolder( WorkflowRepoImpl.WORKFLOWS_PATH );
    }


    @Test
    public void createWorkflowsTest() throws WorkflowRepoException {
        UUID id1 = repo.createWorkflow( "test1" );
        UUID id2 = repo.createWorkflow( "test2" );
        Map<UUID, WorkflowDefModel> workflows = repo.getWorkflowDefs();
        assertEquals( 2, workflows.size() );
        assertEquals( "test1", workflows.get( id1 ).getName() );
        assertEquals( "test2", workflows.get( id2 ).getName() );

        assertEquals( "test1", repo.getWorkflowDef( id1 ).getName() );
        assertEquals( "test2", repo.getWorkflowDef( id2 ).getName() );
    }


    @Test
    public void writeVersionTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "versionTest" );
        int version = repo.writeVersion( id, "Initial version", new WorkflowModel() );

        assertNotNull( repo.readVersion( id, version ) );
        assertEquals( "Initial version", repo.getWorkflowDef( id ).getVersions().get( version ).getDescription() );
    }


    @Test
    public void writeMultipleVersionsTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "multiVersionWorkflow" );

        repo.writeVersion( id, "First version", new WorkflowModel() );
        repo.writeVersion( id, "Second version", new WorkflowModel() );
        repo.writeVersion( id, "Third version", new WorkflowModel() );

        WorkflowDefModel def = repo.getWorkflowDef( id );
        assertEquals( "First version", def.getVersions().get( 0 ).getDescription() );
        assertEquals( "Second version", def.getVersions().get( 1 ).getDescription() );
        assertEquals( "Third version", def.getVersions().get( 2 ).getDescription() );
    }


    @Test
    public void deleteWorkflowTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "toDelete" );
        assertTrue( repo.doesExist( id ) );
        repo.deleteWorkflow( id );
        assertFalse( repo.doesExist( id ) );
    }


    @Test
    public void deleteVersionTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "testDeleteVersion" );
        WorkflowModel wfModel = new WorkflowModel();
        int version1 = repo.writeVersion( id, "Version 1", wfModel );
        int version2 = repo.writeVersion( id, "Version 2", wfModel );

        repo.deleteVersion( id, version1 );
        assertFalse( repo.doesExist( id, version1 ) );
        assertTrue( repo.doesExist( id, version2 ) );
    }


    @Test
    public void incrementVersionAfterDeletionTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "incrementAfterDeletion" );

        int version1 = repo.writeVersion( id, "Version 1", new WorkflowModel() );
        int version2 = repo.writeVersion( id, "Version 2", new WorkflowModel() );
        int version3 = repo.writeVersion( id, "Version 3", new WorkflowModel() );
        int version4 = repo.writeVersion( id, "Version 4", new WorkflowModel() );

        repo.deleteVersion( id, version2 );
        repo.deleteVersion( id, version4 );

        // the largest existing version (3 in this case) is incremented
        assertEquals( version4, repo.writeVersion( id, "Version 4", new WorkflowModel() ) );
        assertEquals( 0, version1 );
        assertEquals( 2, version3 );
        assertEquals( 3, version4 );

        assertTrue( repo.doesExist( id, version1 ) );
        assertFalse( repo.doesExist( id, version2 ) );
        assertTrue( repo.doesExist( id, version3 ) );
        assertTrue( repo.doesExist( id, version4 ) );
    }


    @Test
    public void renameWorkflowTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "oldName" );
        repo.renameWorkflow( id, "newName" );

        assertEquals( "newName", repo.getWorkflowDef( id ).getName() );
        assertTrue( repo.doesNameExist( "newName" ) );
        assertFalse( repo.doesNameExist( "oldName" ) );
    }


    @Test
    public void updateWorkflowGroupTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "workflow", "originalGroup" );
        assertEquals( "originalGroup", repo.getWorkflowDef( id ).getGroup() );
        repo.updateWorkflowGroup( id, "newGroup" );

        assertEquals( "newGroup", repo.getWorkflowDef( id ).getGroup() );
    }


    @Test
    public void createWorkflowFromVersionTest() throws WorkflowRepoException {
        UUID id = repo.createWorkflow( "original" );
        repo.writeVersion( id, "Irrelevant version 1", new WorkflowModel() );
        int version = repo.writeVersion( id, "Target version", new WorkflowModel() );
        repo.writeVersion( id, "Irrelevant version 2", new WorkflowModel() );

        UUID newId = repo.createWorkflowFromVersion( id, version, "copy" );

        WorkflowDefModel newDef = repo.getWorkflowDef( newId );
        assertEquals( "copy", newDef.getName() );
        assertEquals( 1, newDef.getVersions().size() );
        assertEquals( "Target version", newDef.getVersions().get( 0 ).getDescription() );
    }


    @Test
    public void createWorkflowWithExistingNameTest() {
        assertDoesNotThrow( () -> repo.createWorkflow( "duplicateName" ) );
        WorkflowRepoException e = assertThrows(
                WorkflowRepo.WorkflowRepoException.class,
                () -> repo.createWorkflow( "duplicateName" )
        );
        assertTrue( e.getMessage().startsWith( "Name already exists" ) );
    }

}
