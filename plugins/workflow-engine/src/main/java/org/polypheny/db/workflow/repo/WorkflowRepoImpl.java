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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.WorkflowModel;

/**
 * A singleton implementation of the {@link WorkflowRepo} interface that stores workflows as files in the HOME dir.
 */
public class WorkflowRepoImpl implements WorkflowRepo {

    private static WorkflowRepoImpl INSTANCE = null;

    private static final String DEF_FILE = "meta.json";
    public static final String WORKFLOWS_PATH = "data/workflows";
    private final ObjectMapper mapper = new ObjectMapper();
    private final PolyphenyHomeDirManager phm = PolyphenyHomeDirManager.getInstance();
    private final File rootPath = phm.registerNewFolder( WORKFLOWS_PATH );


    private WorkflowRepoImpl() {
    }


    public static WorkflowRepoImpl getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new WorkflowRepoImpl();
        }
        return INSTANCE;
    }


    @Override
    public Map<UUID, WorkflowDefModel> getWorkflowDefs() throws WorkflowRepoException {
        Map<UUID, WorkflowDefModel> defs = new HashMap<>();
        File[] directories = rootPath.listFiles( File::isDirectory );
        if ( directories == null ) {
            throw new WorkflowRepoException( "Failed to list directories in workflow path: " + rootPath );
        }

        for ( File dir : directories ) {
            UUID id = WorkflowRepo.getIdFromString( dir.getName() );
            if ( id == null ) {
                continue; // Skip if the folder name is not a valid UUID (instead of throwing exception)
            }

            File defFile = new File( dir, DEF_FILE );
            if ( defFile.exists() ) {
                try {
                    defs.put( id, mapper.readValue( defFile, WorkflowDefModel.class ) );
                } catch ( IOException e ) {
                    throw new WorkflowRepoException( "Failed to read or deserialize " + DEF_FILE + " for workflow ID: " + id, e );
                }
            }
        }

        return defs;
    }


    @Override
    public WorkflowDefModel getWorkflowDef( UUID id ) throws WorkflowRepoException {
        File dir = getWorkflowDir( id );
        File defFile = new File( dir, DEF_FILE );
        if ( !defFile.exists() ) {
            throw new WorkflowRepoException( DEF_FILE + " not found for workflow ID: " + id );
        }
        try {
            return mapper.readValue( defFile, WorkflowDefModel.class );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to read or deserialize " + DEF_FILE + " for ID: " + id, e );
        }
    }


    @Override
    public UUID createWorkflow( String name ) throws WorkflowRepoException {
        if ( doesNameExist( name ) ) {
            throw new WorkflowRepoException( "Name already exists: " + name );
        }

        UUID id = UUID.randomUUID();
        File workflowDir = new File( rootPath, id.toString() );

        try {
            if ( !workflowDir.mkdir() ) {
                throw new WorkflowRepoException( "Failed to create workflow directory: " + workflowDir.getAbsolutePath() );
            }
        } catch ( SecurityException e ) {
            throw new WorkflowRepoException( "Insufficient permissions to create workflow directory: " + workflowDir.getAbsolutePath(), e );
        }
        serializeToFile( new File( workflowDir, DEF_FILE ), new WorkflowDefModel( name ) );

        return id;
    }


    @Override
    public WorkflowModel readVersion( UUID id, int version ) throws WorkflowRepoException {
        File dir = getWorkflowDir( id );
        File file = new File( dir, version + ".json" );

        if ( !doesExist( id, version ) || !file.exists() ) {
            throw new WorkflowRepoException( "Workflow " + id + ", v" + version + " does not exist." );
        }

        try {
            return mapper.readValue( file, WorkflowModel.class );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to read or deserialize " + id + ", v" + version + ".", e );
        }
    }


    @Override
    public int writeVersion( UUID id, String description, WorkflowModel wf ) throws WorkflowRepoException {
        WorkflowDefModel def = getWorkflowDef( id );
        int version = def.addVersion( description );

        File workflowDir = getWorkflowDir( id );
        serializeToFile( new File( workflowDir, version + ".json" ), wf );  // new version
        serializeToFile( new File( getWorkflowDir( id ), DEF_FILE ), def );  // updated definition

        return version;
    }


    @Override
    public void deleteWorkflow( UUID id ) throws WorkflowRepoException {
        if ( !doesExist( id ) ) {
            throw new WorkflowRepoException( "Unable to delete non-existent workflow " + id );
        }
        if ( !phm.recursiveDeleteFolder( WORKFLOWS_PATH + "/" + id.toString() ) ) {
            throw new WorkflowRepoException( "Failed to delete workflow " + id );
        }
    }


    @Override
    public void deleteVersion( UUID id, int version ) throws WorkflowRepoException {
        if ( !doesExist( id, version ) ) {
            throw new WorkflowRepoException( "Unable to delete non-existent workflow version " + id + " v" + version );
        }

        File dir = getWorkflowDir( id );
        File versionFile = new File( dir, version + ".json" );
        if ( !versionFile.exists() ) {
            throw new WorkflowRepoException( "Version file " + versionFile.getName() + " not found for workflow " + id );
        }
        if ( !versionFile.delete() ) {
            throw new WorkflowRepoException( "Failed to delete version file: " + versionFile.getAbsolutePath() );
        }

        WorkflowDefModel def = getWorkflowDef( id );
        def.removeVersion( version );
        serializeToFile( new File( dir, DEF_FILE ), def );
    }


    @Override
    public void renameWorkflow( UUID id, String name ) throws WorkflowRepoException {
        WorkflowDefModel def = getWorkflowDef( id );
        if ( def.getName().equals( name ) ) {
            return; // same name as before
        }
        if ( doesNameExist( name ) ) {
            throw new WorkflowRepoException( "A workflow with name " + name + " already exists" );
        }
        def.setName( name );
        serializeToFile( new File( getWorkflowDir( id ), DEF_FILE ), def );  // updated definition

    }


    private void serializeToFile( File file, Object value ) throws WorkflowRepoException {
        try ( FileWriter fileWriter = new FileWriter( file ) ) {
            mapper.writeValue( fileWriter, value );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to write workflow JSON file: " + file.getAbsolutePath() + ". " + e.getMessage(), e );
        }
    }


    private File getWorkflowDir( UUID id ) throws WorkflowRepoException {
        // Locate the workflow directory based on the workflow ID
        File workflowDir = new File( rootPath, id.toString() );

        if ( !workflowDir.exists() || !workflowDir.isDirectory() ) {
            throw new WorkflowRepoException( "Workflow directory does not exist for ID: " + id );
        }
        return workflowDir;
    }

}
