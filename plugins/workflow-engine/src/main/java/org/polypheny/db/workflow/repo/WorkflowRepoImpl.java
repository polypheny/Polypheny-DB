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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.HttpCode;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.workflow.models.JobModel;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.WorkflowModel;

/**
 * A singleton implementation of the {@link WorkflowRepo} interface that stores workflows as files in the HOME dir.
 */
public class WorkflowRepoImpl implements WorkflowRepo {

    private static WorkflowRepoImpl INSTANCE = null;

    private static final String DEF_FILE = "meta.json";
    private static final String JOBS_FILE = "jobs.json";
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
            throw new WorkflowRepoException( DEF_FILE + " not found for workflow ID: " + id, HttpCode.NOT_FOUND );
        }
        try {
            return mapper.readValue( defFile, WorkflowDefModel.class );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to read or deserialize " + DEF_FILE + " for ID: " + id, e );
        }
    }


    @Override
    public UUID createWorkflow( String name, @Nullable String group ) throws WorkflowRepoException {
        if ( name.length() > MAX_NAME_LENGTH ) {
            throw new WorkflowRepoException( "Name must not exceed " + MAX_NAME_LENGTH + " characters", HttpCode.BAD_REQUEST );
        }
        if ( group != null && group.length() > MAX_NAME_LENGTH ) {
            throw new WorkflowRepoException( "Group must not exceed " + MAX_NAME_LENGTH + " characters", HttpCode.BAD_REQUEST );
        }
        if ( doesNameExist( name ) ) {
            throw new WorkflowRepoException( "Name already exists: " + name, HttpCode.CONFLICT );
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
        serializeToFile( new File( workflowDir, DEF_FILE ), new WorkflowDefModel( name, group ) );

        return id;
    }


    @Override
    public WorkflowModel readVersion( UUID id, int version ) throws WorkflowRepoException {
        File dir = getWorkflowDir( id );
        File file = new File( dir, version + ".json" );

        if ( !doesExist( id, version ) || !file.exists() ) {
            throw new WorkflowRepoException( "Workflow " + id + ", v" + version + " does not exist.", HttpCode.NOT_FOUND );
        }

        try {
            return mapper.readValue( file, WorkflowModel.class );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to read or deserialize " + id + ", v" + version + ".", e );
        }
    }


    @Override
    public int writeVersion( UUID id, String description, WorkflowModel wf ) throws WorkflowRepoException {
        if ( description.length() > MAX_DESCRIPTION_LENGTH ) {
            throw new WorkflowRepoException( "Description must not exceed " + MAX_DESCRIPTION_LENGTH + " characters", HttpCode.BAD_REQUEST );
        }
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
            throw new WorkflowRepoException( "Unable to delete non-existent workflow " + id, HttpCode.NOT_FOUND );
        }
        if ( !phm.recursiveDeleteFolder( WORKFLOWS_PATH + "/" + id.toString() ) ) {
            throw new WorkflowRepoException( "Failed to delete workflow " + id );
        }
    }


    @Override
    public void deleteVersion( UUID id, int version ) throws WorkflowRepoException {
        WorkflowDefModel def = getWorkflowDef( id );
        if ( def.getVersions().size() <= 1 ) {
            throw new WorkflowRepoException( "Cannot delete the only remaining version of workflow " + def.getName(), HttpCode.FORBIDDEN );
        }

        if ( !doesExist( id, version ) ) {
            throw new WorkflowRepoException( "Unable to delete non-existent workflow version " + def.getName() + " v" + version, HttpCode.NOT_FOUND );
        }

        File dir = getWorkflowDir( id );
        File versionFile = new File( dir, version + ".json" );
        if ( !versionFile.exists() ) {
            throw new WorkflowRepoException( "Version file " + versionFile.getName() + " not found for workflow " + def.getName(), HttpCode.NOT_FOUND );
        }
        if ( !versionFile.delete() ) {
            throw new WorkflowRepoException( "Failed to delete version file: " + versionFile.getAbsolutePath() );
        }

        def.removeVersion( version );
        serializeToFile( new File( dir, DEF_FILE ), def );
    }


    @Override
    public void renameWorkflow( UUID id, String name ) throws WorkflowRepoException {
        WorkflowDefModel def = getWorkflowDef( id );
        if ( def.getName().equals( name ) ) {
            return; // same name as before
        }
        if ( name.length() > MAX_NAME_LENGTH ) {
            throw new WorkflowRepoException( "Name must not exceed " + MAX_NAME_LENGTH + " characters", HttpCode.BAD_REQUEST );
        }
        if ( doesNameExist( name ) ) {
            throw new WorkflowRepoException( "A workflow with name " + name + " already exists", HttpCode.CONFLICT );
        }
        def.setName( name );
        serializeToFile( new File( getWorkflowDir( id ), DEF_FILE ), def );  // updated definition
    }


    @Override
    public void updateWorkflowGroup( UUID id, String group ) throws WorkflowRepoException {
        WorkflowDefModel def = getWorkflowDef( id );
        if ( def.getGroup().equals( group ) ) {
            return;
        }
        if ( group.length() > MAX_NAME_LENGTH ) {
            throw new WorkflowRepoException( "Group must not exceed " + MAX_NAME_LENGTH + " characters", HttpCode.BAD_REQUEST );
        }
        def.setGroup( group );
        serializeToFile( new File( getWorkflowDir( id ), DEF_FILE ), def );  // updated definition
    }


    @Override
    public Map<UUID, JobModel> getJobs() throws WorkflowRepoException {
        File file = new File( rootPath, JOBS_FILE );
        if ( !file.exists() ) {
            serializeToFile( file, Map.of() );
            return Map.of();
        }
        if ( file.isDirectory() ) {
            throw new WorkflowRepoException( JOBS_FILE + " must not be a directory" );
        }
        try {
            return mapper.readValue( file, new TypeReference<Map<UUID, JobModel>>() {
            } );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to read or deserialize " + JOBS_FILE, e );
        }
    }


    @Override
    public void setJob( JobModel model ) throws WorkflowRepoException {
        if ( model.getName().length() > MAX_NAME_LENGTH ) {
            throw new WorkflowRepoException( "Name must not exceed " + MAX_NAME_LENGTH + " characters", HttpCode.BAD_REQUEST );
        }
        Map<UUID, JobModel> models = new HashMap<>( getJobs() );
        models.put( model.getJobId(), model );
        serializeToFile( new File( rootPath, JOBS_FILE ), models );
    }


    @Override
    public void removeJob( UUID jobId ) throws WorkflowRepoException {
        Map<UUID, JobModel> models = new HashMap<>( getJobs() );
        models.remove( jobId );
        serializeToFile( new File( rootPath, JOBS_FILE ), models );
    }


    private void serializeToFile( File file, Object value ) throws WorkflowRepoException {
        try ( FileWriter fileWriter = new FileWriter( file ) ) {
            mapper.writeValue( fileWriter, value );
        } catch ( IOException e ) {
            throw new WorkflowRepoException( "Failed to write JSON file: " + file.getAbsolutePath() + ". " + e.getMessage(), e );
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
