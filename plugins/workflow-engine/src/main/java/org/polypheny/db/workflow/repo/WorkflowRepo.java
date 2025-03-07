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

import io.javalin.http.HttpCode;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.workflow.dag.WorkflowImpl;
import org.polypheny.db.workflow.models.JobModel;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.WorkflowModel;

public interface WorkflowRepo {

    int MAX_NAME_LENGTH = 128;
    int MAX_DESCRIPTION_LENGTH = 1024;

    /**
     * Retrieves all workflow definitions stored in the repository.
     *
     * @return a map of workflow IDs to their {@link WorkflowDefModel} definitions.
     * @throws WorkflowRepoException if one of the workflow definitions cannot be read.
     */
    Map<UUID, WorkflowDefModel> getWorkflowDefs() throws WorkflowRepoException;

    /**
     * Retrieves the workflow definition for the given workflow id.
     *
     * @return the {@link WorkflowDefModel} corresponding to the id.
     * @throws WorkflowRepoException if the workflow definition cannot be read.
     */
    WorkflowDefModel getWorkflowDef( UUID id ) throws WorkflowRepoException;

    /**
     * Creates a new workflow with the specified name and group.
     *
     * @param name the name of the new workflow to create.
     * @param group the group of the new workflow or null to use the default group.
     * @return the ID of the newly created workflow.
     * @throws WorkflowRepoException if the workflow cannot be created, such as when a workflow with the
     * same name already exists or if an error occurs during creation.
     */
    UUID createWorkflow( String name, @Nullable String group ) throws WorkflowRepoException;

    /**
     * Creates a new workflow with the specified name in the default group.
     *
     * @param name the name of the new workflow to create.
     * @return the ID of the newly created workflow.
     * @throws WorkflowRepoException if the workflow cannot be created, such as when a workflow with the
     * same name already exists or if an error occurs during creation.
     */
    default UUID createWorkflow( String name ) throws WorkflowRepoException {
        return createWorkflow( name, null );
    }

    /**
     * Reads a specific version of a workflow by ID.
     *
     * @param id the unique ID of the workflow.
     * @param version the version number to read.
     * @return the {@link WorkflowModel} for the specified version.
     * @throws WorkflowRepoException if the workflow cannot be read.
     */
    WorkflowModel readVersion( UUID id, int version ) throws WorkflowRepoException;

    /**
     * Writes a new version of the specified workflow.
     *
     * @param id the unique ID of the workflow.
     * @param description a description of the new version.
     * @param wf the workflow model to be saved as a new version.
     * @return the newly created version number
     * @throws WorkflowRepoException if the workflow cannot be written.
     */
    int writeVersion( UUID id, String description, WorkflowModel wf ) throws WorkflowRepoException;

    /**
     * Deletes an entire workflow, including all its versions.
     *
     * @param id the unique ID of the workflow.
     * @throws WorkflowRepoException if the workflow cannot be deleted, such as when the ID does not exist
     * or if an error occurs during the deletion process.
     */
    void deleteWorkflow( UUID id ) throws WorkflowRepoException;

    /**
     * Deletes a specific version of a workflow.
     *
     * @param id the unique ID of the workflow.
     * @param version the version number to delete.
     * @throws WorkflowRepoException if the workflow cannot be deleted, such as when the ID does not exist
     * or if an error occurs during the deletion process.
     */
    void deleteVersion( UUID id, int version ) throws WorkflowRepoException;

    /**
     * Renames a workflow to the specified (unique) name.
     *
     * @param id the unique ID of the workflow.
     * @param name the new name for the workflow.
     * @throws WorkflowRepoException if the workflow cannot be renamed, such as when a workflow with this name already exists
     * or if an error occurs during the renaming process.
     */
    void renameWorkflow( UUID id, String name ) throws WorkflowRepoException;

    /**
     * Changes the group of a workflow to the specified value.
     *
     * @param id the unique ID of the workflow.
     * @param group the new group for the workflow.
     * @throws WorkflowRepoException if the workflow cannot be modified, such as if an error occurs during the process.
     */
    void updateWorkflowGroup( UUID id, String group ) throws WorkflowRepoException;

    Map<UUID, JobModel> getJobs() throws WorkflowRepoException;

    void setJob( JobModel model ) throws WorkflowRepoException;

    void removeJob( UUID jobId ) throws WorkflowRepoException;

    default JobModel getJob( UUID jobId ) throws WorkflowRepoException {
        JobModel model = getJobs().get( jobId );
        if ( model == null ) {
            throw new WorkflowRepoException( "Job does not exist: " + jobId, HttpCode.NOT_FOUND );
        }
        return model;
    }

    ;

    /**
     * Checks if a workflow with the specified name already exists in the repository.
     *
     * @param name the name of the workflow to check for existence.
     * @return {@code true} if a workflow with the given name exists, {@code false} otherwise.
     */
    default boolean doesNameExist( String name ) {
        try {
            return getWorkflowDefs().values().stream()
                    .anyMatch( workflowDef -> workflowDef.getName().equals( name ) );
        } catch ( WorkflowRepoException e ) {
            return false;
        }
    }

    /**
     * Retrieves the workflowId for the given workflow name.
     *
     * @return the workflowId corresponding to the specified unique name or null if no workflow with that name exists.
     */
    default UUID getWorkflowId( String name ) {
        try {
            return getWorkflowDefs().entrySet().stream()
                    .filter( e -> e.getValue().getName().equals( name ) )
                    .map( Entry::getKey ).findAny().orElse( null );
        } catch ( WorkflowRepoException e ) {
            return null;
        }
    }

    /**
     * Checks if a workflow with a valid definition exists in the repository.
     *
     * @param id the unique ID of the workflow to check.
     * @return {@code true} if the workflow exists and is valid, {@code false} otherwise.
     */
    default boolean doesExist( UUID id ) {
        try {
            getWorkflowDef( id );
            return true;
        } catch ( WorkflowRepoException e ) {
            return false;
        }
    }

    /**
     * Checks if a specific version of a workflow exists in the repository.
     *
     * @param id the unique ID of the workflow to check.
     * @param version the version number to check for existence within the workflow.
     * @return {@code true} if the specified version of the workflow exists, {@code false} otherwise.
     */
    default boolean doesExist( UUID id, int version ) {
        try {
            return getWorkflowDef( id ).getVersions().containsKey( version );
        } catch ( WorkflowRepoException e ) {
            return false;
        }
    }

    /**
     * Creates a new workflow by duplicating a specific version of an existing workflow.
     * Only the copied version (with version-nr 0) will be part of the copied workflow.
     *
     * @param id the ID of the existing workflow to copy from.
     * @param version the version number of the existing workflow to copy.
     * @param newName the name for the newly created workflow.
     * @param newGroup the group for the newly created workflow.
     * @return the ID of the newly created workflow.
     * @throws WorkflowRepoException if the workflow cannot be created.
     */
    default UUID createWorkflowFromVersion( UUID id, int version, String newName, String newGroup ) throws WorkflowRepoException {
        WorkflowDefModel old = getWorkflowDefs().get( id );
        UUID newId = createWorkflow( newName, newGroup );
        writeVersion( newId, "Copy of '" + old.getName() + " v" + version + "'", readVersion( id, version ) );
        return newId;
    }

    default UUID importWorkflow( String name, String group, WorkflowModel workflow ) throws WorkflowRepoException {
        WorkflowModel validated;
        try {
            validated = WorkflowImpl.fromModel( workflow ).toModel( false );
        } catch ( Exception e ) {
            throw new WorkflowRepoException( "Workflow has an invalid format: " + e.getMessage(), HttpCode.BAD_REQUEST );
        }
        UUID workflowId = createWorkflow( name, group );
        writeVersion( workflowId, "Imported", validated );
        return workflowId;
    }


    default void printWorkflowDefs() throws WorkflowRepoException {
        for ( Map.Entry<UUID, WorkflowDefModel> entry : getWorkflowDefs().entrySet() ) {
            UUID workflowId = entry.getKey();
            WorkflowDefModel workflowDef = entry.getValue();
            System.out.println( "Workflow ID: " + workflowId.toString() + " => " + workflowDef );
        }
    }


    /**
     * Checks whether the given string is a valid UUID.
     * This is useful for validating workflow IDs.
     *
     * @param str string representation to check
     * @return true if it is a valid UUID
     */
    static boolean isValidUUID( String str ) {
        try {
            UUID.fromString( str );
            return true;
        } catch ( IllegalArgumentException e ) {
            return false;
        }
    }

    /**
     * Returns a UUID represented by the given string or null if it is invalid.
     *
     * @param str string representation of a UUID
     * @return the UUID corresponding to str or null if not a valid UUID.
     */
    static UUID getIdFromString( String str ) {
        try {
            return UUID.fromString( str );
        } catch ( IllegalArgumentException e ) {
            return null;
        }
    }


    @Getter
    class WorkflowRepoException extends IOException {

        private final HttpCode errorCode;


        public WorkflowRepoException( String message, Throwable cause, HttpCode errorCode ) {
            super( message, cause );
            this.errorCode = errorCode;
        }


        public WorkflowRepoException( String message ) {
            this( message, null, HttpCode.INTERNAL_SERVER_ERROR );
        }


        public WorkflowRepoException( String message, HttpCode errorCode ) {
            this( message, null, errorCode );
        }


        public WorkflowRepoException( String message, Throwable cause ) {
            this( message, cause, HttpCode.INTERNAL_SERVER_ERROR );
        }


        @Override
        public String toString() {
            // If there's a cause, include its message as well
            if ( getCause() != null ) {
                return getMessage() + " (caused by: " + getCause().toString() + ")";
            }
            return getMessage();
        }

    }

}
