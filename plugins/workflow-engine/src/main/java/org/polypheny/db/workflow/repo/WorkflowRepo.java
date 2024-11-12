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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.models.WorkflowModel;

public interface WorkflowRepo {

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
     * Creates a new workflow with the specified name.
     *
     * @param name the name of the new workflow to create.
     * @return the ID of the newly created workflow.
     * @throws WorkflowRepoException if the workflow cannot be created, such as when a workflow with the
     * same name already exists or if an error occurs during creation.
     */
    UUID createWorkflow( String name ) throws WorkflowRepoException;

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
     * @return the ID of the newly created workflow.
     * @throws WorkflowRepoException if the workflow cannot be created.
     */
    default UUID createWorkflowFromVersion( UUID id, int version, String newName ) throws WorkflowRepoException {
        WorkflowDefModel old = getWorkflowDefs().get( id );
        UUID newId = createWorkflow( newName );
        writeVersion( newId, old.getVersions().get( version ).getDescription(), readVersion( id, version ) );
        return newId;
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

        WorkflowRepoException( String message ) {
            super( message );
        }


        WorkflowRepoException( String message, Throwable cause ) {
            super( message, cause );
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
