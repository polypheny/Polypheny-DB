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

package org.polypheny.db.workflow.dag.activities.impl;

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.ContextConsumer;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.dag.variables.VariableStore;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.models.WorkflowDefModel;
import org.polypheny.db.workflow.repo.WorkflowRepo;
import org.polypheny.db.workflow.repo.WorkflowRepo.WorkflowRepoException;
import org.polypheny.db.workflow.repo.WorkflowRepoImpl;
import org.polypheny.db.workflow.session.NestedSession;
import org.polypheny.db.workflow.session.NestedSessionManager;

@ActivityDefinition(type = "nestedWorkflow", displayName = "Execute Workflow", categories = { ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.ANY, isOptional = true, isMulti = true) },
        outPorts = { @OutPort(type = PortType.ANY, description = "The first output of the nested workflow. If the selected workflow does not specify any outputs, an empty collection is created."),
                @OutPort(type = PortType.ANY, description = "The second output of the nested workflow. If the selected workflow does not specify a second output, an empty collection is created.") },
        shortDescription = "Executes a specific version of an existing workflow. For deeply nested workflows, a sufficient number of global workers must be configured, as each workflow requires at least one worker."
)
@BoolSetting(key = "fromId", displayName = "Select Workflow by ID", pos = 0,
        defaultValue = false,
        shortDescription = "While the selection by name is more convenient, the selection by ID is more robust, as names can be changed.")
@StringSetting(key = "workflowName", displayName = "Workflow Name", pos = 1,
        autoCompleteType = AutoCompleteType.WORKFLOW_NAMES,
        subPointer = "fromId", subValues = { "false" }, maxLength = WorkflowRepo.MAX_NAME_LENGTH)
@StringSetting(key = "workflowId", displayName = "Workflow ID", pos = 2,
        subPointer = "fromId", subValues = { "true" }, maxLength = 37)
@IntSetting(key = "version", displayName = "Version", pos = 3,
        min = 0, defaultValue = 1) // version 0 is often empty, therefore we set it to 1
@StringSetting(key = "variables", displayName = "Nested Workflow Variables", pos = 4,
        textEditor = true, language = "json", maxLength = 10 * 1024, defaultValue = "{}",
        shortDescription = "Overwrite workflow variables in the nested workflow without influencing the workflow variables of this workflow.")

@BoolSetting(key = "transferVars", displayName = "Transfer Workflow Variables", pos = 5,
        defaultValue = false, group = ADVANCED_GROUP,
        shortDescription = "If true, all workflow variables become accessible in the nested workflow.")
@BoolSetting(key = "transferDynamic", displayName = "Transfer Dynamic Variables", pos = 6,
        defaultValue = false, group = ADVANCED_GROUP,
        shortDescription = "If true, all dynamic variables become accessible in the nested workflow.")
@BoolSetting(key = "transferEnv", displayName = "Transfer Environment Variables", pos = 7,
        defaultValue = false, group = ADVANCED_GROUP,
        shortDescription = "If true, all environment variables are transferred to each 'Nested Input Activity' in the nested workflow.")
@SuppressWarnings("unused")
@Slf4j
public class NestedWorkflowActivity implements VariableWriter, ContextConsumer {

    private static final int POLL_INTERVAL_MS = 100; // how often to check if workflow finished execution or should be interrupted
    private static final ObjectMapper mapper = new ObjectMapper();
    private final WorkflowRepo repo = WorkflowRepoImpl.getInstance();

    private NestedSessionManager nestedManager;
    private StorageManager storageManager;
    private UUID activityId;
    private boolean isAllowed = true; // depending on the context, nested workflow execution is not always possible

    private NestedSession session;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( !isAllowed ) {
            throw new ActivityException( "Nested Workflows are not allowed in this context. Try executing the workflow as a user session." );
        }
        if ( settings.keysPresent( "fromId", "workflowName", "workflowId", "version" ) ) {
            UUID workflowId = getWorkflowId( settings.getBool( "fromId" ), settings.getString( "workflowName" ), settings.getString( "workflowId" ) );
            int version = settings.getInt( "version" );

            if ( !repo.doesExist( workflowId ) ) {
                throw new InvalidSettingException( "Specified workflow does not exist: " + workflowId, "workflowId" );
            }
            if ( !repo.doesExist( workflowId, version ) ) {
                throw new InvalidSettingException( "Specified workflow version does not exist: " + version, "version" );
            }
            try {
                if ( repo.readVersion( workflowId, version ).getActivities().isEmpty() ) {
                    throw new InvalidSettingException( "Specified workflow version does not contain any activities", "version" );
                }
            } catch ( WorkflowRepoException e ) {
                throw new InvalidSettingException( "Unable to read workflow version", "version" );
            }
            if ( nestedManager != null ) {
                if ( nestedManager.isCyclic( workflowId, version ) ) {
                    throw new InvalidSettingException( "Detected cycle: Specified workflow cannot be its own successor.", "workflowId" );
                }
                NestedSession oldSession = nestedManager.getSessionForActivity( activityId );
                if ( oldSession != null && !oldSession.isFor( workflowId, version ) ) {
                    try {
                        nestedManager.terminateSession( activityId );
                    } catch ( Exception e ) {
                        throw new GenericRuntimeException( "Unable to terminate nested session: " + e.getMessage(), e );
                    }
                }
            }
        }
        if ( settings.keysPresent( "variables" ) ) {
            String varStr = settings.getString( "variables" );
            if ( !varStr.isBlank() ) {
                try {
                    mapper.readValue( varStr, ObjectNode.class );
                } catch ( Exception e ) {
                    throw new InvalidSettingException( "Invalid workflow variables: " + e.getMessage(), "variables" );
                }
            }
        }
        return List.of( UnknownType.of(), UnknownType.of() );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) throws Exception {
        UUID workflowId = getWorkflowId( settings.getBool( "fromId" ), settings.getString( "workflowName" ), settings.getString( "workflowId" ) );
        int version = settings.getInt( "version" );
        String varStr = settings.getString( "variables" );
        boolean transferVars = settings.getBool( "transferVars" );
        boolean transferDynamic = settings.getBool( "transferDynamic" );
        boolean transferEnv = settings.getBool( "transferEnv" );

        initSession( workflowId, version, ctx );
        session.linkInputs( inputs );

        ctx.logInfo( "Starting execution in session: " + session.getSessionId() );
        session.execute( buildVariableStore( varStr, transferVars, transferDynamic, transferEnv, ctx.getVariableStore() ) );
        waitToFinish( ctx );

        if ( session.getExitCode() == 0 ) {
            ctx.logInfo( "Nested Workflow finished successfully" );
            setOutputs( ctx );
            Map<String, JsonNode> dynamicVars = session.getDynamicOutputVariables();
            System.out.println( "Dynamic vars are " + dynamicVars );
            if ( dynamicVars != null ) {
                dynamicVars.forEach( writer::setVariable );
            }
        } else {
            // failed
            ctx.throwException( "Nested Workflow finished unsuccessfully." );
        }

    }


    @Override
    public void reset() {
        if ( session != null ) {
            session.reset();
            session = null;
        }
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "fromId", "workflowName", "workflowId", "version" ) ) {
            try {
                UUID workflowId = getWorkflowId( settings.getBool( "fromId" ), settings.getString( "workflowName" ), settings.getString( "workflowId" ) );
                WorkflowDefModel def = repo.getWorkflowDef( workflowId );
                return String.format( "Execute Workflow '%s v%s'", def.getName(), settings.getInt( "version" ) );
            } catch ( Exception e ) {
                return null;
            }
        }
        return null;
    }


    @Override
    public void accept( UUID activityId, NestedSessionManager nestedManager, StorageManager sm ) {
        this.activityId = activityId;
        if ( isAllowed && this.nestedManager == null ) {
            if ( nestedManager == null ) {
                isAllowed = false;
                return;
            }
            this.nestedManager = nestedManager;
            this.storageManager = sm;
        }
    }


    private UUID getWorkflowId( boolean fromId, String name, String id ) throws InvalidSettingException {
        if ( fromId ) {
            try {
                return UUID.fromString( id );
            } catch ( Exception e ) {
                throw new InvalidSettingException( "Not a valid UUID: " + id, "workflowId" );
            }
        } else {
            if ( name.isEmpty() ) {
                throw new InvalidSettingException( "Workflow name must not be empty.", "workflowName" );
            }
            UUID workflowId = repo.getWorkflowId( name );
            if ( workflowId == null ) {
                throw new InvalidSettingException( "Workflow does not exist: " + name, "workflowName" );
            }
            return workflowId;
        }
    }


    private void initSession( UUID workflowId, int version, ExecutionContext ctx ) throws Exception {
        NestedSession oldSession = nestedManager.getSessionForActivity( activityId );
        if ( oldSession == null ) {
            // create session
            ctx.logInfo( "Creating new session" );
            session = nestedManager.createSession( activityId, workflowId, version );
        } else if ( oldSession.isFor( workflowId, version ) ) {
            // reuse session
            ctx.logInfo( "Reusing existing session for the same workflow" );
            session = oldSession;
            if ( session.getWorkflowState() != WorkflowState.IDLE ) {
                // we should never get here
                ctx.logError( "Encountered nested workflow that is not IDLE" );
                log.error( "Encountered nested workflow that is not IDLE. Attempting to terminate: {}_v{}", workflowId, version );
                try {
                    nestedManager.terminateSession( activityId );
                } catch ( Exception e ) {
                    log.error( "Nested workflow does not respond to termination.", e );
                    throw new GenericRuntimeException( "Encountered nested workflow that is not IDLE and cannot be terminated: " + workflowId, e );
                }
                throw new GenericRuntimeException( "Encountered nested workflow that is not IDLE." );
            }
        } else {
            // terminate old, then create new session (should already have been done in previewOutTypes()
            ctx.logInfo( "Terminating old session and creating new session" );
            nestedManager.terminateSession( activityId );
            session = nestedManager.createSession( activityId, workflowId, version );
        }
    }


    private ReadableVariableStore buildVariableStore( String varStr, boolean transferVars, boolean transferDynamic, boolean transferEnv, ReadableVariableStore inStore ) throws JsonProcessingException {
        VariableStore outStore = new VariableStore();
        Map<String, JsonNode> workflowVars = new HashMap<>();
        if ( transferVars ) {
            workflowVars.putAll( inStore.getWorkflowVariables() );
        }
        if ( transferDynamic ) {
            inStore.getDynamicVariables().forEach( outStore::setVariable );
        }
        if ( transferEnv ) {
            inStore.getEnvVariables().forEach( outStore::setEnvVariable );
        }
        if ( !varStr.isBlank() ) {
            ObjectNode root = mapper.readValue( varStr, ObjectNode.class );
            root.properties().forEach( e -> workflowVars.put( e.getKey(), e.getValue() ) );
        }
        outStore.updateWorkflowVariables( workflowVars );
        return outStore;
    }


    private void waitToFinish( ExecutionContext ctx ) throws Exception {
        loop:
        while ( true ) {
            switch ( session.getWorkflowState() ) {
                case EXECUTING -> {
                    try {
                        ctx.checkInterrupted();
                    } catch ( ExecutorException e ) {
                        session.interrupt();
                    }
                    ctx.updateProgress( session.getProgress() );
                }
                case INTERRUPTED -> {
                    // wait for interrupt to finish
                }
                case IDLE -> {
                    break loop;
                }
            }
            try {
                Thread.sleep( POLL_INTERVAL_MS );
            } catch ( InterruptedException e ) {
                // This is the "wrong" way to interrupt the execution. We still try to handle it correctly.
                if ( session.getWorkflowState() == WorkflowState.EXECUTING ) {
                    nestedManager.terminateSession( activityId );
                }
                throw new GenericRuntimeException( "Thread was forcefully interrupted. The nested workflow might still be executing." );
            }
        }
    }


    private void setOutputs( ExecutionContextImpl ctx ) {
        List<CheckpointReader> outputs = session.getOutputs();
        for ( int i = 0; i < NestedOutputActivity.MAX_OUTPUTS; i++ ) {
            CheckpointReader output = outputs.get( i );
            if ( output == null ) {
                ctx.logInfo( "Nested workflow did not produce output for index " + i );
                ctx.createDocWriter( i );
            } else {
                storageManager.linkCheckpoint( activityId, i, output );
                output.close();
            }
        }
    }

}
