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

import static org.polypheny.db.workflow.dag.activities.impl.NestedInputActivity.INDEX_KEY;

import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.ContextConsumer;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.session.NestedSessionManager;

@ActivityDefinition(type = "nestedInput", displayName = "Nested Workflow Input", categories = { ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = {}, outPorts = { @OutPort(type = PortType.ANY) },
        shortDescription = "Defines an input for when executing this workflow from within another workflow. At most one input per index can exist."
                + " Any dynamic or environment variables transferred from the parent workflow become available at this activity."
)
@IntSetting(key = INDEX_KEY, displayName = "Input Index", pos = 0,
        min = 0, max = 100, defaultValue = 0,
        shortDescription = "The index that this input represents. Must be statically known (cannot be set by a variable).")
@SuppressWarnings("unused")
@Slf4j
public class NestedInputActivity implements VariableWriter, ContextConsumer {

    public static final String INDEX_KEY = "index";

    private NestedSessionManager nestedManager;
    private StorageManager storageManager;
    private UUID activityId;
    private boolean isAllowed = true; // nestedInput can only be executed if it's actually in a nested session


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( !settings.keysPresent( INDEX_KEY ) ) {
            throw new InvalidSettingException( "Index must be statically defined.", INDEX_KEY );
        }
        return UnknownType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) throws Exception {
        if ( !isAllowed ) {
            ctx.throwException( "A NestedInputActivity can only be executed from within a parent workflow." );
        }

        // we can only now link the input, since the activity gets reset between the call to setInput and execute
        CheckpointReader inputToLink = nestedManager.removeInput( activityId );
        if ( inputToLink == null ) {
            ctx.throwException( "The required input was not set by the parent workflow: " + settings.getInt( INDEX_KEY ) );
        }
        storageManager.linkCheckpoint( activityId, 0, inputToLink );
        nestedManager.getInDynamicVars().forEach( writer::setVariable );
        nestedManager.getInEnvVars().forEach( writer::setEnvVariable );
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( INDEX_KEY ) ) {
            return "Nested Workflow Input " + settings.getInt( INDEX_KEY );
        }
        return null;
    }


    @Override
    public void accept( UUID activityId, NestedSessionManager nestedManager, StorageManager sm ) {
        this.activityId = activityId;
        if ( this.nestedManager == null ) {
            if ( nestedManager == null || !nestedManager.isNested() ) {
                isAllowed = false;
                return;
            }
            this.nestedManager = nestedManager;
            this.storageManager = sm;
        }
    }

}
