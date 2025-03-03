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

import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.ContextConsumer;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.session.NestedSessionManager;

@ActivityDefinition(type = "nestedOutput", displayName = "Nested Workflow Outputs", categories = { ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.ANY), @InPort(type = PortType.ANY, isOptional = true) }, outPorts = {},
        shortDescription = "Defines the outputs for when executing this workflow from within another workflow. At most one such activity can exist in a workflow."
)
@BoolSetting(key = "exportDynamic", displayName = "Export Dynamic Variables", pos = 0,
        defaultValue = false,
        shortDescription = "Whether dynamic variables are exported to the parent workflow.")
@SuppressWarnings("unused")
@Slf4j
public class NestedOutputActivity implements Activity, ContextConsumer {

    public static final int MAX_OUTPUTS = 2;
    private NestedSessionManager nestedManager;
    private boolean isAllowed = true; // activity is only allowed in a nested session


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        // The activity itself doesn't do anything, it's only a "pseudo-activity" to identify the relevant checkpoints that will get linked in the StorageManager of the parent activity.
        if ( !isAllowed ) {
            ctx.throwException( "A NestedOutputActivity can only be executed from within a parent workflow." );
        }
        if ( settings.getBool( "exportDynamic" ) ) {
            System.out.println( "Exporting Dynamic Variables: " + ctx.getVariableStore().getDynamicVariables() );
            nestedManager.setOutDynamicVars( ctx.getVariableStore().getDynamicVariables() );
        }
    }


    @Override
    public void accept( UUID uuid, NestedSessionManager nestedManager, StorageManager storageManager ) {
        if ( this.nestedManager == null ) {
            if ( nestedManager == null || !nestedManager.isNested() ) {
                isAllowed = false;
                return;
            }
            this.nestedManager = nestedManager;
        }
    }

}
