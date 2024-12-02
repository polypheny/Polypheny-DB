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

package org.polypheny.db.workflow.dag.activities;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.variables.VariableStore;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;

@Getter
public class ActivityWrapper {

    private final Activity activity;
    private final String type;

    private final UUID id;
    private final Map<String, JsonNode> serializableSettings;  // may contain variables that need to be replaced first
    @Setter
    private ActivityConfigModel config;
    @Setter
    private RenderModel rendering;
    private final VariableStore variables; // depending on state, this either represents the variables before or after execution

    /**
     * After initialization, the state should never be changed by the wrapper itself.
     * The state is typically changed by the scheduler.
     */
    @Setter
    private ActivityState state = ActivityState.IDLE;


    protected ActivityWrapper( UUID id, Activity activity, String type, Map<String, JsonNode> settings, ActivityConfigModel config, RenderModel rendering ) {
        this.activity = activity;
        this.id = id;
        this.type = type;
        this.serializableSettings = settings;
        this.config = config;
        this.rendering = rendering;

        this.variables = new VariableStore();
    }


    public void updateSettings( Map<String, JsonNode> newSettings ) {
        serializableSettings.putAll( newSettings );
    }


    public Map<String, SettingValue> resolveSettings() {
        return ActivityRegistry.buildSettingValues( type, variables.resolveVariables( serializableSettings ) );
    }


    public Map<String, Optional<SettingValue>> resolveAvailableSettings() {
        return ActivityRegistry.buildAvailableSettingValues( type, variables.resolveAvailableVariables( serializableSettings ) );
    }


    public ActivityDef getDef() {
        return ActivityRegistry.get( getType() );
    }


    public ActivityModel toModel( boolean includeState ) {
        ActivityState state = includeState ? this.state : null;
        return new ActivityModel( type, id, serializableSettings, config, rendering, state );
    }


    public ControlStateMerger getControlStateMerger() {
        ControlStateMerger merger = activity.overrideControlStateMerger();
        if ( merger == null ) {
            return config.getControlStateMerger();
        }
        return merger;
    }


    public static ActivityWrapper fromModel( ActivityModel model ) {
        return new ActivityWrapper( model.getId(), ActivityRegistry.activityFromType( model.getType() ), model.getType(), model.getSettings(), model.getConfig(), model.getRendering() );
    }


    public enum ActivityState {
        IDLE,
        QUEUED,
        EXECUTING,
        SKIPPED,  // => execution was aborted
        FAILED,
        FINISHED,
        SAVED  // => finished + checkpoint created
    }

}
