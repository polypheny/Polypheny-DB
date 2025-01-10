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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.VariableStore;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.TypePreviewModel;

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
    @Setter
    private ActivityState state = ActivityState.IDLE;

    private final VariableStore variables = new VariableStore(); // depending on state, this either represents the variables before (possibly not yet stable) or after execution (always stable)
    @Setter
    private List<Optional<AlgDataType>> outTypePreview; // TODO: ensure this is always up to date
    @Setter
    private List<Optional<AlgDataType>> inTypePreview; // contains the (possibly not yet known) input type
    @Setter
    private SettingsPreview settingsPreview; // contains the (possibly not yet known) settings
    private ActivityException invalidStateReason; // null if the state of this activity is not invalid


    protected ActivityWrapper( UUID id, Activity activity, String type, Map<String, JsonNode> settings, ActivityConfigModel config, RenderModel rendering ) {
        this.activity = activity;
        this.id = id;
        this.type = type;
        this.serializableSettings = settings;
        this.config = config;
        this.rendering = rendering;
    }


    public void updateSettings( Map<String, JsonNode> newSettings ) {
        serializableSettings.putAll( newSettings );
    }


    public void resetSettings() {
        serializableSettings.clear();
        serializableSettings.putAll( ActivityRegistry.getSerializableDefaultSettings( type ) );
    }


    public Settings resolveSettings() throws InvalidSettingException {
        return ActivityRegistry.buildSettingValues( type, variables.resolveVariables( serializableSettings ) );
    }

    // TODO: be careful to use correct variables (must be sure they are correct)


    public SettingsPreview resolveAvailableSettings( boolean hasStableVariables ) throws InvalidSettingException {
        VariableStore store = hasStableVariables ? variables : new VariableStore();
        return ActivityRegistry.buildAvailableSettingValues( type, store.resolveAvailableVariables( serializableSettings ) );
    }


    /**
     * Updates the output type preview based on the provided input type previews and variable stability.
     *
     * @param inTypePreviews a list of optional input type previews.
     * @param hasStableVariables a flag indicating whether stable variables are available.
     * @return the resolved settings preview used for computing the preview.
     * @throws ActivityException if an error occurs during the preview resolution.
     */
    public SettingsPreview updateOutTypePreview( List<Optional<AlgDataType>> inTypePreviews, boolean hasStableVariables ) throws ActivityException {
        try {
            SettingsPreview settings = resolveAvailableSettings( hasStableVariables );
            outTypePreview = activity.previewOutTypes( inTypePreviews, settings );
            invalidStateReason = null;
            return settings;
        } catch ( ActivityException e ) {
            e.setActivity( this );
            invalidStateReason = e;
            throw e;
        }
    }


    public ActivityDef getDef() {
        return ActivityRegistry.get( getType() );
    }


    public ActivityModel toModel( boolean includeState ) {
        if ( includeState ) {
            List<TypePreviewModel> inTypeModels = inTypePreview.stream().map(
                    inType -> inType.map( TypePreviewModel::of ).orElse( null ) ).toList();
            String invalidReason = invalidStateReason == null ? null : invalidStateReason.toString();
            return new ActivityModel( type, id, serializableSettings, config, rendering, this.state, inTypeModels, invalidReason, variables.getVariables() );

        } else {
            return new ActivityModel( type, id, serializableSettings, config, rendering );

        }
    }


    public ControlStateMerger getControlStateMerger() {
        ControlStateMerger merger = activity.overrideControlStateMerger();
        if ( merger == null ) {
            return config.getControlStateMerger();
        }
        return merger;
    }


    public EdgeState canExecute( List<Edge> inEdges ) {
        EdgeState[] dataEdges = new EdgeState[getDef().getInPorts().length];
        List<EdgeState> successEdges = new ArrayList<>();
        List<EdgeState> failEdges = new ArrayList<>();
        for ( Edge edge : inEdges ) {
            if ( edge instanceof DataEdge data ) {
                dataEdges[data.getToPort()] = data.getState();
            } else if ( edge instanceof ControlEdge control ) {
                List<EdgeState> list = control.isOnSuccess() ? successEdges : failEdges;
                list.add( control.getState() );
            }
        }

        if ( !activity.getDataStateMerger().merge( Arrays.asList( dataEdges ) ) ) {
            return EdgeState.INACTIVE;
        }
        return getControlStateMerger().merge( successEdges, failEdges );
    }


    public void resetExecution() {
        activity.reset();
        variables.clear();
        state = ActivityState.IDLE;
    }


    /**
     * Every time the outTypePreview is updated, the activity determines if the inTypePreviews
     * and the SettingsPreview would result in an invalid state.
     * In this case, this method returns false and the reason can be retrieved with {@code getInvalidStateReason()}.
     * If it returns true, this only means that during the last update, no contradictory state was observed.
     *
     * @return true if the last {@code updateOutTypePreview()} succeeded.
     */
    public boolean isValid() {
        return invalidStateReason == null;
    }


    public static ActivityWrapper fromModel( ActivityModel model ) {
        String type = model.getType();
        // ensuring the default value is used for missing settings
        Map<String, JsonNode> settings = new HashMap<>( ActivityRegistry.getSerializableDefaultSettings( type ) );
        settings.putAll( model.getSettings() );
        return new ActivityWrapper( model.getId(), ActivityRegistry.activityFromType( type ), type, settings, model.getConfig(), model.getRendering() );
    }


    @Override
    public String toString() {
        return "ActivityWrapper{" +
                "type='" + type + '\'' +
                ", id=" + id +
                ", state=" + state +
                '}';
    }


    public enum ActivityState {
        IDLE( false, false ),
        QUEUED( false, false ),
        EXECUTING( false, false ),
        SKIPPED( false, false ),  // => execution was aborted
        FAILED( true, false ),
        FINISHED( true, true ),
        SAVED( true, true );  // => finished + checkpoint created

        @Getter
        private final boolean isExecuted;
        @Getter
        private final boolean isSuccess;


        ActivityState( boolean isExecuted, boolean isSuccess ) {
            this.isExecuted = isExecuted;
            this.isSuccess = isSuccess;
        }
    }

}
