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
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;
import org.polypheny.db.workflow.dag.activities.ActivityDef.InPortDef;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.dag.variables.VariableStore;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.ExecutionInfoModel;
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
    private List<TypePreview> outTypePreview; // TODO: ensure this is always up to date
    @Setter
    private List<TypePreview> inTypePreview; // contains the (possibly not yet known) input type
    private SettingsPreview settingsPreview; // contains the (possibly not yet known) settings
    private List<InvalidSettingException> invalidSettings = List.of();
    private ActivityException invalidStateReason; // any non-setting related Exception
    @Setter
    private ExecutionInfo executionInfo; // execution info from last execution


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


    /**
     * Updates the output type preview based on the provided input type previews and variable stability.
     *
     * @param inTypePreviews a list of optional input type previews.
     * @param hasStableVariables a flag indicating whether stable variables are available. If false, variables are ignored in the preview, as they might still change.
     * @throws ActivityException if the preview could not be resolved without errors. The settingsPreview is still updated, with empty optionals for failed settings.
     */
    public void updateOutTypePreview( List<TypePreview> inTypePreviews, boolean hasStableVariables ) throws ActivityException {
        VariableStore store = hasStableVariables ? variables : new VariableStore();
        Pair<SettingsPreview, List<InvalidSettingException>> built = ActivityRegistry.buildAvailableSettingValues(
                type, store.resolveAvailableVariables( serializableSettings ) );
        settingsPreview = built.left;
        invalidSettings = built.right;

        try {
            invalidStateReason = null;
            outTypePreview = activity.previewOutTypes( inTypePreviews, settingsPreview );

            // check for missing inputs to warn the user immediately
            for ( Pair<TypePreview, InPortDef> pair : Pair.zip( inTypePreviews, Arrays.asList( getDef().getInPorts() ) ) ) {
                if ( pair.left.isMissing() && !pair.right.isOptional() ) {
                    throw new InvalidInputException( "Required input is missing", inTypePreviews.indexOf( pair.left ) );
                }
            }
        } catch ( InvalidSettingException e ) {
            invalidSettings.add( e );
        } catch ( ActivityException e ) {
            invalidStateReason = e; // any other problem, e.g. an invalid input
            throw e;
        }
        if ( !invalidSettings.isEmpty() ) {
            throw invalidSettings.get( 0 );
        }
    }


    public ActivityDef getDef() {
        return ActivityRegistry.get( getType() );
    }


    public ActivityModel toModel( boolean includeState ) {
        if ( includeState ) {
            List<TypePreviewModel> inTypeModels = inTypePreview.stream().map(
                    inType -> inType.map( TypePreviewModel::of ).orElse( null ) ).toList();
            String invalidReason = invalidStateReason == null ? null : invalidStateReason.getMessage();
            Map<String, String> invalidSettingsMap = invalidSettings.stream().collect(
                    Collectors.toMap( InvalidSettingException::getSettingKey, InvalidSettingException::getMessage ) );
            ExecutionInfoModel infoModel = executionInfo == null ? null : executionInfo.toModel();
            return new ActivityModel( type, id, serializableSettings, config, rendering,
                    this.state, inTypeModels, invalidReason, invalidSettingsMap, variables.getVariables(), infoModel );

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


    public void resetExecution( ReadableVariableStore workflowVariables ) {
        activity.reset();
        variables.reset( workflowVariables );
        state = ActivityState.IDLE;
        executionInfo = null;
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
