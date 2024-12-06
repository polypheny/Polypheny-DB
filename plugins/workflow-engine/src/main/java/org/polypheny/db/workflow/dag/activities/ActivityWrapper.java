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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
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
    @Setter
    private ActivityState state = ActivityState.IDLE;

    private final VariableStore variables; // depending on state, this either represents the variables before (possibly not yet stable) or after execution (always stable)
    @Setter
    private List<Optional<AlgDataType>> outTypePreview; // TODO: ensure this is always up to date


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

    // TODO: be careful to use correct variables (must be sure they are correct)


    public Map<String, Optional<SettingValue>> resolveAvailableSettings( boolean hasStableVariables ) {
        VariableStore store = hasStableVariables ? variables : new VariableStore();
        return ActivityRegistry.buildAvailableSettingValues( type, store.resolveAvailableVariables( serializableSettings ) );
    }


    public Map<String, Optional<SettingValue>> updateOutTypePreview( List<Optional<AlgDataType>> inTypePreviews, boolean hasStableVariables ) throws ActivityException {
        Map<String, Optional<SettingValue>> settings = resolveAvailableSettings( hasStableVariables );
        outTypePreview = activity.previewOutTypes( inTypePreviews, settings );
        return settings;
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

        if ( !activity.getDataStateMerger().merge( List.of( dataEdges ) ) ) {
            return EdgeState.INACTIVE;
        }
        return getControlStateMerger().merge( successEdges, failEdges );
    }


    public void resetExecution() {
        activity.reset();
        variables.clear();
        state = ActivityState.IDLE;
    }


    public static ActivityWrapper fromModel( ActivityModel model ) {
        return new ActivityWrapper( model.getId(), ActivityRegistry.activityFromType( model.getType() ), model.getType(), model.getSettings(), model.getConfig(), model.getRendering() );
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
        IDLE( false ),
        QUEUED( false ),
        EXECUTING( false ),
        SKIPPED( false ),  // => execution was aborted
        FAILED( true ),
        FINISHED( true ),
        SAVED( true );  // => finished + checkpoint created

        @Getter
        private final boolean isExecuted;


        ActivityState( boolean isExecuted ) {
            this.isExecuted = isExecuted;
        }
    }

}
