/*
 * Copyright 2019-2025 The Polypheny Project
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity.ContextConsumer;
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
import org.polypheny.db.workflow.dag.variables.VariableStore;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.ExecutionInfoModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.TypePreviewModel;
import org.polypheny.db.workflow.session.NestedSessionManager;

@Slf4j
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

    /**
     * If the activity is part of a common transaction, this indicates whether the transaction was rolled back.
     */
    @Setter
    private boolean isRolledBack = false;

    private final VariableStore variables = new VariableStore(); // depending on state, this either represents the variables before (possibly not yet stable) or after execution (always stable)
    @Setter
    private List<TypePreview> outTypePreview;
    @Setter
    private List<TypePreview> inTypePreview; // contains the (possibly not yet known) input type
    private SettingsPreview settingsPreview; // contains the (possibly not yet known) settings
    private List<InvalidSettingException> invalidSettings = List.of();
    private ActivityException invalidStateReason; // any non-setting related Exception
    private String dynamicName; // null if the activity has no dynamic name
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
        ActivityDef def = getDef();

        try {
            dynamicName = activity.getDynamicName( inTypePreviews, settingsPreview );
        } catch ( Exception e ) {
            dynamicName = null;
        }

        try {
            invalidStateReason = null;
            outTypePreview = activity.previewOutTypes( inTypePreviews, settingsPreview );

            // check for missing inputs to warn the user immediately
            for ( int i = 0; i < inTypePreviews.size(); i++ ) {
                TypePreview preview = inTypePreviews.get( i );
                InPortDef port = def.getInPort( i );
                if ( preview.isMissing() && !port.isOptional() ) {
                    throw new InvalidInputException( "Required input is missing", i );
                }
                if ( !port.getType().couldBeCompatibleWith( preview ) ) {
                    throw new InvalidInputException( "Input type " + preview.getDataModel() + " is incompatible with defined port type " + port.getType(),
                            i );
                }
            }
        } catch ( InvalidSettingException e ) {
            invalidSettings.add( e );
        } catch ( ActivityException e ) {
            invalidStateReason = e; // any other problem, e.g. an invalid input
            throw e;
        } catch ( Exception e ) {
            log.warn( "Unhandled exception while updating out type preview", e );
            invalidStateReason = new ActivityException( "Unexpected Error while updating preview: " + e.getMessage() ); // ideally this should never happen
            throw invalidStateReason;
        } finally {
            if ( outTypePreview == null ) {
                outTypePreview = getDef().getDefaultOutTypePreview();
            }
        }
        if ( !invalidSettings.isEmpty() ) {
            throw invalidSettings.get( 0 );
        }
    }


    public void mergeOutTypePreview( List<AlgDataType> outTypes ) {
        List<TypePreview> merged = new ArrayList<>( outTypes.stream().map( TypePreview::ofType ).toList() );
        for ( int i = 0; i < merged.size(); i++ ) {
            TypePreview preview = merged.get( i );
            TypePreview old = this.outTypePreview.get( i );
            if ( old.isPresent() && old.getDataModel() == preview.getDataModel() ) {
                if ( old.getDataModel() == DataModel.DOCUMENT || old.getDataModel() == DataModel.GRAPH ) {
                    merged.set( i, old ); // The old one might contain information about fields that the new one doesn't have
                }
            }
        }
        this.outTypePreview = Collections.unmodifiableList( merged );
    }


    public void applyContext( @Nullable NestedSessionManager nestedManager, StorageManager sm ) {
        if ( activity instanceof ContextConsumer consumer ) {
            consumer.accept( id, nestedManager, sm );
        }
    }


    public ActivityDef getDef() {
        return ActivityRegistry.get( getType() );
    }


    public ActivityModel toModel( boolean includeState ) {
        if ( includeState ) {
            String invalidReason = invalidStateReason == null ? null : invalidStateReason.getMessage();
            Map<String, String> invalidSettingsMap = invalidSettings.stream().collect(
                    Collectors.toMap( InvalidSettingException::getSettingKey, InvalidSettingException::getMessage ) );
            ExecutionInfoModel infoModel = executionInfo == null ? null : executionInfo.toModel( true );
            return new ActivityModel( type, id, serializableSettings, config, rendering,
                    this.state, this.isRolledBack, getInTypeModels(), getOutTypeModels(), invalidReason,
                    invalidSettingsMap, variables.getPublicVariables( true, true ),
                    dynamicName, infoModel );

        } else {
            return new ActivityModel( type, id, serializableSettings, config, rendering );

        }
    }


    public List<TypePreviewModel> getInTypeModels() {
        return TypePreviewModel.of( inTypePreview, getDef(), true );
    }


    public List<TypePreviewModel> getOutTypeModels() {
        return TypePreviewModel.of( outTypePreview, getDef(), false );
    }


    public ControlStateMerger getControlStateMerger() {
        ControlStateMerger merger = activity.overrideControlStateMerger();
        if ( merger == null ) {
            return config.getControlStateMerger();
        }
        return merger;
    }


    public EdgeState canExecute( List<Edge> inEdges ) {
        EdgeState[] dataEdges = new EdgeState[getDef().getDynamicInPortCount( inEdges )];
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


    public void resetExecution( Map<String, JsonNode> workflowVariables ) {
        activity.reset();
        variables.reset( workflowVariables );
        state = ActivityState.IDLE;
        isRolledBack = false;
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
