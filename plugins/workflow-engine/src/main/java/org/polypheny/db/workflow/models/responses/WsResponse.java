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

package org.polypheny.db.workflow.models.responses;

import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.ACTIVITY_UPDATE;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.CHECKPOINT_DATA;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.ERROR;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.PROGRESS_UPDATE;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.RENDERING_UPDATE;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.STATE_UPDATE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.TypePreviewModel;
import org.polypheny.db.workflow.models.requests.WsRequest.RequestType;

/**
 * The structure of workflows is modified with requests sent over the websocket.
 */
public class WsResponse {

    public final ResponseType type;
    public final UUID msgId;
    public final UUID parentId;


    public WsResponse( ResponseType type, @Nullable UUID parentId ) {
        this.type = type;
        this.parentId = parentId;
        this.msgId = UUID.randomUUID();
    }


    public enum ResponseType {
        ACTIVITY_UPDATE, // single activity
        RENDERING_UPDATE, // only renderModel of an activity
        STATE_UPDATE, // all edge and activity states
        PROGRESS_UPDATE,
        ERROR,
        CHECKPOINT_DATA
    }


    public static class ActivityUpdateResponse extends WsResponse {

        public final ActivityModel activity;


        public ActivityUpdateResponse( @Nullable UUID parentId, ActivityWrapper activity ) {
            super( ACTIVITY_UPDATE, parentId );
            this.activity = activity.toModel( true );
        }

    }


    public static class RenderingUpdateResponse extends WsResponse {

        public final UUID activityId;
        public final RenderModel rendering;


        public RenderingUpdateResponse( @Nullable UUID parentId, ActivityWrapper wrapper ) {
            super( RENDERING_UPDATE, parentId );
            this.activityId = wrapper.getId();
            this.rendering = wrapper.getRendering();
        }

    }


    public static class StateUpdateResponse extends WsResponse {

        public final WorkflowState workflowState;
        public final Map<UUID, ActivityState> activityStates = new HashMap<>();
        public final List<UUID> rolledBack = new ArrayList<>();
        public final Map<UUID, List<TypePreviewModel>> inTypePreviews = new HashMap<>();
        public final Map<UUID, List<TypePreviewModel>> outTypePreviews = new HashMap<>();
        public final Map<UUID, String> activityInvalidReasons = new HashMap<>();
        public final Map<UUID, Map<String, String>> activityInvalidSettings = new HashMap<>();
        public final List<EdgeModel> edgeStates;


        public StateUpdateResponse( @Nullable UUID parentId, Workflow workflow ) {
            super( STATE_UPDATE, parentId );
            workflowState = workflow.getState();

            for ( ActivityWrapper wrapper : workflow.getActivities() ) {
                UUID id = wrapper.getId();
                activityStates.put( id, wrapper.getState() );
                if (wrapper.isRolledBack()) {
                    rolledBack.add( id );
                }
                inTypePreviews.put( id, wrapper.getInTypeModels() );
                outTypePreviews.put( id, wrapper.getOutTypeModels() );
                ActivityException e = wrapper.getInvalidStateReason();
                if ( e != null ) {
                    activityInvalidReasons.put( id, e.getMessage() );
                }
                List<InvalidSettingException> invalidSettings = wrapper.getInvalidSettings();
                if ( !invalidSettings.isEmpty() ) {
                    activityInvalidSettings.put( id, invalidSettings.stream().collect(
                            Collectors.toMap( InvalidSettingException::getSettingKey, InvalidSettingException::getMessage ) ) );
                }
            }
            edgeStates = workflow.getEdges().stream().map( e -> e.toModel( true ) ).toList();
        }

    }


    public static class ProgressUpdateResponse extends WsResponse {

        public final Map<UUID, Double> progress;


        public ProgressUpdateResponse( @Nullable UUID parentId, Map<UUID, Double> progress ) {
            super( PROGRESS_UPDATE, parentId );
            this.progress = progress;
        }

    }


    public static class CheckpointDataResponse extends WsResponse {

        public final Result<?, ?> result;
        public final int limit; // the fixed limit on the number of rows / docs / nodes
        public final long totalCount; // total number of rows / docs / nodes


        public CheckpointDataResponse( @Nullable UUID parentId, Result<?, ?> result, int limit, long totalCount ) {
            super( CHECKPOINT_DATA, parentId );
            this.result = result;
            this.limit = limit;
            this.totalCount = totalCount;
        }

    }


    public static class ErrorResponse extends WsResponse {

        public final String reason;
        public final String cause; // null if there is no cause
        public final RequestType parentType;


        public ErrorResponse( @Nullable UUID parentId, Throwable error, RequestType parentType ) {
            super( ERROR, parentId );
            this.reason = error.getMessage();
            Throwable cause = error.getCause();
            this.cause = cause == null ? null : cause.getMessage();
            this.parentType = parentType;
        }

    }

}
