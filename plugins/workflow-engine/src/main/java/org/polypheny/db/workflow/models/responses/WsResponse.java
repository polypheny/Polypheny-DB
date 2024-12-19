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
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.PROGRESS_UPDATE;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.RENDERING_UPDATE;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.STATE_UPDATE;
import static org.polypheny.db.workflow.models.responses.WsResponse.ResponseType.WORKFLOW_UPDATE;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.dag.Workflow.WorkflowState;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.WorkflowModel;

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
        WORKFLOW_UPDATE, // entire workflow
        ACTIVITY_UPDATE, // single activity
        RENDERING_UPDATE, // only renderModel of an activity
        STATE_UPDATE, // all edge and activity states
        PROGRESS_UPDATE,
    }


    public static class WorkflowUpdateResponse extends WsResponse {

        public final WorkflowModel workflow;


        public WorkflowUpdateResponse( @Nullable UUID parentId, WorkflowModel workflow ) {
            super( WORKFLOW_UPDATE, parentId );
            this.workflow = workflow;
        }

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
        public final Map<UUID, ActivityState> activityStates;
        public final List<EdgeModel> edgeStates;


        public StateUpdateResponse( @Nullable UUID parentId, Workflow workflow ) {
            super( STATE_UPDATE, parentId );
            workflowState = workflow.getState();
            activityStates = workflow.getActivities().stream().collect( Collectors.toMap(
                    ActivityWrapper::getId,
                    ActivityWrapper::getState
            ) );
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

}
