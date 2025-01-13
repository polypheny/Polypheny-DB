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

package org.polypheny.db.workflow.models.requests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.EdgeModel;
import org.polypheny.db.workflow.models.RenderModel;
import org.polypheny.db.workflow.models.WorkflowConfigModel;

/**
 * The structure of workflows is modified with requests sent over the websocket.
 */
public class WsRequest {

    public RequestType type;
    public UUID msgId;


    public enum RequestType {
        CREATE_ACTIVITY,
        DELETE_ACTIVITY,
        UPDATE_ACTIVITY,
        CLONE_ACTIVITY,
        CREATE_EDGE,
        DELETE_EDGE,
        EXECUTE,
        INTERRUPT,
        RESET,
        UPDATE_CONFIG, // workflow config
        UPDATE_VARIABLES,
        GET_CHECKPOINT,
        KEEPALIVE
    }


    public static class CreateActivityRequest extends WsRequest {

        public String activityType;
        public RenderModel rendering;

    }


    public static class UpdateActivityRequest extends WsRequest {

        public UUID targetId;

        // only the fields to be updated are non-null
        public @Nullable Map<String, JsonNode> settings; // an empty map resets all settings to their default values
        public @Nullable ActivityConfigModel config;
        public @Nullable RenderModel rendering;

    }


    public static class DeleteActivityRequest extends WsRequest {

        public UUID targetId;

    }


    public static class CloneActivityRequest extends WsRequest {

        public UUID targetId;
        public double posX;
        public double posY;

    }


    public static class CreateEdgeRequest extends WsRequest {

        public EdgeModel edge;

    }


    public static class DeleteEdgeRequest extends WsRequest {

        public EdgeModel edge;

    }


    public static class ExecuteRequest extends WsRequest {

        public @Nullable UUID targetId; // or null to execute all

    }


    public static class InterruptRequest extends WsRequest {

    }


    public static class ResetRequest extends WsRequest {

        public @Nullable UUID rootId; // or null to reset all

    }


    public static class UpdateConfigRequest extends WsRequest {

        public WorkflowConfigModel workflowConfig;

    }


    public static class UpdateVariablesRequest extends WsRequest {

        public Map<String, JsonNode> variables;

    }


    public static class GetCheckpointRequest extends WsRequest {

        public UUID activityId;
        public int outputIndex;

    }

}
